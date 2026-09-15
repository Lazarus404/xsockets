### ----------------------------------------------------------------------
###
### Copyright (c) 2026 Jahred Love and Xirsys LLC <experts@xirsys.com>
###
### All rights reserved.
###
### XSockets is licensed by Xirsys under the Apache
### License, Version 2.0. (the "License");
###
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###      http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.
###
### See LICENSE for the full license text.
###
### ----------------------------------------------------------------------

defmodule XSockets.Accumulator.Reorder do
  @moduledoc """
  Reordering decorator over any inner `Accumulator`.

  Whole packets from the inner accumulator are tagged with an integer key via
  `key_fun.(packet, meta)`. Keys must be contiguous for in-order release,
  starting at the first key observed. Packets tagged `:unordered` skip the
  reorder buffer.

  ## What problem this solves

  UDP and relay paths can deliver packets out of order. Wrapping an inner
  accumulator with sequence keys lets a tier release contiguous packets to the
  handler while bounding latency and buffer size.

  ## RFCs

  No STUN/TURN RFC; optional sequencing helper for relay or media tiers.
  Sequence numbers may mirror [RFC 3550](https://www.rfc-editor.org/rfc/rfc3550)
  (RTP) when `key_fun/2` extracts them, but this module does not parse RTP.

  ## Options

    * `:inner` - inner accumulator module (default: `Accumulator.Raw`)
    * `:inner_opts` - options passed to `inner.init/1`
    * `:key_fun` - required `(packet, meta) -> integer() | :unordered`
    * `:name` - config lookup key for `Config.reorder_opts/2`
    * `:window`, `:max_delay_ms`, `:on_overflow`, `:enabled` - tunables via
      config and/or explicit opts (`Config.reorder_opts/2`)
    * `:clock` - injectable clock, defaults to `System.monotonic_time/1`
    * `:max_size` - bound on the ready queue (default: `1024`)
  """
  @behaviour XSockets.Accumulator

  alias XSockets.{Accumulator.Raw, Config}

  @default_ready_max 1024

  @doc """
  Builds reorder state. Raises if `:key_fun` is missing.

  ## Parameters

    * `opts` - see module options

      iex> key_fun = fn <<n, _::binary>>, _ -> n end
      iex> acc = XSockets.Accumulator.Reorder.init(key_fun: key_fun)
      iex> acc =
      ...>   acc
      ...>   |> XSockets.Accumulator.Reorder.push(<<2>>, %{})
      ...>   |> XSockets.Accumulator.Reorder.push(<<1>>, %{})
      iex> {:ok, <<1>>, _, acc} = XSockets.Accumulator.Reorder.pop(acc)
      iex> {:ok, <<2>>, _, acc} = XSockets.Accumulator.Reorder.pop(acc)
      iex> elem(XSockets.Accumulator.Reorder.pop(acc), 0)
      :more
  """
  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name)
    tunable = Config.reorder_opts(name, opts)

    inner_mod = Keyword.get(opts, :inner, Raw)
    inner_opts = Keyword.get(opts, :inner_opts, [])

    %{
      inner: inner_mod.init(inner_opts),
      inner_mod: inner_mod,
      key_fun: Keyword.fetch!(opts, :key_fun),
      clock: Keyword.get(opts, :clock, &System.monotonic_time/1),
      enabled: Keyword.get(tunable, :enabled, true),
      window: Keyword.get(tunable, :window, 16),
      max_size: Keyword.get(opts, :max_size, @default_ready_max),
      max_delay_ms: Keyword.get(tunable, :max_delay_ms, 100),
      on_overflow: Keyword.get(tunable, :on_overflow, :flush_oldest),
      next_key: nil,
      release_floor: nil,
      pending: %{},
      ready: :queue.new(),
      overflow: false
    }
  end

  @doc """
  Pushes `chunk` into the inner accumulator.

  ## Parameters

    * `acc` - state from `init/1`
    * `chunk` - bytes for the inner accumulator
    * `meta` - forwarded to `key_fun` after the inner `pop/1`
  """
  @impl true
  def push(%{enabled: false, inner: inner, inner_mod: mod} = acc, chunk, meta) do
    %{acc | inner: mod.push(inner, chunk, meta)}
  end

  def push(acc, chunk, meta) do
    %{acc | inner: acc.inner_mod.push(acc.inner, chunk, meta)}
  end

  @doc """
  Releases the next in-order packet, or `{:more, acc}` while a gap remains.

  ## Parameters

    * `acc` - state after `push/3`
  """
  @impl true
  def pop(%{enabled: false, inner: inner, inner_mod: mod} = acc) do
    case mod.pop(inner) do
      {:ok, packet, meta, inner} -> {:ok, packet, meta, %{acc | inner: inner}}
      {:more, inner} -> {:more, %{acc | inner: inner}}
      {:error, reason, inner} -> {:error, reason, %{acc | inner: inner}}
    end
  end

  def pop(acc) do
    acc =
      acc
      |> ingest_inner()
      |> release_contiguous()
      |> apply_timeouts()
      |> apply_window_overflow()

    case acc.overflow do
      true ->
        {:error, :buffer_overflow, %{acc | overflow: false}}

      false ->
        case :queue.out(acc.ready) do
          {{:value, {packet, meta}}, ready} ->
            {:ok, packet, meta, %{acc | ready: ready}}

          {:empty, _} ->
            {:more, acc}
        end
    end
  end

  defp ingest_inner(acc) do
    case acc.inner_mod.pop(acc.inner) do
      {:ok, packet, meta, inner} ->
        acc = %{acc | inner: inner}
        acc = enqueue_packet(acc, packet, meta)
        ingest_inner(acc)

      {:more, inner} ->
        %{acc | inner: inner}

      {:error, _reason, inner} ->
        %{acc | inner: inner}
    end
  end

  defp enqueue_packet(acc, packet, meta) do
    case acc.key_fun.(packet, meta) do
      :unordered ->
        maybe_bound_ready(acc, {packet, meta})

      key when is_integer(key) ->
        if stale_key?(acc, key) do
          acc
        else
          now = acc.clock.(:millisecond)

          next_key =
            case acc.next_key do
              nil -> key
              existing -> min(existing, key)
            end

          pending =
            acc.pending
            |> Map.put(key, %{packet: packet, meta: meta, received_at: now})
            |> maybe_drop_newest(key, acc.on_overflow, acc.window)

          %{acc | next_key: next_key, pending: pending}
        end
    end
  end

  defp stale_key?(%{release_floor: nil}, _key), do: false

  defp stale_key?(%{release_floor: floor}, key) when is_integer(floor) and is_integer(key) do
    key < floor
  end

  defp maybe_drop_newest(pending, key, :drop_newest, window)
       when map_size(pending) > window do
    Map.delete(pending, key)
  end

  defp maybe_drop_newest(pending, _key, _policy, _window), do: pending

  defp release_contiguous(%{next_key: nil} = acc), do: acc

  defp release_contiguous(acc) do
    case Map.fetch(acc.pending, acc.next_key) do
      {:ok, %{packet: packet, meta: meta}} ->
        released = acc.next_key

        acc
        |> Map.update!(:pending, &Map.delete(&1, released))
        |> Map.put(:next_key, released + 1)
        |> Map.put(:release_floor, released + 1)
        |> maybe_bound_ready({packet, meta})
        |> release_contiguous()

      :error ->
        acc
    end
  end

  defp apply_timeouts(acc) do
    now = acc.clock.(:millisecond)

    timed_out =
      acc.pending
      |> Enum.filter(fn {_key, entry} -> now - entry.received_at >= acc.max_delay_ms end)
      |> Enum.map(fn {key, _} -> key end)
      |> Enum.sort()

    case timed_out do
      [] ->
        acc

      [key | _] ->
        force_release(acc, key)
    end
  end

  defp apply_window_overflow(acc) do
    if map_size(acc.pending) >= acc.window do
      case acc.on_overflow do
        :flush_oldest ->
          key = acc.pending |> Map.keys() |> Enum.min()
          force_release(acc, key)

        :flush_all ->
          acc.pending
          |> Map.keys()
          |> Enum.sort()
          |> Enum.reduce(acc, &force_release(&2, &1))

        :drop_newest ->
          acc

        _ ->
          acc
      end
    else
      acc
    end
  end

  defp force_release(acc, key) do
    case Map.fetch(acc.pending, key) do
      {:ok, %{packet: packet, meta: meta}} ->
        acc =
          acc
          |> Map.update!(:pending, &Map.delete(&1, key))
          |> Map.put(:next_key, key + 1)
          |> Map.put(:release_floor, key + 1)
          |> maybe_bound_ready({packet, meta})

        release_contiguous(acc)

      :error ->
        acc
    end
  end

  defp maybe_bound_ready(%{ready: ready, max_size: max_size} = acc, entry) do
    ready = :queue.in(entry, ready)

    if :queue.len(ready) > max_size do
      {{:value, _}, ready} = :queue.out(ready)
      %{acc | ready: ready, overflow: true}
    else
      %{acc | ready: ready}
    end
  end
end
