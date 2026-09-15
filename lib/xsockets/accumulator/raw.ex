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

defmodule XSockets.Accumulator.Raw do
  @moduledoc """
  No framing: each `push/3` is one immediately poppable packet.

  ## What problem this solves

  Datagram transports (UDP, DTLS) and inner tiers that already receive whole
  payloads need no length header parsing. Each read or descend push becomes one
  handler packet.

  ## RFCs

  - [RFC 8489](https://www.rfc-editor.org/rfc/rfc8489) (STUN over UDP; one datagram per message)
  - [RFC 9147](https://www.rfc-editor.org/rfc/rfc9147) (DTLS datagram payloads)

  ## Options

    * `:max_size` - maximum queued whole packets (default: `1024`)

  Overflow drops the oldest packet and surfaces `:buffer_overflow` on the next
  `pop/1`, then draining continues.
  """
  @behaviour XSockets.Accumulator

  @default_max 1024

  @doc """
  Builds an empty packet queue.

  ## Parameters

    * `opts` - `:max_size` (default `1024`)

      iex> acc = XSockets.Accumulator.Raw.init([])
      iex> acc = XSockets.Accumulator.Raw.push(acc, "hello", %{n: 1})
      iex> {:ok, "hello", %{n: 1}, acc} = XSockets.Accumulator.Raw.pop(acc)
      iex> elem(XSockets.Accumulator.Raw.pop(acc), 0)
      :more
  """
  @impl true
  def init(opts) do
    %{
      queue: :queue.new(),
      max_size: Keyword.get(opts, :max_size, @default_max),
      overflow: false
    }
  end

  @doc """
  Enqueues `chunk` as one packet. Drops the oldest entry when over `:max_size`.

  ## Parameters

    * `acc` - state from `init/1`
    * `chunk` - the whole packet
    * `meta` - metadata returned with this packet from `pop/1`
  """
  @impl true
  def push(%{queue: queue, max_size: max_size} = acc, chunk, meta) do
    queue = :queue.in({chunk, meta}, queue)

    if :queue.len(queue) > max_size do
      {{:value, _dropped}, queue} = :queue.out(queue)
      %{acc | queue: queue, overflow: true}
    else
      %{acc | queue: queue}
    end
  end

  @doc """
  Dequeues one packet, reports overflow, or returns `{:more, acc}` when empty.

  ## Parameters

    * `acc` - state after `push/3`
  """
  @impl true
  def pop(%{overflow: true} = acc) do
    {:error, :buffer_overflow, %{acc | overflow: false}}
  end

  def pop(%{queue: queue} = acc) do
    case :queue.out(queue) do
      {{:value, {packet, meta}}, rest} ->
        {:ok, packet, meta, %{acc | queue: rest}}

      {:empty, _} ->
        {:more, acc}
    end
  end
end
