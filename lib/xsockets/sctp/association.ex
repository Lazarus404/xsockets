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

defmodule XSockets.Sctp.Association do
  @moduledoc """
  WebRTC SCTP association over a caller-owned byte pipe (typically DTLS
  application data).

  ## What problem this solves

  WebRTC data channels run SCTP encapsulated in DTLS, not raw SCTP over IP.
  This module drives an SCTP association in sans-IO mode via `ex_sctp`: the
  caller feeds inbound DTLS plaintext with `handle_packet/2` and must send
  every `{:transmit, packets}` event back on the DTLS socket. DCEP negotiation
  and user messages are surfaced as events (`:connected`, `{:channel_open, ...}`,
  `{:message, ...}`).

  This is **not** `Transport.SCTP` (OTP `:gen_sctp` over IP).

  Requires the optional Hex dependency `ex_sctp` (Rust toolchain to compile).

  ## RFCs

  - [RFC 4960](https://www.rfc-editor.org/rfc/rfc4960) - SCTP (association, streams)
  - [RFC 8261](https://www.rfc-editor.org/rfc/rfc8261) - SCTP over DTLS
  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) - WebRTC data channels
  - [RFC 8832](https://www.rfc-editor.org/rfc/rfc8832) - DCEP (via `Sctp.Dcep`)
  """

  require Logger

  alias XSockets.Sctp.Dcep

  @dcep_ppi 50
  @string_ppi 51
  @binary_ppi 53
  @empty_string_ppi 56
  @empty_binary_ppi 57

  @typedoc """
  Opaque SCTP association state.

  ## Fields

    * `ref` - `ex_sctp` NIF handle
    * `state` - association lifecycle (`:new`, `:connecting`, `:connected`, `:closed`)
    * `id_type` - even/odd stream id policy for DTLS client/server roles
    * `channels` - map of channel ref to `channel/0`
    * `timer` - optional timeout timer ref from `{:timeout, ms}` events
  """
  @type t :: %{
          ref: ExSCTP.t(),
          state: :new | :connecting | :connected | :closed,
          id_type: :even | :odd | nil,
          channels: %{optional(reference()) => channel()},
          timer: reference() | nil
        }

  @typedoc """
  One negotiated WebRTC data channel.

  ## Fields

    * `ref` - caller-facing channel reference
    * `id` - SCTP stream id (even for offerer, odd for answerer, per role)
    * `label` - DCEP channel label
    * `ordered` - whether messages preserve order on this stream
    * `reliability` - DCEP reliability (`:reliable`, `:rexmit`, or `:timed`)
    * `param` - retransmit count or lifetime ms from DCEP
    * `protocol` - subprotocol string from DCEP Open
    * `ready_state` - `:connecting`, `:open`, or `:closed`
  """
  @type channel :: %{
          ref: reference(),
          id: non_neg_integer() | nil,
          label: String.t(),
          ordered: boolean(),
          reliability: :reliable | :rexmit | :timed,
          param: non_neg_integer(),
          protocol: String.t(),
          ready_state: :connecting | :open | :closed
        }

  @typedoc """
  Events returned to the caller after IO or timeout.

  ## Variants

    * `:connected` - SCTP association established
    * `:disconnected` - association closed
    * `{:transmit, packets}` - SCTP packets to send on the DTLS socket
    * `{:timeout, ms}` - schedule `handle_timeout/1` after `ms` (or `nil` to cancel)
    * `{:channel_open, ref, label, id, meta}` - data channel ready; `meta` has
      `:reliability`, `:order`, and `:param` from DCEP
    * `{:message, ref, payload}` - user message on an open channel
    * `{:channel_closed, ref}` - remote closed the stream
  """
  @type event ::
          :connected
          | :disconnected
          | {:transmit, [binary()]}
          | {:timeout, non_neg_integer() | nil}
          | {:channel_open, reference(), String.t(), non_neg_integer(), map()}
          | {:message, reference(), binary()}
          | {:channel_closed, reference()}

  @doc """
  Returns true when the `ex_sctp` NIF is loaded and SCTP associations can run.
  """
  @spec available?() :: boolean()
  def available?, do: Code.ensure_loaded?(ExSCTP)

  @doc """
  Creates a new SCTP association in `:new` state.

  ## Options

    * `:role` - `:active` (even stream ids; call `connect/1`) or `:passive`
      (odd ids; wait for peer INIT). Default `:passive`.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    ensure_ex_sctp!()

    role = Keyword.get(opts, :role, :passive)

    %{
      ref: ExSCTP.new(),
      state: :new,
      id_type: role_to_id_type(role),
      channels: %{},
      timer: nil
    }
  end

  @doc """
  Starts SCTP as the association initiator (DTLS client / offerer side).

  Only valid when `state` is `:new`. Returns events (typically `{:transmit, ...}`)
  and updated association state.
  """
  @spec connect(t()) :: {[event()], t()}
  def connect(%{state: :new} = assoc) do
    :ok = ExSCTP.connect(assoc.ref)
    drain(%{assoc | state: :connecting})
  end

  def connect(assoc), do: {[], assoc}

  @doc """
  Feeds one plaintext SCTP packet received from the peer (e.g. DTLS `:ssl` data).

  Returns all pending events after processing the chunk. No-op when the
  association is `:closed`.
  """
  @spec handle_packet(t(), binary()) :: {[event()], t()}
  def handle_packet(%{state: :closed} = assoc, _data), do: {[], assoc}

  def handle_packet(assoc, data) when is_binary(data) do
    :ok = ExSCTP.handle_data(assoc.ref, data)
    drain(assoc)
  end

  @doc """
  Handles a previously scheduled SCTP timeout (`{:timeout, ms}` event).

  Call when the timer from a `{:timeout, ms}` event fires.
  """
  @spec handle_timeout(t()) :: {[event()], t()}
  def handle_timeout(%{state: :closed} = assoc), do: {[], assoc}

  def handle_timeout(assoc) do
    _ = ExSCTP.handle_timeout(assoc.ref)
    drain(assoc)
  end

  @doc """
  Sends a user message on an open channel.

  `type` is `:string` (PPI 51) or `:binary` (PPI 53).
  """
  @spec send(t(), reference(), binary(), :string | :binary) :: {[event()], t()} | {:error, atom(), t()}
  def send(assoc, channel_ref, data, type \\ :string)

  def send(%{state: :closed} = assoc, _channel_ref, _data, _type),
    do: {:error, :closed, assoc}

  def send(assoc, channel_ref, data, type)
      when is_reference(channel_ref) and is_binary(data) and type in [:string, :binary] do
    case Map.fetch(assoc.channels, channel_ref) do
      {:ok, %{ready_state: :open, id: id}} when is_integer(id) ->
        {ppi, payload} = to_raw(data, type)
        :ok = ExSCTP.send(assoc.ref, id, ppi, payload)
        drain(assoc)

      {:ok, _} ->
        {:error, :not_open, assoc}

      :error ->
        {:error, :unknown_channel, assoc}
    end
  end

  @doc """
  Returns channel metadata for `ref`, or `nil` when the channel is unknown.
  """
  @spec get_channel(t(), reference()) :: channel() | nil
  def get_channel(assoc, ref), do: Map.get(assoc.channels, ref)

  @doc """
  Opens a local data channel (DCEP DataChannelOpen) after the association is connected.

  Returns `{events, channel_ref, assoc}` or `{:error, reason, assoc}`.
  """
  @spec open_channel(t(), String.t(), keyword()) ::
          {[event()], reference(), t()} | {:error, atom(), t()}
  def open_channel(assoc, label, opts \\ [])

  def open_channel(%{state: state} = assoc, _label, _opts) when state != :connected do
    {:error, :not_connected, assoc}
  end

  def open_channel(assoc, label, opts) when is_binary(label) do
    ordered = Keyword.get(opts, :ordered, true)
    protocol = Keyword.get(opts, :protocol, "")
    reliability = Keyword.get(opts, :reliability, :reliable)
    param = Keyword.get(opts, :param, 0)
    id = next_local_id(assoc)

    case ExSCTP.open_stream(assoc.ref, id) do
      :ok ->
        open = %Dcep.Open{
          reliability: reliability,
          order: if(ordered, do: :ordered, else: :unordered),
          label: label,
          protocol: protocol,
          priority: 0,
          param: param
        }

        :ok = ExSCTP.send(assoc.ref, id, @dcep_ppi, Dcep.encode(open))

        channel = %{
          ref: make_ref(),
          id: id,
          label: label,
          ordered: ordered,
          reliability: reliability,
          param: param,
          protocol: protocol,
          ready_state: :connecting
        }

        assoc = %{assoc | channels: Map.put(assoc.channels, channel.ref, channel)}
        {events, assoc} = drain(assoc)
        {events, channel.ref, assoc}

      {:error, reason} ->
        {:error, reason, assoc}
    end
  end

  defp next_local_id(%{id_type: :even, channels: channels}) do
    used = for {_k, %{id: id}} when is_integer(id) <- channels, rem(id, 2) == 0, do: id
    if used == [], do: 0, else: Enum.max(used) + 2
  end

  defp next_local_id(%{id_type: :odd, channels: channels}) do
    used = for {_k, %{id: id}} when is_integer(id) <- channels, rem(id, 2) == 1, do: id
    if used == [], do: 1, else: Enum.max(used) + 2
  end

  defp role_to_id_type(:active), do: :even
  defp role_to_id_type(:passive), do: :odd

  defp ensure_ex_sctp! do
    unless available?() do
      raise """
      ex_sctp is not loaded. Add {:ex_sctp, \"~> 0.1\"} to your mix.exs deps \
      (declared optional by xsockets), install a Rust toolchain, then \
      mix deps.get && mix compile.
      """
    end
  end

  defp drain(assoc, events \\ []) do
    case handle_ex_event(assoc, ExSCTP.poll(assoc.ref)) do
      {:none, assoc} ->
        {Enum.reverse(events), assoc}

      {nil, assoc} ->
        drain(assoc, events)

      {ev, assoc} when is_list(ev) ->
        drain(assoc, Enum.reverse(ev) ++ events)

      {ev, assoc} ->
        drain(assoc, [ev | events])
    end
  end

  defp handle_ex_event(assoc, :none), do: {:none, assoc}
  defp handle_ex_event(assoc, :disconnected), do: {:disconnected, %{assoc | state: :closed}}
  defp handle_ex_event(assoc, {:transmit, packets}), do: {{:transmit, packets}, assoc}

  defp handle_ex_event(assoc, :connected) do
    {:connected, %{assoc | state: :connected}}
  end

  defp handle_ex_event(assoc, {:timeout, val}) do
    if assoc.timer, do: Process.cancel_timer(assoc.timer)

    timer =
      case val do
        nil -> nil
        ms when is_integer(ms) -> Process.send_after(self(), :sctp_timeout, ms)
      end

    {{:timeout, val}, %{assoc | timer: timer}}
  end

  defp handle_ex_event(assoc, {:stream_opened, _id}), do: {nil, assoc}

  defp handle_ex_event(assoc, {:stream_closed, id}) do
    case channel_by_id(assoc, id) do
      {ref, _} ->
        channels = Map.delete(assoc.channels, ref)
        {{:channel_closed, ref}, %{assoc | channels: channels}}

      nil ->
        {nil, assoc}
    end
  end

  defp handle_ex_event(assoc, {:data, id, @dcep_ppi, data}) do
    with {:ok, dcep} <- Dcep.decode(data),
         {:ok, assoc, events} <- handle_dcep(assoc, id, dcep) do
      {events, assoc}
    else
      _ ->
        Logger.warning("sctp DCEP failed on stream #{id}; closing stream")
        _ = ExSCTP.close_stream(assoc.ref, id)
        {nil, assoc}
    end
  end

  defp handle_ex_event(assoc, {:data, id, ppi, data}) do
    with {:ok, payload} <- from_raw(data, ppi),
         {ref, %{ready_state: :open}} <- channel_by_id(assoc, id) do
      {{:message, ref, payload}, assoc}
    else
      _ ->
        Logger.info("sctp data dropped stream=#{id} ppi=#{ppi} bytes=#{byte_size(data)}")
        {nil, assoc}
    end
  end

  defp handle_dcep(assoc, id, %Dcep.Open{} = open) do
    with false <- Enum.any?(assoc.channels, fn {_k, v} -> v.id == id end),
         true <- valid_remote_id?(assoc, id) do
      :ok = ExSCTP.send(assoc.ref, id, @dcep_ppi, Dcep.encode(%Dcep.Ack{}))

      channel = %{
        ref: make_ref(),
        id: id,
        label: open.label,
        ordered: open.order == :ordered,
        reliability: open.reliability,
        param: open.param,
        protocol: open.protocol,
        ready_state: :open
      }

      assoc = %{assoc | channels: Map.put(assoc.channels, channel.ref, channel)}

      reliability = open.reliability
      param = open.param

      case ExSCTP.configure_stream(assoc.ref, id, channel.ordered, reliability, param) do
        :ok ->
          # Flush Ack transmits now (same pattern as open_channel/3).
          {tx_events, assoc} = drain(assoc)
          meta = channel_open_meta(channel)

          {:ok, assoc, [{:channel_open, channel.ref, channel.label, id, meta} | tx_events]}

        {:error, reason} ->
          Logger.warning("sctp configure_stream failed id=#{id}: #{inspect(reason)}")
          :error
      end
    else
      _ ->
        Logger.warning(
          "sctp DCEP Open rejected id=#{id} id_type=#{inspect(assoc.id_type)} existing=#{map_size(assoc.channels)}"
        )

        :error
    end
  end

  defp handle_dcep(assoc, id, %Dcep.Ack{}) do
    case channel_by_id(assoc, id) do
      {ref, %{ready_state: :connecting} = ch} ->
        ch = %{ch | ready_state: :open}
        assoc = %{assoc | channels: Map.put(assoc.channels, ref, ch)}
        meta = channel_open_meta(ch)
        {:ok, assoc, [{:channel_open, ref, ch.label, id, meta}]}

      _ ->
        :error
    end
  end

  defp channel_open_meta(ch) do
    %{
      reliability: Map.get(ch, :reliability, :reliable),
      order: if(Map.get(ch, :ordered, true), do: :ordered, else: :unordered),
      param: Map.get(ch, :param, 0)
    }
  end

  defp channel_by_id(%{channels: channels}, id) do
    Enum.find(channels, fn {_k, v} -> v.id == id end)
  end

  defp valid_remote_id?(%{id_type: :even}, id), do: rem(id, 2) == 1
  defp valid_remote_id?(%{id_type: :odd}, id), do: rem(id, 2) == 0
  defp valid_remote_id?(%{id_type: nil}, _), do: true

  defp from_raw(data, ppi) when ppi in [@string_ppi, @binary_ppi], do: {:ok, data}
  defp from_raw(_data, ppi) when ppi in [@empty_string_ppi, @empty_binary_ppi], do: {:ok, <<>>}
  defp from_raw(_, _), do: :error

  defp to_raw(<<>>, :string), do: {@empty_string_ppi, <<0>>}
  defp to_raw(data, :string), do: {@string_ppi, data}
  defp to_raw(<<>>, :binary), do: {@empty_binary_ppi, <<0>>}
  defp to_raw(data, :binary), do: {@binary_ppi, data}
end
