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

defmodule XSockets.Transport.SCTP do
  @moduledoc """
  SCTP transport (`:gen_sctp`) when the OTP driver and OS support SCTP.

  ## What problem this solves

  Some deployments speak SCTP over IP. This module implements the `Transport`
  behaviour for that path: bind, client `connect/3`, send on an association and
  stream, and normalize `{:sctp, ...}` mailbox messages.

  Associations are not accepted via `accept/2` (always
  `{:error, :sctp_not_supported}`). Use `XSockets.Sctp.Listener` to own the listen
  socket and track associations. WebRTC SCTP-over-DTLS remains
  `XSockets.Sctp.Association`.

      iex> XSockets.Transport.SCTP.framing()
      :datagram
      iex> XSockets.Transport.SCTP.handle_message({:sctp, :sock, {127, 0, 0, 1}, 9, [], "hi"}, :sock)
      {:data, "hi", {{127, 0, 0, 1}, 9}}
      iex> XSockets.Transport.SCTP.handle_message({:sctp_closed, :sock}, :sock)
      {:closed, :normal}

  ## RFCs

  - [RFC 4960](https://www.rfc-editor.org/rfc/rfc4960) - SCTP (streams, associations)
  """
  @behaviour XSockets.Transport

  alias XSockets.{Config, Telemetry}

  @listen_opts [
    reuseaddr: true,
    active: false,
    sctp_nodelay: true,
    sctp_autoclose: 0,
    sctp_maxseg: 1400,
    sctp_initmsg: {:sctp_initmsg, 10, 10, 4, 30_000}
  ]

  @doc false
  @impl true
  def listen(ip, port, opts) do
    listen_opts =
      @listen_opts
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(opts)
      |> Keyword.put(:ip, ip)

    try do
      case :gen_sctp.open(port, listen_opts) do
        {:ok, sock} ->
          case :gen_sctp.listen(sock, true) do
            :ok ->
              Telemetry.emit(:sctp_listener_created, %{}, %{ip: ip, port: port})
              {:ok, sock}

            {:error, _} = error ->
              _ = close(sock)
              error
          end

        {:error, :eprotonosupport} ->
          {:error, :sctp_not_supported}

        {:error, _} = error ->
          error
      end
    rescue
      _ -> {:error, :sctp_not_supported}
    end
  end

  @doc """
  Returns true when this VM can open an SCTP socket (OTP driver + OS support).

  False on hosts without SCTP (common on some macOS builds); listeners then
  return `{:error, :sctp_not_supported}`. Optional feature - not required to
  use the rest of XSockets.
  """
  @spec available?() :: boolean()
  def available? do
    case listen({127, 0, 0, 1}, 0, []) do
      {:ok, sock} ->
        close(sock)
        true

      {:error, _} ->
        false
    end
  end

  @doc """
  SCTP has no `accept/2`; associations arrive as messages on the listen socket.

  Always returns `{:error, :sctp_not_supported}` so callers use `Sctp.Listener`
  instead of `Acceptor`.

  ## Parameters

    * `_listen_sock` - SCTP listen socket (unused)
    * `_timeout` - accept timeout (unused)
  """
  @impl true
  def accept(_listen_sock, _timeout), do: {:error, :sctp_not_supported}

  @doc """
  Sends `data` on an association.

  `to` may be:

    * `{assoc_id, stream}` - explicit association and stream
    * `%{assoc_id: id, stream: n}` - map form (`stream` defaults to `0`)
    * `nil` / other - assoc `0`, stream `0` (single-assoc convenience)
  """
  @impl true
  def send(socket, data, to) do
    {assoc_id, stream} = resolve_dest(to)

    try do
      :gen_sctp.send(socket, assoc_id, stream, data)
    rescue
      _ -> {:error, :sctp_not_supported}
    end
  end

  @doc false
  @impl true
  def setopts(socket, opts), do: :inet.setopts(socket, opts)

  @doc false
  @impl true
  def sockname(socket), do: :inet.sockname(socket)

  @doc false
  @impl true
  def peername(socket), do: :inet.peername(socket)

  @doc false
  @impl true
  def close(socket) do
    try do
      :gen_sctp.close(socket)
    catch
      _, _ -> :ok
    end

    Telemetry.emit(:socket_closed, %{protocol: :sctp}, %{})
    :ok
  end

  @doc false
  @impl true
  def controlling_process(socket, pid), do: :gen_sctp.controlling_process(socket, pid)

  @doc """
  Outbound SCTP connect (optional `Transport` callback).

  Opens a local socket, then calls `:gen_sctp.connect/5`. Returns `{:ok, socket}`
  on success. Prefer `connect_assoc/3` when the caller needs the association id
  immediately (connect may consume the COMM_UP mailbox event).
  """
  @impl true
  def connect(ip, port, opts \\ []) do
    case connect_assoc(ip, port, opts) do
      {:ok, socket, _assoc_id} -> {:ok, socket}
      {:error, _} = error -> error
    end
  end

  @doc """
  Like `connect/3` but also returns the association id from `:gen_sctp.connect/5`.
  """
  @spec connect_assoc(:inet.ip_address(), :inet.port_number(), keyword()) ::
          {:ok, socket :: term(), assoc_id :: term()} | {:error, term()}
  def connect_assoc(ip, port, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    open_opts =
      opts
      |> Keyword.drop([:timeout, :ip])
      |> Keyword.put_new(:active, true)

    try do
      open_opts =
        @listen_opts
        |> Keyword.merge(buffer_opts())
        |> Keyword.merge(open_opts)
        |> Keyword.put(:ip, {0, 0, 0, 0})

      case :gen_sctp.open(0, open_opts) do
        {:ok, socket} ->
          case :gen_sctp.connect(socket, ip, port, [], timeout) do
            {:ok, assoc} ->
              {:ok, socket, assoc_id_from_connect(assoc)}

            {:error, reason} = error ->
              _ = close(socket)
              normalize_connect_error(error, reason)
          end

        {:error, :eprotonosupport} ->
          {:error, :sctp_not_supported}

        {:error, _} = error ->
          error
      end
    rescue
      _ -> {:error, :sctp_not_supported}
    end
  end

  defp assoc_id_from_connect({:sctp_assoc_change, _state, _ew, _ob, _unack, assoc_id}),
    do: assoc_id

  defp normalize_connect_error(_error, {:sctp_assoc_change, :cant_assoc, _, _, _, _}),
    do: {:error, :cant_assoc}

  defp normalize_connect_error(error, _reason), do: error

  @doc false
  @impl true
  def framing(), do: :datagram

  @doc """
  Maps SCTP mailbox messages into `Transport` events (`:data` / `:closed` / `:ignore`).

  Association up/down events are `:ignore` here so the behaviour contract stays
  stable. Use `decode_message/1` from `Sctp.Listener` for association lifecycle.
  """
  @impl true
  def handle_message(msg, _socket) do
    case decode_message(msg) do
      {:data, data, from, _meta} -> {:data, data, from}
      {:closed, reason} -> {:closed, reason}
      _ -> :ignore
    end
  end

  @doc """
  Rich SCTP mailbox decode for association owners.

  Accepts both OTP shapes:

    * `{:sctp, sock, ip, port, {anc, payload}}` (current)
    * `{:sctp, sock, ip, port, anc, payload}` (legacy / docs)

  Returns:

    * `{:data, binary, {ip, port}, meta}` where `meta` may include `:assoc_id` / `:stream`
    * `{:assoc_up, assoc_id, {ip, port}}`
    * `{:assoc_down, assoc_id, reason}`
    * `{:closed, reason}`
    * `:ignore`
  """
  @spec decode_message(term()) ::
          {:data, binary(), {tuple(), integer()}, map()}
          | {:assoc_up, term(), {tuple(), integer()}}
          | {:assoc_down, term(), term()}
          | {:closed, term()}
          | :ignore
  def decode_message({:sctp, _sock, ip, port, {anc, payload}}) do
    decode_payload(ip, port, anc, payload)
  end

  def decode_message({:sctp, _sock, ip, port, anc, payload}) do
    decode_payload(ip, port, anc, payload)
  end

  def decode_message({:sctp_closed, _}), do: {:closed, :normal}
  def decode_message({:sctp_error, _, reason}), do: {:closed, reason}
  def decode_message(_), do: :ignore

  defp decode_payload(ip, port, anc, data) when is_binary(data) do
    {:data, data, {ip, port}, assoc_meta(anc)}
  end

  defp decode_payload(ip, port, _anc, event) when is_tuple(event) do
    case assoc_event(event) do
      {:up, assoc_id} -> {:assoc_up, assoc_id, {ip, port}}
      {:down, assoc_id, reason} -> {:assoc_down, assoc_id, reason}
      :ignore -> :ignore
    end
  end

  defp decode_payload(_ip, _port, _anc, _payload), do: :ignore

  @doc """
  Extracts `{assoc_id, stream}` from SCTP ancillary data when present.
  """
  @spec assoc_meta(term()) :: %{optional(:assoc_id) => integer(), optional(:stream) => integer()}
  def assoc_meta(anc) when is_list(anc) do
    Enum.reduce(anc, %{}, fn
      other, acc when is_tuple(other) ->
        case elem(other, 0) do
          :sctp_sndrcvinfo when tuple_size(other) >= 9 ->
            Map.merge(acc, %{
              stream: elem(other, 1),
              assoc_id: elem(other, tuple_size(other) - 1)
            })

          _ ->
            acc
        end

      _, acc ->
        acc
    end)
  end

  def assoc_meta(_), do: %{}

  defp assoc_event(event) when is_tuple(event) and tuple_size(event) >= 2 do
    case elem(event, 0) do
      :sctp_assoc_change ->
        state = elem(event, 1)
        assoc_id = elem(event, tuple_size(event) - 1)

        cond do
          state in [:comm_up, :restart] ->
            {:up, assoc_id}

          state in [:comm_lost, :shutdown_complete, :shutdown_comp, :cant_assoc] ->
            {:down, assoc_id, state}

          true ->
            :ignore
        end

      :sctp_shutdown_event when tuple_size(event) >= 2 ->
        {:down, elem(event, 1), :shutdown}

      _ ->
        :ignore
    end
  end

  defp assoc_event(_), do: :ignore

  defp resolve_dest({assoc_id, stream}) when is_integer(assoc_id) and is_integer(stream),
    do: {assoc_id, stream}

  defp resolve_dest(%{assoc_id: assoc_id, stream: stream})
       when is_integer(assoc_id) and is_integer(stream),
       do: {assoc_id, stream}

  defp resolve_dest(%{assoc_id: assoc_id}) when is_integer(assoc_id), do: {assoc_id, 0}
  defp resolve_dest(_), do: {0, 0}

  defp buffer_opts do
    size = Config.buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end
end
