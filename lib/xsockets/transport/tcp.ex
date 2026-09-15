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

defmodule XSockets.Transport.TCP do
  @moduledoc """
  Plain TCP transport (`:gen_tcp`) for TURN-over-TCP and outbound clients.

  ## What problem this solves

  TURN can run over TCP (pt.4 of RFC 5766 / RFC 8656). Stream transports need
  listen/accept, byte-oriented reads, and normalized `{:tcp, ...}` messages for
  the shared drain engine. This module implements the `Transport` behaviour on
  top of `:gen_tcp`, including optional outbound `connect/3`.

  IPv6 listen sockets on Linux set `ipv6_v6only`.

  ## RFCs

  - [RFC 5766](https://www.rfc-editor.org/rfc/rfc5766) - TURN (TCP allocations)
  - [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) - TURN (updated TCP usage)
  - [RFC 6062](https://www.rfc-editor.org/rfc/rfc6062) - TURN TCP relay (ConnectionBind)
  """
  @behaviour XSockets.Transport

  alias XSockets.{Config, Telemetry}

  @listen_opts [
    reuseaddr: true,
    keepalive: true,
    backlog: 100,
    active: false,
    nodelay: true
  ]

  @doc false
  @impl true
  def listen(ip, port, opts) do
    listen_opts =
      @listen_opts
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(opts)
      |> with_ip_family(ip)

    case :gen_tcp.listen(port, listen_opts) do
      {:ok, _sock} = ok ->
        Telemetry.emit(:tcp_listener_created, %{}, %{ip: ip, port: port})
        ok

      {:error, _} = error ->
        error
    end
  end

  @doc false
  @impl true
  def accept(listen_sock, timeout) do
    case :gen_tcp.accept(listen_sock, timeout) do
      {:ok, cli} = ok ->
        _ = :inet.setopts(cli, [:binary])
        Telemetry.emit(:connection_accepted, %{protocol: :tcp}, %{})
        ok

      {:error, _} = error ->
        error
    end
  end

  @doc false
  @impl true
  def send(socket, data, _to), do: :gen_tcp.send(socket, data)

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
    :gen_tcp.close(socket)
    Telemetry.emit(:socket_closed, %{protocol: :tcp}, %{})
    :ok
  end

  @doc false
  @impl true
  def controlling_process(socket, pid), do: :gen_tcp.controlling_process(socket, pid)

  @doc false
  @impl true
  def framing(), do: :stream

  @connect_opts [
    active: false,
    nodelay: true
  ]

  @doc """
  Connects to `{ip, port}` with a 5 second timeout.

  ## Parameters

    * `ip` - destination address
    * `port` - destination port
    * `opts` - extra `:gen_tcp.connect/4` options merged after defaults
  """
  @impl true
  def connect(ip, port, opts \\ []) do
    connect_opts =
      @connect_opts
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(opts)
      |> with_ip_family(ip)

    :gen_tcp.connect(ip, port, connect_opts, 5_000)
  end

  @doc """
  Maps `:tcp` / `:tcp_closed` / `:tcp_error` messages.

      iex> XSockets.Transport.TCP.handle_message({:tcp, :port, "hi"}, :sock)
      {:data, "hi", nil}
      iex> XSockets.Transport.TCP.handle_message({:tcp_closed, :port}, :sock)
      {:closed, :normal}
      iex> XSockets.Transport.TCP.handle_message({:tcp_error, :port, :econnreset}, :sock)
      {:closed, :econnreset}
      iex> XSockets.Transport.TCP.handle_message(:other, :sock)
      :ignore
      iex> XSockets.Transport.TCP.framing()
      :stream
  """
  @impl true
  def handle_message({:tcp, _port, data}, _socket), do: {:data, data, nil}
  def handle_message({:tcp_closed, _}, _socket), do: {:closed, :normal}
  def handle_message({:tcp_error, _, reason}, _socket), do: {:closed, reason}
  def handle_message(_, _socket), do: :ignore

  defp buffer_opts do
    size = Config.buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end

  defp with_ip_family(opts, ip) when tuple_size(ip) == 8 do
    # OTP wants the bare atom `:inet6`, not `{:inet6, true}` (that raises :badarg).
    opts =
      opts
      |> Keyword.put(:ip, ip)
      |> maybe_ipv6_v6only()

    [:inet6 | opts]
  end

  defp with_ip_family(opts, ip), do: Keyword.put(opts, :ip, ip)

  defp maybe_ipv6_v6only(opts) do
    case :os.type() do
      {:unix, :linux} -> Keyword.put(opts, :ipv6_v6only, true)
      _ -> opts
    end
  end
end
