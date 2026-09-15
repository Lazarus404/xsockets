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

defmodule XSockets.Transport.UDP do
  @moduledoc """
  UDP transport (`:gen_udp`) for datagram listeners and high-rate relay sockets.

  ## What problem this solves

  Connectionless datagrams need bind/listen, mailbox normalization, and optional
  relay helpers outside the drain loop. This module wraps `:gen_udp` behind the
  `Transport` behaviour.

  `listen/3` opens a client-facing socket (passive until the owner arms active).
  `open_relay/2` opens a relay socket (active-once, large buffers, ICMP error
  queue on Linux). IPv6 listen sockets on Linux set `ipv6_v6only`.

  ## RFCs

  - [RFC 768](https://www.rfc-editor.org/rfc/rfc768) - UDP
  - Common host use: [RFC 8489](https://www.rfc-editor.org/rfc/rfc8489) STUN,
    [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) TURN
  """
  @behaviour XSockets.Transport

  alias XSockets.{Config, Telemetry}

  @listen_opts [active: false]
  @relay_opts [
    buffer: 1024 * 1024,
    recbuf: 1024 * 1024,
    sndbuf: 1024 * 1024
  ]

  @doc false
  @impl true
  def listen(ip, port, opts) do
    listen_opts =
      @listen_opts
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(opts)
      |> with_ip_family(ip)
      |> Keyword.put(:reuseaddr, true)
      |> maybe_reuseport(opts)

    case :gen_udp.open(port, listen_opts) do
      {:ok, sock} = ok ->
        _ = :inet.setopts(sock, [:binary])
        Telemetry.emit(:udp_listener_created, %{}, %{ip: ip, port: port})
        ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Opens a relay UDP socket (active-once, 1 MiB buffers, ICMP errqueue on Linux).

  ## Parameters

    * `ip` - bind address
    * `opts` - extra open options; `:port` (default `0`) is pulled out before merge
  """
  @spec open_relay(:inet.ip_address(), keyword()) :: {:ok, term()} | {:error, term()}
  def open_relay(ip, opts \\ []) do
    port = Keyword.get(opts, :port, 0)
    opts = Keyword.delete(opts, :port)

    listen_opts =
      @relay_opts
      |> Keyword.merge(opts)
      |> with_ip_family(ip)
      |> Keyword.merge(relay_errqueue_opts())

    case :gen_udp.open(port, listen_opts) do
      {:ok, sock} = ok ->
        _ = :inet.setopts(sock, Config.active_socket_opts())
        ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Sets the don't-fragment bit. Returns `{:error, :not_supported}` when the OS
  rejects both the inet option and the raw fallback.

  ## Parameters

    * `socket` - open UDP socket (IPv4 or IPv6)
  """
  @spec set_dont_fragment(port() | {:"$inet", atom(), term()}) ::
          :ok | {:error, :not_supported}
  def set_dont_fragment(socket) do
    case :inet.sockname(socket) do
      {:ok, {{_, _, _, _}, _}} -> set_df_v4(socket)
      {:ok, {{_, _, _, _, _, _, _, _}, _}} -> set_df_v6(socket)
      _ -> {:error, :not_supported}
    end
  end

  @doc """
  Sets IPv4 TOS on `socket`.

  ## Parameters

    * `socket` - open UDP socket
    * `tos` - type-of-service byte
  """
  @spec set_tos(term(), non_neg_integer()) :: :ok | {:error, term()}
  def set_tos(socket, tos), do: :inet.setopts(socket, [{:tos, tos}])

  @doc """
  Sets IPv4 TTL or IPv6 hop limit from the socket's family.

  ## Parameters

    * `socket` - open UDP socket
    * `ttl` - hop limit / TTL
  """
  @spec set_hop_limit(term(), pos_integer()) :: :ok | {:error, term()}
  def set_hop_limit(socket, ttl) do
    case :inet.sockname(socket) do
      {:ok, {{_, _, _, _}, _}} -> :inet.setopts(socket, [{:ttl, ttl}])
      {:ok, {{_, _, _, _, _, _, _, _}, _}} -> :inet.setopts(socket, [{:hoplimit, ttl}])
      _ -> {:error, :einval}
    end
  end

  @doc """
  Sets the IPv6 flow label on `socket`.

  ## Parameters

    * `socket` - open IPv6 UDP socket
    * `label` - 20-bit flow label
  """
  @spec set_flow_label(term(), non_neg_integer()) :: :ok | {:error, term()}
  def set_flow_label(socket, label) do
    case :inet.setopts(socket, [{:flowinfo, label}]) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc false
  @impl true
  def accept(_socket, _timeout), do: {:error, :connectionless}

  @doc false
  @impl true
  def send(socket, data, {ip, port}), do: :gen_udp.send(socket, ip, port, data)

  @doc false
  @impl true
  def setopts(socket, opts), do: :inet.setopts(socket, opts)

  @doc false
  @impl true
  def sockname(socket), do: :inet.sockname(socket)

  @doc false
  @impl true
  def peername(_socket), do: {:error, :connectionless}

  @doc """
  Transfers UDP socket message delivery to `pid` (used for relay ingress).

  ## Parameters

    * `socket` - open UDP socket
    * `pid` - process that will receive `{:udp, ...}` / `{:udp_passive, ...}`
  """
  @impl true
  def controlling_process(socket, pid), do: :gen_udp.controlling_process(socket, pid)

  @doc false
  @impl true
  def close(socket) do
    :gen_udp.close(socket)
    Telemetry.emit(:socket_closed, %{protocol: :udp}, %{})
    :ok
  end

  @doc false
  @impl true
  def framing(), do: :datagram

  @doc """
  Maps `:udp` / `:udp_error` messages. ICMP errors become `{:icmp, info}`.

  `info` is `%{type, code, error_data, peer}`. Integer `error_data` is passed
  through; destination-unreachable / packet-too-big payloads may carry an MTU.

      iex> alias XSockets.Transport.UDP
      iex> UDP.handle_message({:udp, :port, {127, 0, 0, 1}, 3478, "hi"}, :sock)
      {:data, "hi", {{127, 0, 0, 1}, 3478}}
      iex> UDP.handle_message({:udp_error, :sock, {:icmp, 3, 4, 1280, {{8, 8, 8, 8}, 3478}}}, :sock)
      {:icmp, %{type: 3, code: 4, error_data: 1280, peer: {{8, 8, 8, 8}, 3478}}}
      iex> UDP.handle_message(:other, :sock)
      :ignore
      iex> UDP.framing()
      :datagram
  """
  @impl true
  def handle_message({:udp, _port, ip, port, data}, _socket), do: {:data, data, {ip, port}}

  def handle_message({:udp_error, _udp_socket, reason}, _socket) do
    case parse_icmp_error(reason) do
      {:ok, info} -> {:icmp, info}
      :ignore -> :ignore
    end
  end

  def handle_message(_, _socket), do: :ignore

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

  defp relay_errqueue_opts do
    case :os.type() do
      {:unix, :linux} -> [recverr: true]
      _ -> []
    end
  end

  defp set_df_v4(socket) do
    cond do
      match?(:ok, :inet.setopts(socket, [{:dontfrag, true}])) -> :ok
      match?(:ok, :inet.setopts(socket, [{:raw, 0, 10, <<2::native-32>>}])) -> :ok
      true -> {:error, :not_supported}
    end
  end

  defp set_df_v6(socket) do
    cond do
      match?(:ok, :inet.setopts(socket, [{:dontfrag, true}])) -> :ok
      match?(:ok, :inet.setopts(socket, [{:raw, 0, 41, <<2::native-32>>}])) -> :ok
      true -> {:error, :not_supported}
    end
  end

  defp parse_icmp_error({:icmp, type, code, info, peer}) when is_tuple(peer) do
    icmp_payload(type, code, info, peer)
  end

  defp parse_icmp_error({:icmp, type, code, info, ip, port}) do
    icmp_payload(type, code, info, {ip, port})
  end

  defp parse_icmp_error(%{type: type, code: code, info: info, peer: peer}) when is_tuple(peer) do
    icmp_payload(type, code, info, peer)
  end

  defp parse_icmp_error(%{"type" => type, "code" => code, "info" => info, "peer" => peer})
       when is_tuple(peer) do
    icmp_payload(type, code, info, peer)
  end

  defp parse_icmp_error(_), do: :ignore

  defp icmp_payload(type, code, info, peer) do
    {:ok,
     %{
       type: type,
       code: code,
       error_data: icmp_error_data(type, code, info),
       peer: peer
     }}
  end

  defp icmp_error_data(_type, _code, info) when is_integer(info), do: info
  defp icmp_error_data(3, 4, <<mtu::32, _::binary>>), do: mtu
  defp icmp_error_data(2, 0, <<mtu::32, _::binary>>), do: mtu
  defp icmp_error_data(_type, _code, _), do: 0

  defp buffer_opts do
    size = Config.listener_buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end

  defp maybe_reuseport(opts, source) do
    if Keyword.get(source, :reuseport, false) do
      Keyword.put(opts, :reuseport, true)
    else
      opts
    end
  end
end
