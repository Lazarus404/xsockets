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

defmodule XSockets.Client do
  @moduledoc """
  Outbound dialer that runs the same drain loop as accepted connections.

  ## What problem this solves

  Hosts that dial out (for example TCP clients) need to connect and then
  frame/dispatch with the same `Engine` path as inbound `Acceptor` children.
  `connect/1` dials via the transport, then starts a `Connection` under
  `SockSupervisor`.

  ## RFCs

  - No dialer RFC; optional host use includes [RFC 6062](https://www.rfc-editor.org/rfc/rfc6062) TURN TCP
  """

  alias XSockets.SockSupervisor

  @doc """
  Dials `ip`/`port` and starts a `Connection` for the socket.

  Required: `:transport`, `:ip`, `:port`, and either `:pipeline` or
  `:accumulator` + `:handler`.

  Supports transports that implement optional `connect/3` (`XSockets.Transport.TCP`,
  `XSockets.Transport.TLS`). Merge `XSockets.Transport.TLS.security_opts/1` into
  `:connect_opts` for server or mutual TLS.
  """
  @spec connect(keyword()) :: {:ok, pid()} | {:error, term()}
  def connect(opts) when is_list(opts) do
    transport = Keyword.fetch!(opts, :transport)
    ip = Keyword.fetch!(opts, :ip)
    port = Keyword.fetch!(opts, :port)
    connect_opts = Keyword.get(opts, :connect_opts, [])

    unless function_exported?(transport, :connect, 3) do
      {:error, :connect_not_supported}
    else
      case transport.connect(ip, port, connect_opts) do
        {:ok, socket} ->
          conn_opts =
            opts
            |> Keyword.take([
              :pipeline,
              :accumulator,
              :handler,
              :assigns,
              :handler_state,
              :tick_interval_ms
            ])
            |> Keyword.merge(
              transport: transport,
              socket: socket,
              listener: self()
            )

          case SockSupervisor.start_connection(conn_opts) do
            {:ok, pid} ->
              case maybe_controlling_process(transport, socket, pid) do
                :ok ->
                  _ = transport.setopts(socket, XSockets.Config.active_socket_opts())
                  {:ok, pid}

                {:error, reason} ->
                  GenServer.stop(pid)
                  _ = transport.close(socket)
                  {:error, reason}
              end

            {:error, _} = err ->
              _ = transport.close(socket)
              err
          end

        {:error, _} = err ->
          err
      end
    end
  end

  defp maybe_controlling_process(transport, socket, pid) do
    if function_exported?(transport, :controlling_process, 2) do
      transport.controlling_process(socket, pid)
    else
      :ok
    end
  end
end
