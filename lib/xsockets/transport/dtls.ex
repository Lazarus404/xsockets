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

defmodule XSockets.Transport.DTLS do
  @moduledoc """
  DTLS datagram transport for secure datagram listeners.

  ## What problem this solves

  Hosts that need encrypted datagram channels (for example WebRTC or TURN over
  DTLS) use DTLS over UDP. This module is a thin `Transport` adapter: it
  delegates to `Transport.TLS` with `protocol: :dtls`, reports `:datagram`
  framing, reuses TLS certificate and cipher defaults, and exposes `connect/3`
  for outbound DTLS dials.

      iex> XSockets.Transport.DTLS.framing()
      :datagram
      iex> XSockets.Transport.DTLS.handle_message({:ssl, :port, "hi"}, :sock)
      {:data, "hi", nil}

  ## RFCs

  - [RFC 9147](https://www.rfc-editor.org/rfc/rfc9147) - DTLS 1.3
  - Common host use: [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) TURN over DTLS
  """
  @behaviour XSockets.Transport

  alias XSockets.Transport.TLS
  alias XSockets.Telemetry

  @doc false
  @impl true
  def listen(ip, port, opts), do: TLS.listen(ip, port, Keyword.put(opts, :protocol, :dtls))

  @doc false
  @impl true
  def accept(listen_sock, timeout), do: TLS.accept(listen_sock, timeout, :dtls)

  @doc false
  @impl true
  def send(socket, data, to), do: TLS.send(socket, data, to)

  @doc false
  @impl true
  def setopts(socket, opts), do: TLS.setopts(socket, opts)

  @doc false
  @impl true
  def sockname(socket), do: TLS.sockname(socket)

  @doc false
  @impl true
  def peername(socket), do: TLS.peername(socket)

  @doc false
  @impl true
  def close(socket) do
    :ssl.close(socket)
    Telemetry.emit(:socket_closed, %{protocol: :dtls}, %{})
    :ok
  end

  @doc false
  @impl true
  def controlling_process(socket, pid), do: TLS.controlling_process(socket, pid)

  @doc false
  @impl true
  def framing(), do: :datagram

  @doc """
  Outbound DTLS client connect (optional `Transport` callback).

  Delegates to `Transport.TLS.connect/3` with `protocol: :dtls`.
  """
  @impl true
  def connect(ip, port, opts \\ []) do
    TLS.connect(ip, port, Keyword.put(opts, :protocol, :dtls))
  end

  @doc false
  @impl true
  def handle_message(msg, socket), do: TLS.handle_message(msg, socket)
end
