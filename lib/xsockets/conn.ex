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

defmodule XSockets.Conn do
  @moduledoc """
  Transport-level connection context passed to handlers.

  Built by `Connection` (streams) or `DatagramServer` (datagrams). Handlers
  read addresses and `assigns`; they do not own the socket.

      iex> conn = %XSockets.Conn{client_ip: {127, 0, 0, 1}, client_port: 3478}
      iex> conn.client_port
      3478
      iex> conn.assigns
      %{}

  ## What problem this solves

  Handler callbacks need peer and local 5-tuple data plus caller metadata without
  exposing raw socket ownership. `%Conn{}` is the stable read-only view
  Host handlers use for logging,
  rate limits, and reply routing.

  ## RFCs

  Addresses and ports identify the peer and local endpoint for logging, rate
  limits, and reply routing. Common host use includes ICE/STUN/TURN 5-tuples
  ([RFC 8489](https://www.rfc-editor.org/rfc/rfc8489),
  [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656),
  [RFC 8445](https://www.rfc-editor.org/rfc/rfc8445)).
  """

  @typedoc """
  Connection context passed to `XSockets.Handler` callbacks.

  ## Fields

  * `:listener` - `Acceptor` or `DatagramServer` pid, if any
  * `:socket` - opaque transport socket handle
  * `:client_ip` - peer IP; updated per datagram on UDP
  * `:client_port` - peer port; updated per datagram on UDP
  * `:server_ip` - local bind IP
  * `:server_port` - local bind port
  * `:assigns` - caller-supplied map; unchanged by the engine
  """
  @type t :: %__MODULE__{
          listener: pid() | nil,
          socket: term(),
          client_ip: :inet.ip_address() | nil,
          client_port: :inet.port_number() | nil,
          server_ip: :inet.ip_address() | nil,
          server_port: :inet.port_number() | nil,
          assigns: map()
        }

  defstruct listener: nil,
            socket: nil,
            client_ip: nil,
            client_port: nil,
            server_ip: nil,
            server_port: nil,
            assigns: %{}
end
