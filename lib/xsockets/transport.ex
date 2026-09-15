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

defmodule XSockets.Transport do
  @moduledoc """
  Uniform socket I/O behaviour for TCP, UDP, TLS, DTLS, and SCTP.

  Implementations live under `XSockets.Transport.*`. `Acceptor` and
  `DatagramServer` call these callbacks; they never talk to `:gen_tcp` /
  `:gen_udp` / `:ssl` directly.

  Optional callbacks: `peername/1`, `controlling_process/2`, `connect/3`.

  ## What problem this solves

  Host listeners bind several wire
  protocols. Each OTP driver exposes different APIs and `handle_info/2` message
  shapes. This behaviour gives acceptors, datagram servers, connections, and the
  drain engine one surface for listen/accept/send/setopts and for normalizing
  inbound messages into `{:data, binary(), from()}` events.

  ## RFCs

  This layer moves bytes; STUN/TURN parsing lives in the host application.

  - [RFC 8489](https://www.rfc-editor.org/rfc/rfc8489) (STUN over UDP and TCP)
  - [RFC 5766](https://www.rfc-editor.org/rfc/rfc5766) / [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) (TURN relay transports)
  - [RFC 8445](https://www.rfc-editor.org/rfc/rfc8445) (ICE; transport selection)
  - [RFC 9293](https://www.rfc-editor.org/rfc/rfc9293) (TCP stream framing)
  - [RFC 8446](https://www.rfc-editor.org/rfc/rfc8446) (TLS over TCP)
  - [RFC 9147](https://www.rfc-editor.org/rfc/rfc9147) (DTLS over UDP)
  """

  @typedoc "Opaque socket handle returned by the underlying driver."
  @type socket :: term()

  @typedoc "IPv4 or IPv6 address tuple."
  @type ip_address :: :inet.ip_address()

  @typedoc "UDP/TCP port in `0..65535`."
  @type port_number :: :inet.port_number()

  @typedoc """
  Peer address for a datagram, or `nil` for a connected stream socket.
  """
  @type from :: {ip_address(), port_number()} | nil

  @doc """
  Opens a listening socket.

  ## Parameters

    * `ip` - bind address (`{0, 0, 0, 0}` or `{0, 0, 0, 0, 0, 0, 0, 0}`)
    * `port` - bind port; `0` lets the OS assign one
    * `opts` - extra listen options merged after library defaults
  """
  @callback listen(ip_address(), port_number(), keyword()) ::
              {:ok, socket()} | {:error, term()}

  @doc """
  Accepts one inbound connection from a listen socket.

  Connectionless transports return `{:error, :connectionless}`.

  ## Parameters

    * `socket` - listen socket from `listen/3`
    * `timeout` - milliseconds to wait, or `:infinity`
  """
  @callback accept(socket(), timeout()) :: {:ok, socket()} | {:error, term()}

  @doc """
  Sends `data` on `socket`.

  ## Parameters

    * `socket` - connected or datagram socket
    * `data` - payload
    * `from` - `{ip, port}` for datagrams; ignored (`nil`) on streams
  """
  @callback send(socket(), iodata(), from()) :: :ok | {:error, term()}

  @doc """
  Applies socket options (typically `[:binary, active: :once]` to re-arm).

  ## Parameters

    * `socket` - open socket
    * `opts` - option keyword list understood by the driver
  """
  @callback setopts(socket(), keyword()) :: :ok | {:error, term()}

  @doc """
  Local address of `socket`.

  ## Parameters

    * `socket` - open socket
  """
  @callback sockname(socket()) ::
              {:ok, {ip_address(), port_number()}}
              | {:local, binary()}
              | {:unspec, <<>>}
              | {:undefined, any()}
              | {:error, term()}

  @doc """
  Remote address of a connected `socket`.

  Optional. Datagram transports typically return `{:error, :connectionless}`.

  ## Parameters

    * `socket` - connected socket
  """
  @callback peername(socket()) ::
              {:ok, {ip_address(), port_number()}}
              | {:local, binary()}
              | {:unspec, <<>>}
              | {:undefined, any()}
              | {:error, term()}

  @doc """
  Closes `socket`. Always returns `:ok`.

  ## Parameters

    * `socket` - socket to close
  """
  @callback close(socket()) :: :ok

  @doc """
  Normalizes a driver message into a drain-engine event.

  ## Parameters

    * `message` - raw `handle_info/2` payload (`{:tcp, ...}`, `{:udp, ...}`, ...)
    * `socket` - socket the message belongs to

  ## Returns

    * `{:data, binary(), from()}` - payload ready to push into the accumulator
    * `{:closed, reason}` - peer or error close
    * `{:icmp, map()}` - ICMP error from a datagram socket (relay sockets)
    * `:ignore` - nothing to drain (unknown or control message)
  """
  @callback handle_message(term(), socket()) ::
              {:data, binary(), from()}
              | {:closed, term()}
              | {:icmp, map()}
              | :ignore

  @doc """
  Transfers `socket` ownership to `pid` after accept.

  Optional. Required for stream acceptors that spawn a `Connection` process.

  ## Parameters

    * `socket` - accepted client socket
    * `pid` - process that will receive subsequent messages
  """
  @callback controlling_process(socket(), pid()) :: :ok | {:error, term()}

  @doc """
  Opens an outbound connection.

  Optional. Used when this library is the TCP client.

  ## Parameters

    * `ip` - destination address
    * `port` - destination port
    * `opts` - extra connect options merged after library defaults
  """
  @callback connect(ip_address(), port_number(), keyword()) :: {:ok, socket()} | {:error, term()}

  @doc """
  How this transport frames application payloads on the wire.

    * `:stream` - bytes are a continuous stream; 4-byte alignment is applied
      when a caller asks for it
    * `:datagram` - each send is one message; no padding
  """
  @callback framing() :: :stream | :datagram

  @optional_callbacks peername: 1, controlling_process: 2, connect: 3
end
