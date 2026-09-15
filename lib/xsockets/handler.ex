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

defmodule XSockets.Handler do
  @moduledoc """
  Business logic for one pipeline tier.

  `handle_packet/4` is required. `handle_connect/1` and `handle_disconnect/2`
  are optional.

  Return values from `handle_packet/4`:

    * `{:ok, state}` - packet consumed, no reply
    * `{:reply, iodata(), state}` - send `iodata` on the same connection
    * `{:descend, tier, payload, state}` - push `payload` into another named tier
    * `{:close, state}` - stop the connection after this packet
    * `{:busy, state}` - packet consumed; stop further drain until the owner
      flushes outbound writes (see `Connection` write queue)

  ## What problem this solves

  After an accumulator yields a whole packet, something must interpret it.
  Host applications implement this behaviour per tier so protocol logic stays
  out of the generic drain engine.

  ## RFCs

  This behaviour only defines the callback contract; it does not parse messages.
  Host handlers commonly implement STUN/TURN/ICE (RFC 8489 / 8656 / 8445) or
  other application protocols atop the same drain loop.
  """

  alias XSockets.Conn

  @typedoc "Opaque handler state, returned from `handle_connect/1` or `handle_packet/4`."
  @type state :: term()

  @typedoc "Packet metadata from the accumulator (`:from`, `:received_at`, ...)."
  @type meta :: map()

  @doc """
  Called once when a stream connection is accepted (not used on stateless UDP).

  ## Parameters

    * `conn` - transport context (`XSockets.Conn`)
  """
  @callback handle_connect(Conn.t()) :: {:ok, state()}

  @doc """
  Handles one whole packet from this tier's accumulator.

  ## Parameters

    * `packet` - framed payload (header stripped for length-prefixed tiers)
    * `meta` - accumulator metadata for this packet
    * `conn` - connection context (addresses, socket, assigns)
    * `state` - previous handler state
  """
  @callback handle_packet(binary(), meta(), Conn.t(), state()) ::
              {:ok, state()}
              | {:reply, iodata(), state()}
              | {:descend, atom(), binary(), state()}
              | {:close, state()}
              | {:busy, state()}

  @doc """
  Called when the connection process exits.

  ## Parameters

    * `reason` - exit reason (`:normal`, `:tcp_closed`, ...)
    * `state` - last handler state
  """
  @callback handle_disconnect(term(), state()) :: :ok

  @optional_callbacks handle_connect: 1, handle_disconnect: 2
end
