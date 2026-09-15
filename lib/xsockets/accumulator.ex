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

defmodule XSockets.Accumulator do
  @moduledoc """
  Per-tier packet framing state machine.

  Built-in implementations: `XSockets.Accumulator.Raw`,
  `XSockets.Accumulator.LengthPrefixed`, and
  `XSockets.Accumulator.Reorder`.

  ## What problem this solves

  Stream and datagram transports deliver arbitrary byte chunks. Before a handler
  runs, something must decide when a logical packet is complete. Each pipeline
  tier owns an accumulator that buffers input via `push/3` and yields whole
  packets from `pop/1`.

  ## RFCs

  Accumulators implement wire framing, not STUN/TURN semantics:

  - [RFC 8489](https://www.rfc-editor.org/rfc/rfc8489) (STUN message length on TCP, pt. 6)
  - [RFC 5766](https://www.rfc-editor.org/rfc/rfc5766) / [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) (TURN ChannelData length fields)

  Choose `LengthPrefixed` or `Raw` to match the tier's on-the-wire layout.

  ## Bounded buffers

  Every implementation should accept a `:max_size` option that limits how much
  data or how many whole packets may be held. Because `push/3` returns only
  `acc()`, overflow is typically recorded during `push/3` (drop oldest, set a
  flag) and surfaced once from the next `pop/1` as
  `{:error, :buffer_overflow, acc}`. The engine emits `:frame_error` telemetry
  and continues draining after overflow.
  """

  @typedoc "Opaque accumulator state. Shape is private to the implementation."
  @type acc :: term()

  @typedoc "Per-packet metadata merged across `push/3` calls until a packet is popped."
  @type meta :: map()

  @doc """
  Builds initial accumulator state.

  ## Parameters

    * `opts` - implementation-specific keyword list (`:max_size`, `:header_size`, ...)
  """
  @callback init(keyword()) :: acc()

  @doc """
  Appends `chunk` and `meta` to the accumulator. Does not extract packets.

  ## Parameters

    * `acc` - current state from `init/1` or a previous `push/3` / `pop/1`
    * `chunk` - inbound bytes (one datagram, or a stream read)
    * `meta` - metadata merged into the next popped packet
  """
  @callback push(acc(), binary(), meta()) :: acc()

  @doc """
  Extracts one whole packet, or reports that more data is needed.

  ## Parameters

    * `acc` - current state

  ## Returns

    * `{:ok, packet, meta, acc}` - one complete packet
    * `{:more, acc}` - incomplete; wait for another `push/3`
    * `{:error, reason, acc}` - framing error (`:buffer_overflow`, ...); drain continues
  """
  @callback pop(acc()) ::
              {:ok, binary(), meta(), acc()}
              | {:more, acc()}
              | {:error, term(), acc()}
end
