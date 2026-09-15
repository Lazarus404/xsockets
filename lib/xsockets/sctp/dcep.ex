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

defmodule XSockets.Sctp.Dcep do
  @moduledoc """
  Data Channel Establishment Protocol (DCEP) encode and decode.

  ## What problem this solves

  WebRTC data channels negotiate labels, ordering, and reliability with DCEP
  messages on SCTP PPI 50 before user data flows. This module parses and builds
  DCEP Open and Ack payloads used by `Sctp.Association`.

  ## RFCs

  - [RFC 8832](https://www.rfc-editor.org/rfc/rfc8832) - DCEP (Open, Ack wire format)
  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) - WebRTC data channels (SCTP usage)
  """

  defmodule Ack do
    @moduledoc """
    DCEP Data Channel Ack struct and wire helpers.

    ## Internal

    Used by `XSockets.Sctp.Dcep.encode/1` and `decode/1`.

    ## What problem this solves

    Carries the empty Ack payload (message type 0x02) as a typed value so
    association logic can pattern-match without raw binaries.

    ## RFCs

    - [RFC 8832](https://www.rfc-editor.org/rfc/rfc8832) - DCEP Ack
    """

    @typedoc """
    DCEP Data Channel Ack (message type 0x02).

    ## Fields

    Empty struct; the wire form is a single type byte.
    """
    @type t :: %__MODULE__{}

    defstruct []

    @doc false
    def decode(<<>>), do: {:ok, %__MODULE__{}}
    def decode(<<_, _, _>>), do: {:ok, %__MODULE__{}}
    def decode(_), do: :error

    @doc false
    def encode(%__MODULE__{}), do: <<0x02>>
  end

  defmodule Open do
    @moduledoc """
    DCEP Data Channel Open struct and wire helpers.

    ## Internal

    Used by `XSockets.Sctp.Dcep.encode/1` and `decode/1`.

    ## What problem this solves

    Represents an Open message (type 0x03) with reliability, order, label, and
    protocol fields so hosts and `Sctp.Association` share one decoded shape.

    ## RFCs

    - [RFC 8832](https://www.rfc-editor.org/rfc/rfc8832) - DCEP Open
    """

    @typedoc """
    DCEP Data Channel Open (message type 0x03).

    ## Fields

      * `reliability` - `:reliable`, `:rexmit`, or `:timed`
      * `order` - `:ordered` or `:unordered`
      * `label` - channel label (UTF-8)
      * `protocol` - subprotocol string (often empty)
      * `priority` - DCEP priority field
      * `param` - reliability parameter (retransmit count or lifetime ms)
    """
    @type t :: %__MODULE__{
            reliability: :reliable | :rexmit | :timed,
            order: :ordered | :unordered,
            label: String.t(),
            protocol: String.t(),
            priority: non_neg_integer(),
            param: non_neg_integer()
          }

    @enforce_keys [:reliability, :order, :label, :protocol, :priority, :param]
    defstruct @enforce_keys

    @doc false
    def decode(<<ch_type::8, priority::16, param::32, label_len::16, proto_len::16, rest::binary>>) do
      with {:ok, reliability, order} <- channel_type(ch_type),
           <<label::binary-size(^label_len), rest::binary>> <- rest,
           <<protocol::binary-size(^proto_len)>> <- rest do
        {:ok,
         %__MODULE__{
           reliability: reliability,
           order: order,
           param: param,
           label: label,
           protocol: protocol,
           priority: priority
         }}
      else
        _ -> :error
      end
    end

    def decode(_), do: :error

    @doc false
    def encode(%__MODULE__{} = open) do
      ch_type = from_channel_type(open.reliability, open.order)
      label_len = byte_size(open.label)
      proto_len = byte_size(open.protocol)

      <<0x03::8, ch_type::8, open.priority::16, open.param::32, label_len::16, proto_len::16,
        open.label::binary, open.protocol::binary>>
    end

    defp channel_type(0x00), do: {:ok, :reliable, :ordered}
    defp channel_type(0x80), do: {:ok, :reliable, :unordered}
    defp channel_type(0x01), do: {:ok, :rexmit, :ordered}
    defp channel_type(0x81), do: {:ok, :rexmit, :unordered}
    defp channel_type(0x02), do: {:ok, :timed, :ordered}
    defp channel_type(0x82), do: {:ok, :timed, :unordered}
    defp channel_type(_), do: :error

    defp from_channel_type(:reliable, :ordered), do: 0x00
    defp from_channel_type(:reliable, :unordered), do: 0x80
    defp from_channel_type(:rexmit, :ordered), do: 0x01
    defp from_channel_type(:rexmit, :unordered), do: 0x81
    defp from_channel_type(:timed, :ordered), do: 0x02
    defp from_channel_type(:timed, :unordered), do: 0x82
  end

  @doc """
  Decodes a DCEP message from an SCTP user message body (after the type byte).

  Returns `{:ok, %Ack{}}` or `{:ok, %Open{}}`, or `:error` on malformed input.
  """
  @spec decode(binary()) :: {:ok, Ack.t() | Open.t()} | :error
  def decode(<<0x03::8, rest::binary>>), do: Open.decode(rest)
  def decode(<<0x02::8, rest::binary>>), do: Ack.decode(rest)
  def decode(_), do: :error

  @doc """
  Encodes a DCEP struct (`Ack` or `Open`) to wire bytes.
  """
  @spec encode(Ack.t() | Open.t()) :: binary()
  def encode(%mod{} = msg), do: mod.encode(msg)
end
