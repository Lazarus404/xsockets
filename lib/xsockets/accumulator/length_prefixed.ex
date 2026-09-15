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

defmodule XSockets.Accumulator.LengthPrefixed do
  @moduledoc """
  Frames packets using a fixed-size length header.

  The popped packet is the body only (header is stripped). Multiple packets
  in one `push/3` are drained by repeated `pop/1`.

  ## What problem this solves

  TCP and TLS deliver a byte stream with no message boundaries. A length prefix
  (configurable width and endianness) lets the engine split the stream into
  discrete packets before handlers run.

  ## RFCs

  - [RFC 8489](https://www.rfc-editor.org/rfc/rfc8489) (STUN over TCP; 2-byte length, pt. 6)
  - [RFC 5766](https://www.rfc-editor.org/rfc/rfc5766) / [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) (TURN ChannelData 4-byte length)
  - [RFC 9293](https://www.rfc-editor.org/rfc/rfc9293) (TCP byte-stream transport)

  ## Options

    * `:header_size` - header width in bytes (default: `2`)
    * `:length_offset` - byte offset of the length field (default: `0`)
    * `:length_size` - width of the length field in bytes (default: header size)
    * `:endianness` - `:big` or `:little` (default: `:big`)
    * `:length_includes_header` - whether length counts the header (default: `false`)
    * `:max_size` - max buffered bytes before `:buffer_overflow` (default: `65536`)
  """
  @behaviour XSockets.Accumulator

  @default_max 65_536

  @doc """
  Builds an empty byte buffer with the given header layout.

  ## Parameters

    * `opts` - see module options

      iex> alias XSockets.Accumulator.LengthPrefixed
      iex> acc = LengthPrefixed.init(header_size: 2)
      iex> acc = LengthPrefixed.push(acc, <<0, 3, "ABC">>, %{})
      iex> {:ok, "ABC", %{}, acc} = LengthPrefixed.pop(acc)
      iex> elem(LengthPrefixed.pop(acc), 0)
      :more
  """
  @impl true
  def init(opts) do
    header_size = Keyword.get(opts, :header_size, 2)
    length_size = Keyword.get(opts, :length_size, header_size)
    length_offset = Keyword.get(opts, :length_offset, 0)

    %{
      buffer: <<>>,
      meta: %{},
      header_size: header_size,
      length_size: length_size,
      length_offset: length_offset,
      endianness: Keyword.get(opts, :endianness, :big),
      length_includes_header: Keyword.get(opts, :length_includes_header, false),
      max_size: Keyword.get(opts, :max_size, @default_max)
    }
  end

  @doc """
  Appends `chunk` to the buffer and merges `meta`.

  ## Parameters

    * `acc` - state from `init/1`
    * `chunk` - more bytes (may be a partial header or several packets)
    * `meta` - merged into the next successfully popped packet
  """
  @impl true
  def push(%{buffer: buffer} = acc, chunk, meta) do
    buffer = <<buffer::binary, chunk::binary>>
    %{acc | buffer: buffer, meta: Map.merge(acc.meta, meta)}
  end

  @doc """
  Extracts one body when a complete header+payload is buffered.

  ## Parameters

    * `acc` - state after `push/3`
  """
  @impl true
  def pop(%{buffer: buffer, max_size: max_size} = acc) when byte_size(buffer) > max_size do
    {:error, :buffer_overflow, acc}
  end

  def pop(%{buffer: <<>>} = acc), do: {:more, acc}

  def pop(%{buffer: buffer, header_size: header_size} = acc)
      when byte_size(buffer) < header_size do
    {:more, acc}
  end

  def pop(acc) do
    case packet_size(acc) do
      :incomplete ->
        {:more, acc}

      {:ok, total, body_size} ->
        header_size = acc.header_size

        if byte_size(acc.buffer) < total do
          {:more, acc}
        else
          <<_header::binary-size(^header_size), packet::binary-size(^body_size), rest::binary>> =
            acc.buffer

          meta = acc.meta
          {:ok, packet, meta, %{acc | buffer: rest, meta: %{}}}
        end
    end
  end

  defp packet_size(%{
         buffer: buffer,
         header_size: header_size,
         length_size: length_size,
         length_offset: length_offset,
         endianness: endianness,
         length_includes_header: includes_header?
       }) do
    if byte_size(buffer) < header_size do
      :incomplete
    else
      <<header::binary-size(^header_size), _rest::binary>> = buffer
      length_bin = binary_part(header, length_offset, length_size)

      length =
        case {endianness, length_size} do
          {:big, 1} -> :binary.at(length_bin, 0)
          {:little, 1} -> :binary.at(length_bin, 0)
          {:big, 2} -> :binary.decode_unsigned(length_bin, :big)
          {:little, 2} -> :binary.decode_unsigned(length_bin, :little)
          {:big, 4} -> :binary.decode_unsigned(length_bin, :big)
          {:little, 4} -> :binary.decode_unsigned(length_bin, :little)
          _ -> :binary.decode_unsigned(length_bin, endianness)
        end

      body_size =
        if includes_header?, do: max(length - header_size, 0), else: length

      total = header_size + body_size
      {:ok, total, body_size}
    end
  end
end
