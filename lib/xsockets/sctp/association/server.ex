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

defmodule XSockets.Sctp.Association.Server do
  @moduledoc """
  GenServer bridge around `XSockets.Sctp.Association` for a caller-owned DTLS pipe.

  ## What problem this solves

  Sans-IO `Sctp.Association` returns event lists the host must drain. This process
  owns association state, accepts inbound plaintext via `handle_packet/2`, and
  delivers events to an `:owner` pid as `{:xsockets_sctp, events}`.

  Requires the optional Hex dependency `ex_sctp`.

  ## RFCs

  - [RFC 8261](https://www.rfc-editor.org/rfc/rfc8261) - SCTP over DTLS
  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) - WebRTC data channels
  """
  use GenServer

  alias XSockets.Sctp.Association

  @doc """
  Starts the association server.

  ## Options

    * `:owner` - pid that receives `{:xsockets_sctp, events}` (default `self()`)
    * `:role` - `:active` or `:passive` (default `:passive`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "Feeds one DTLS plaintext SCTP packet into the association."
  @spec handle_packet(pid(), binary()) :: :ok
  def handle_packet(pid, data) when is_binary(data), do: GenServer.cast(pid, {:packet, data})

  @doc "Starts the association as initiator (`:active` role)."
  @spec connect(pid()) :: :ok
  def connect(pid), do: GenServer.cast(pid, :connect)

  @doc "Opens a data channel; events (including transmit) go to the owner."
  @spec open_channel(pid(), String.t(), keyword()) :: :ok
  def open_channel(pid, label, opts \\ []) when is_binary(label) do
    GenServer.cast(pid, {:open_channel, label, opts})
  end

  @doc false
  @impl true
  def init(opts) do
    unless Association.available?() do
      {:stop, :ex_sctp_unavailable}
    else
      owner = Keyword.get(opts, :owner, self())
      role = Keyword.get(opts, :role, :passive)
      assoc = Association.new(role: role)
      {:ok, %{owner: owner, assoc: assoc}}
    end
  end

  @doc false
  @impl true
  def handle_cast(:connect, state) do
    {events, assoc} = Association.connect(state.assoc)
    notify(state.owner, events)
    {:noreply, %{state | assoc: assoc}}
  end

  def handle_cast({:packet, data}, state) do
    {events, assoc} = Association.handle_packet(state.assoc, data)
    notify(state.owner, events)
    {:noreply, %{state | assoc: assoc}}
  end

  def handle_cast({:open_channel, label, opts}, state) do
    case Association.open_channel(state.assoc, label, opts) do
      {:error, reason, assoc} ->
        notify(state.owner, [{:error, reason}])
        {:noreply, %{state | assoc: assoc}}

      {events, _channel_ref, assoc} ->
        notify(state.owner, events)
        {:noreply, %{state | assoc: assoc}}
    end
  end

  @doc false
  @impl true
  def handle_info(:sctp_timeout, state) do
    {events, assoc} = Association.handle_timeout(state.assoc)
    notify(state.owner, events)
    {:noreply, %{state | assoc: assoc}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp notify(_owner, []), do: :ok
  defp notify(owner, events), do: send(owner, {:xsockets_sctp, events})
end
