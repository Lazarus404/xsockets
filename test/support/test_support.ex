### ----------------------------------------------------------------------
###
### Copyright (c) 2013 - 2020 Jahred Love and Xirsys LLC <experts@xirsys.com>
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

defmodule XSockets.TestSupport do
  @moduledoc false

  defmodule CollectHandler do
    @behaviour XSockets.Handler

    alias XSockets.Conn

    @impl true
    def handle_connect(%Conn{assigns: %{agent: agent}}), do: {:ok, agent}

    @impl true
    def handle_packet(packet, _meta, %Conn{assigns: %{agent: agent}}, _state) do
      Agent.update(agent, fn packets -> [packet | packets] end)
      {:ok, agent}
    end

    @impl true
    def handle_disconnect(_reason, _state), do: :ok
  end

  def start_collector do
    Agent.start_link(fn -> [] end)
  end

  def packets(agent) do
    agent |> Agent.get(fn list -> list end) |> Enum.reverse()
  end

  def frame(body) when is_binary(body) do
    <<byte_size(body)::16, body::binary>>
  end

  def frame_many(bodies) do
    Enum.map(bodies, &frame/1) |> IO.iodata_to_binary()
  end
end
