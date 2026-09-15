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

defmodule XSockets.SockSupervisor do
  @moduledoc """
  Dynamic supervisor for per-connection `Connection` processes.

  ## What problem this solves

  Each accepted TCP/TLS/DTLS client needs an isolated process with its own
  accumulator state and drain loop. `SockSupervisor` is a `DynamicSupervisor`
  that starts `Connection` children on demand from `Acceptor`.

  Start this before `Acceptor`. Tests typically call `start_link/0` once.

  ## Internal note

  OTP supervision only; no wire protocol logic lives here.

  ## RFCs

  - [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) - TURN (one client session per connection)
  - No STUN/TURN RFC for OTP supervision
  """
  use DynamicSupervisor

  alias XSockets.Connection

  @doc """
  Starts the named connection supervisor.

  ## Parameters

    * `opts` - `:name` (default `XSockets.SockSupervisor`)
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Starts one `Connection` child under `supervisor`.

  ## Parameters

    * `supervisor` - registered name or pid (default `XSockets.SockSupervisor`)
    * `opts` - forwarded to `Connection.start_link/1`
  """
  @spec start_connection(atom() | pid(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_connection(supervisor \\ __MODULE__, opts) do
    spec = {Connection, opts}
    DynamicSupervisor.start_child(supervisor, spec)
  end

  @doc false
  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
