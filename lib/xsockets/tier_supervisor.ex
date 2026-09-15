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

defmodule XSockets.TierSupervisor do
  @moduledoc """
  Dynamic supervisor for async pipeline tier session workers.

  ## What problem this solves

  Pipeline tiers configured with `dispatch: :task` or `:pool` run handler work in
  separate `TierSession` processes so the owning `Connection` or `DatagramServer`
  can keep relaying without blocking. This module is the generic
  `DynamicSupervisor` that starts those workers.

  Use `TierSupervisor.Task` or `TierSupervisor.Pool` for named instances with
  appropriate child limits.

  ## Internal note

  OTP supervision only; tier handlers implement application protocol logic.

  ## RFCs

  - No STUN/TURN RFC; OTP supervision for async pipeline dispatch
  """
  use DynamicSupervisor

  alias XSockets.TierSession

  @doc """
  Starts a named supervisor for tier session workers.

  ## Parameters

    * `opts` - `:name` (required), `:max_children` (default `:infinity`)
  """
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    max_children = Keyword.get(opts, :max_children, :infinity)
    DynamicSupervisor.start_link(__MODULE__, max_children, name: name)
  end

  @doc """
  Starts one `TierSession` under `supervisor`.

  ## Parameters

    * `supervisor` - registered name or pid
    * `opts` - forwarded to `TierSession.start_link/1`
  """
  @spec start_session(atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_session(supervisor, opts) do
    spec = {TierSession, opts}
    DynamicSupervisor.start_child(supervisor, spec)
  end

  @doc false
  @impl true
  def init(max_children) do
    DynamicSupervisor.init(strategy: :one_for_one, max_children: max_children)
  end
end
