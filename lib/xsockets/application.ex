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

defmodule XSockets.Application do
  @moduledoc """
  OTP application that optionally starts socket supervisors.

  ## What problem this solves

  Hosts previously had to start `SockSupervisor` and `TierSupervisor.*` before
  any listener. When `Config.start_supervisors?/0` is true (default), this
  application starts them under `XSockets.Supervisor`. Set
  `config :xsockets, start_supervisors: false` to bring your own tree.
  `XSockets.RateLimit.Table` always starts so rate-limit ETS rows can be swept.

  ## RFCs

  No STUN/TURN RFC; OTP application bootstrap.
  """
  use Application

  alias XSockets.Config

  @doc false
  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(), strategy: :one_for_one, name: XSockets.Supervisor)
  end

  @doc """
  Child specs started when `Config.start_supervisors?/0` is true.

  Returns `[]` when supervisors are disabled so hosts can bring their own tree.
  """
  @spec children() :: [Supervisor.child_spec()]
  def children do
    table = [
      %{
        id: XSockets.RateLimit.Table,
        start: {XSockets.RateLimit.Table, :start_link, [[]]}
      }
    ]

    if Config.start_supervisors?() do
      table ++
        [
          %{
            id: XSockets.SockSupervisor,
            start: {XSockets.SockSupervisor, :start_link, [[]]}
          },
          %{
            id: XSockets.TierSupervisor.Task,
            start: {XSockets.TierSupervisor.Task, :start_link, [[]]}
          },
          %{
            id: XSockets.TierSupervisor.Pool,
            start: {XSockets.TierSupervisor.Pool, :start_link, [[]]}
          }
        ]
    else
      table
    end
  end
end
