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

defmodule XSockets.TierSupervisor.Pool do
  @moduledoc """
  Named pool supervisor for pipeline tiers with `dispatch: :pool`.

  ## What problem this solves

  High-rate descended tiers (for example RTP parsing) can overload the connection
  process if run inline. `:pool` dispatch caps concurrent `TierSession` workers;
  when the pool is full, descends are dropped (at-most-once) and telemetry
  reports saturation.

  Caps children at `Config.tier_pool_size/0` unless `:max_children` is passed.

  ## Internal note

  Thin wrapper around `TierSupervisor` registered as `TierSupervisor.Pool`.

  ## RFCs

  - No STUN/TURN RFC; OTP supervision for bounded async tier workers
  """

  alias XSockets.{Config, TierSupervisor}

  @doc """
  Starts `TierSupervisor` registered as this module.

  ## Parameters

    * `opts` - `:max_children` override
  """
  def start_link(opts \\ []) do
    max_children = Keyword.get(opts, :max_children, Config.tier_pool_size())

    TierSupervisor.start_link(
      name: __MODULE__,
      max_children: max_children
    )
  end
end
