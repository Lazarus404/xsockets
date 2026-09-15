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

defmodule XSockets.TierSupervisor.Task do
  @moduledoc """
  Named supervisor for pipeline tiers with `dispatch: :task`.

  ## What problem this solves

  Some descended tiers should run asynchronously without a hard concurrency cap.
  `:task` dispatch lazily starts one `TierSession` per tier per owner under this
  supervisor (unbounded by default via `:max_children` `:infinity`).

  ## Internal note

  Thin wrapper around `TierSupervisor` registered as `TierSupervisor.Task`.

  ## RFCs

  - No STUN/TURN RFC; OTP supervision for async tier workers
  """

  alias XSockets.TierSupervisor

  @doc """
  Starts `TierSupervisor` registered as this module.

  ## Parameters

    * `opts` - `:max_children` (default `:infinity`)
  """
  def start_link(opts \\ []) do
    max_children = Keyword.get(opts, :max_children, :infinity)

    TierSupervisor.start_link(
      name: __MODULE__,
      max_children: max_children
    )
  end
end
