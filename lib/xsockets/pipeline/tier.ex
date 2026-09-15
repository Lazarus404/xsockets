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

defmodule XSockets.Pipeline.Tier do
  @moduledoc """
  One compiled pipeline tier: accumulator, handler, and dispatch strategy.

      iex> tier = %XSockets.Pipeline.Tier{
      ...>   accumulator: XSockets.Accumulator.Raw,
      ...>   accumulator_opts: [],
      ...>   handler: :my_handler
      ...> }
      iex> tier.dispatch
      :inline
      iex> tier.pool_size
      nil

  ## What problem this solves

  Each pipeline layer needs a frozen snapshot of which accumulator module,
  handler module, and dispatch strategy (`:inline`, `:task`, or `:pool`) the
  engine will use at runtime. `%Tier{}` is that snapshot after compile-time
  `tier/2` expansion.

  ## RFCs

  No STUN/TURN RFC; framing/dispatch infrastructure for host listeners.
  """

  @enforce_keys [:accumulator, :accumulator_opts, :handler]
  defstruct [
    :accumulator,
    :accumulator_opts,
    :handler,
    dispatch: :inline,
    pool_size: nil,
    on_overflow: :drop,
    task_supervisor: XSockets.TierSupervisor.Task,
    pool_supervisor: XSockets.TierSupervisor.Pool
  ]

  @typedoc """
  Runtime tier record produced by `Pipeline.tier/2`.

  ## Fields

  * `:accumulator` - `XSockets.Accumulator` module for this tier
  * `:accumulator_opts` - keyword list passed to `accumulator.init/1`
  * `:handler` - `XSockets.Handler` implementation module
  * `:dispatch` - `:inline` (same process), `:task`, or `:pool`
  * `:pool_size` - max pool children when `dispatch: :pool`; `nil` uses config
  * `:on_overflow` - `:drop` or `:inline_fallback` when the pool is full
  * `:task_supervisor` - named supervisor for `dispatch: :task`
  * `:pool_supervisor` - named supervisor for `dispatch: :pool`
  """
  @type t :: %__MODULE__{
          accumulator: module(),
          accumulator_opts: keyword(),
          handler: module(),
          dispatch: :inline | :task | :pool,
          pool_size: pos_integer() | nil,
          on_overflow: :drop | :inline_fallback,
          task_supervisor: atom(),
          pool_supervisor: atom()
        }
end
