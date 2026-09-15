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

defmodule XSockets.Spec do
  @moduledoc """
  Normalizes `{module, opts}` or a bare `module` into `{module, opts}`.

  Used by `Pipeline` when a tier names an accumulator.

  **Internal API.** Host applications declare accumulators in `tier/2`; they do
  not call this module directly.

  ## What problem this solves

  Pipeline tiers accept either `MyAccumulator` or `{MyAccumulator, opts}` in
  source. One resolver keeps compile-time and runtime paths consistent.

  ## RFCs

  No STUN/TURN RFC; framing/dispatch infrastructure for host listeners.
  """

  @typedoc """
  Accumulator (or other) module, optionally with keyword options.
  """
  @type spec :: module() | {module(), keyword()}

  @doc """
  Returns `{module, opts}`, using `[]` when only a module is given.

  ## Parameters

    * `spec` - `SomeModule` or `{SomeModule, keyword()}`

      iex> XSockets.Spec.resolve(XSockets.Accumulator.Raw)
      {XSockets.Accumulator.Raw, []}

      iex> XSockets.Spec.resolve({XSockets.Accumulator.Raw, header_size: 2})
      {XSockets.Accumulator.Raw, [header_size: 2]}
  """
  @spec resolve(spec()) :: {module(), keyword()}
  def resolve({mod, opts}) when is_atom(mod) and is_list(opts), do: {mod, opts}
  def resolve(mod) when is_atom(mod), do: {mod, []}
end
