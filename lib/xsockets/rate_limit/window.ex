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

defmodule XSockets.RateLimit.Window do
  @moduledoc """
  Shared fixed-window counter over the `:xsockets_rate_limits` ETS table.

  ## Internal

  Used by `Config.check_rate_limit/1` and `XSockets.RateLimit.PerConnection`.
  Hosts should not call this directly; configure a `RateLimit` behaviour module.

  ## What problem this solves

  Multiple limiters need the same O(1) window bump so counts and knobs stay in
  one place instead of drifting across modules. Concurrent first-hits use
  `insert_new` plus retry; same-window bumps use `update_counter` so counts
  do not reset under race.

  ## RFCs

  No transport RFC; operational control-plane protection.
  """

  alias XSockets.Config

  @doc """
  Increments the counter for `key` in the current window.

  Returns `:ok` or `{:error, :rate_limited}` when the key has already hit
  `max_requests_per_window` for this window. No-ops with `:ok` when rate
  limiting is disabled.
  """
  @spec bump(term()) :: :ok | {:error, :rate_limited}
  def bump(key) do
    if Config.rate_limit_enabled?() do
      do_bump(key)
    else
      :ok
    end
  end

  defp do_bump(key) do
    table = XSockets.RateLimit.Table.ensure!()
    window_ms = Config.get(:rate_limit_window, 60_000)
    max_requests = Config.get(:max_requests_per_window, 1000)
    current_window = div(System.monotonic_time(:millisecond), window_ms)

    case :ets.lookup(table, key) do
      [{^key, ^current_window, count}] when count >= max_requests ->
        {:error, :rate_limited}

      [{^key, ^current_window, _count}] ->
        new_count = :ets.update_counter(table, key, {3, 1})

        if new_count > max_requests do
          :ets.insert(table, {key, current_window, max_requests})
          {:error, :rate_limited}
        else
          :ok
        end

      [{^key, _old_window, _}] ->
        :ets.insert(table, {key, current_window, 1})
        :ok

      [] ->
        if :ets.insert_new(table, {key, current_window, 1}) do
          :ok
        else
          do_bump(key)
        end
    end
  end
end
