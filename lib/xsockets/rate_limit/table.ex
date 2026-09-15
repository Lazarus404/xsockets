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

defmodule XSockets.RateLimit.Table do
  @moduledoc """
  Owns the `:xsockets_rate_limits` ETS table and sweeps stale window rows.

  ## What problem this solves

  Rate-limit rows accumulate one entry per seen key. Without a sweep, scanners
  grow the table forever. This process creates the named table and periodically
  deletes rows whose window index is older than the current window.

  Always started from `XSockets.Application` (independent of SockSupervisor).

  ## RFCs

  No transport RFC; operational housekeeping.
  """
  use GenServer

  alias XSockets.Config

  @table :xsockets_rate_limits

  @doc "Starts the table owner."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Ensures the named ETS table exists; returns the table id."
  @spec ensure!() :: :ets.table()
  def ensure! do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :named_table,
          :public,
          {:write_concurrency, true},
          {:read_concurrency, true}
        ])

      table ->
        table
    end
  end

  @doc "Deletes rows whose stored window is older than the current window."
  @spec sweep() :: non_neg_integer()
  def sweep do
    table = ensure!()
    window = Config.get(:rate_limit_window, 60_000)
    current = div(System.monotonic_time(:millisecond), window)

    :ets.select_delete(table, [
      {
        {:"$1", :"$2", :"$3"},
        [{:<, :"$2", current}],
        [true]
      }
    ])
  end

  @doc false
  @impl true
  def init(_opts) do
    _ = ensure!()
    schedule_sweep()
    {:ok, %{}}
  end

  @doc false
  @impl true
  def handle_info(:sweep, state) do
    _ = sweep()
    schedule_sweep()
    {:noreply, state}
  end

  defp schedule_sweep do
    ms = Config.rate_limit_sweep_interval_ms()
    Process.send_after(self(), :sweep, ms)
  end
end
