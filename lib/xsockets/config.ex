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

defmodule XSockets.Config do
  @moduledoc """
  Runtime configuration for `:xsockets`.

  Lookup order for `get/2`: host app (`:config_app`), then `:xsockets`,
  then the caller's `default`.

  Set `config :xsockets, config_app: :my_app` so `config :my_app, ...`
  overrides library defaults without copying every key.

  ## What problem this solves

  Deployments need tunable buffer sizes, UDP session limits, reorder windows,
  and control-plane rate limits without hard-coding constants in transport and
  engine modules.

  ## RFCs

  No transport RFC; operational defaults for socket listeners.
  Rate limiting is intended for control-plane request bursts, not high-rate
  media (see `check_rate_limit/1`).
  """

  @default_reorder [
    enabled: true,
    window: 16,
    max_delay_ms: 100,
    on_overflow: :flush_oldest
  ]

  @reorder_keys [:enabled, :window, :max_delay_ms, :on_overflow]

  @doc """
  Looks up `key` with host-app precedence.

  ## Parameters

    * `key` - atom stored under `:xsockets` or the host `:config_app`
    * `default` - returned when neither application defines `key`

      iex> XSockets.Config.get(:__absent_doctest_key__, :fallback)
      :fallback
  """
  @spec get(atom(), any()) :: any()
  def get(key, default \\ nil) do
    case host_env(key) do
      {:ok, value} ->
        value

      :error ->
        Application.get_env(:xsockets, key, default)
    end
  end

  @doc """
  TLS/DTLS handshake timeout in milliseconds. Default `10_000`.
  """
  @spec ssl_handshake_timeout() :: pos_integer()
  def ssl_handshake_timeout(), do: get(:ssl_handshake_timeout, 10_000)

  @doc """
  Per-connection receive/send buffer size in bytes. Default `262_144`.
  """
  @spec buffer_size() :: pos_integer()
  def buffer_size(), do: get(:buffer_size, 256 * 1024)

  @doc """
  Buffer size for shared listener sockets (UDP listen, TCP accept).

  Defaults to `4_194_304` so the kernel queue can absorb bursts while workers
  drain. Relay sockets set their own buffers in `Transport.UDP.open_relay/2`.
  """
  @spec listener_buffer_size() :: pos_integer()
  def listener_buffer_size(), do: get(:listener_buffer_size, 4 * 1024 * 1024)

  @doc """
  Default `:pool` dispatch size when a tier omits `:pool_size`.
  Falls back to `System.schedulers_online/0`.
  """
  @spec tier_pool_size() :: pos_integer()
  def tier_pool_size(), do: get(:tier_pool_size, System.schedulers_online())

  @doc """
  Idle lifetime for per-peer UDP sessions, in milliseconds. Default `30_000`.
  """
  @spec udp_session_idle_ms() :: pos_integer()
  def udp_session_idle_ms(), do: get(:udp_session_idle_ms, 30_000)

  @doc """
  Maximum concurrent per-peer UDP sessions. Default `100_000`.
  """
  @spec max_udp_sessions() :: pos_integer()
  def max_udp_sessions(), do: get(:max_udp_sessions, 100_000)

  @doc """
  Interval for sweeping idle UDP sessions, in milliseconds. Default `5_000`.
  """
  @spec udp_session_sweep_ms() :: pos_integer()
  def udp_session_sweep_ms(), do: get(:udp_session_sweep_ms, 5_000)

  @doc """
  UDP `{active, N}` batch size. `1` means `active: :once`. Default `32`.
  """
  @spec udp_active_n() :: pos_integer()
  def udp_active_n() do
    case get(:udp_active_n, 32) do
      n when is_integer(n) and n >= 1 -> n
      _ -> 32
    end
  end

  @doc """
  Socket options to re-arm UDP/TCP listeners and relay sockets after each read.

  Uses `{active, N}` when `udp_active_n/0` > 1, else `active: :once`.
  """
  @spec active_socket_opts() :: keyword()
  def active_socket_opts() do
    case udp_active_n() do
      1 -> [:binary, active: :once]
      n -> [:binary, active: n]
    end
  end

  @doc """
  Whether `XSockets.Application` starts Sock/Tier supervisors. Default `true`.
  """
  @spec start_supervisors?() :: boolean()
  def start_supervisors?(), do: get(:start_supervisors, true)

  @doc """
  When true, `Engine.push_and_drain/9` calls the configured `rate_limiter`
  module (default `XSockets.RateLimit.FixedWindow`) when `conn.client_ip` is
  set. Default `true`.
  """
  @spec engine_rate_limit?() :: boolean()
  def engine_rate_limit?(), do: get(:engine_rate_limit, true)

  @doc """
  Module implementing `XSockets.RateLimit`. Default `XSockets.RateLimit.FixedWindow`.
  """
  @spec rate_limiter() :: module()
  def rate_limiter, do: get(:rate_limiter, XSockets.RateLimit.FixedWindow)

  @doc """
  Interval for sweeping stale rate-limit ETS rows. Default `60_000` ms.
  """
  @spec rate_limit_sweep_interval_ms() :: pos_integer()
  def rate_limit_sweep_interval_ms(), do: get(:rate_limit_sweep_interval_ms, 60_000)

  @doc """
  Default pool overflow policy for `dispatch: :pool` tiers without an explicit
  `:on_overflow`. One of `:drop` or `:inline_fallback`. Default `:drop`.
  """
  @spec pool_on_overflow() :: :drop | :inline_fallback
  def pool_on_overflow do
    case get(:pool_on_overflow, :drop) do
      :inline_fallback -> :inline_fallback
      _ -> :drop
    end
  end

  @doc """
  Optional `:send_timeout` (ms) applied before `Engine` replies on stream
  sockets. `nil` leaves the socket default. Default `5000`.
  """
  @spec send_timeout_ms() :: pos_integer() | nil
  def send_timeout_ms(), do: get(:send_timeout_ms, 5_000)

  @doc """
  Max queued outbound iodata entries on a stream `Connection` write queue.
  Overflow drops the newest entry and emits `:write_queue_overflow`. Default `32`.
  """
  @spec write_queue_max() :: pos_integer()
  def write_queue_max(), do: get(:write_queue_max, 32)

  @doc """
  UDP outbound send-failure policy for per-peer write queues.

  * `:retry_peer` (default) - keep the queue and retry with backoff; always re-arm reads
  * `:drop` - clear the peer queue on send error (prefer listener liveness only)
  """
  @spec datagram_write_on_error() :: :retry_peer | :drop
  def datagram_write_on_error do
    case get(:datagram_write_on_error, :retry_peer) do
      :drop -> :drop
      _ -> :retry_peer
    end
  end

  @doc """
  Whether `check_rate_limit/1` is armed. Default `true`.
  """
  @spec rate_limit_enabled?() :: boolean()
  def rate_limit_enabled?(), do: get(:rate_limit_enabled, true)

  @doc """
  Advertised IPv4 bind address. Default `{0, 0, 0, 0}`.
  """
  @spec server_ip() :: :inet.ip_address()
  def server_ip(), do: get(:server_ip, {0, 0, 0, 0})

  @doc """
  Local IPv4 address used when `sockname/1` is unavailable. Default `{0, 0, 0, 0}`.
  """
  @spec server_local_ip() :: :inet.ip_address()
  def server_local_ip(), do: get(:server_local_ip, {0, 0, 0, 0})

  @doc """
  Advertised IPv6 bind address. Default `{0, 0, 0, 0, 0, 0, 0, 0}`.
  """
  @spec server_ip6() :: :inet.ip_address()
  def server_ip6(), do: get(:server_ip6, {0, 0, 0, 0, 0, 0, 0, 0})

  @doc """
  Local IPv6 address used when `sockname/1` is unavailable.
  Default `{0, 0, 0, 0, 0, 0, 0, 0}`.
  """
  @spec server_local_ip6() :: :inet.ip_address()
  def server_local_ip6(), do: get(:server_local_ip6, {0, 0, 0, 0, 0, 0, 0, 0})

  @doc """
  Merged reorder tunables for a named tier.

  Precedence (lowest to highest): library defaults, `:xsockets` `:reorder`
  config, host `:config_app` `:reorder` config, explicit keys in `opts`.

  ## Parameters

    * `name` - tier lookup key (e.g. `:rtp`), or `nil` for library defaults only
    * `opts` - explicit overrides (`:enabled`, `:window`, `:max_delay_ms`, `:on_overflow`)

      iex> opts = XSockets.Config.reorder_opts(nil, [])
      iex> opts[:window]
      16
      iex> opts[:enabled]
      true
      iex> opts[:on_overflow]
      :flush_oldest
  """
  @spec reorder_opts(atom() | nil, keyword()) :: keyword()
  def reorder_opts(name, opts) when is_list(opts) do
    lib_opts = reorder_tier(:xsockets, name)
    host_opts = reorder_tier(host_app(), name)
    explicit = Keyword.take(opts, @reorder_keys)

    @default_reorder
    |> Keyword.merge(lib_opts)
    |> Keyword.merge(host_opts)
    |> Keyword.merge(explicit)
  end

  @doc """
  Fixed-window request counter for one client IP.

  Intended for control-plane requests only. Do not apply this to relayed
  media: a request-shaped budget is exhausted in seconds at packet rates.

  Each IP holds one row (window index + count), so check and update are O(1).

  ## Parameters

    * `client_ip` - peer address used as the ETS key

      iex> XSockets.Config.check_rate_limit({203, 0, 113, 1})
      :ok
  """
  @spec check_rate_limit(:inet.ip_address()) :: :ok | {:error, :rate_limited}
  def check_rate_limit(client_ip) do
    XSockets.RateLimit.Window.bump(client_ip)
  end

  @spec host_app() :: atom() | nil
  defp host_app do
    Application.get_env(:xsockets, :config_app)
  end

  @spec host_env(atom()) :: {:ok, term()} | :error
  defp host_env(key) do
    case host_app() do
      nil ->
        :error

      app ->
        case Application.fetch_env(app, key) do
          {:ok, value} -> {:ok, value}
          :error -> :error
        end
    end
  end

  @spec reorder_tier(atom() | nil, atom() | nil) :: keyword()
  defp reorder_tier(_app, nil), do: []

  defp reorder_tier(app, name) when is_atom(app) and is_atom(name) do
    Application.get_env(app, :reorder, [])
    |> Keyword.get(name, [])
  end
end
