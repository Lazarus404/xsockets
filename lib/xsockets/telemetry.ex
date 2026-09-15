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

defmodule XSockets.Telemetry do
  @moduledoc """
  Optional `:telemetry` events under `[:xsockets, event]`.

  `emit/3` no-ops when `config :xsockets, telemetry_enabled: false`, and
  swallows errors if `:telemetry` is not started. `attach_handlers/0` installs
  library log/counter handlers; callers may attach their own instead.

  ## What problem this solves

  Operators need
  connection counts, byte totals, rate-limit hits, and SSL handshake outcomes
  without instrumenting every transport module by hand. This module centralizes
  emit helpers, default handlers, and coarse health heuristics.

  ## RFCs

  No STUN/TURN RFC; operational telemetry for socket listeners.
  """

  require Logger

  alias XSockets.Config

  @enabled_key {__MODULE__, :enabled}

  @doc """
  Executes `[:xsockets, event_name]` when telemetry is enabled.

  ## Parameters

    * `event_name` - last segment of the event name (atom)
    * `measurements` - numeric map (`%{bytes: n}`, ...)
    * `metadata` - context map (`%{ip: tuple, port: n}`, ...)

      iex> XSockets.Telemetry.emit(:doctest_event, %{count: 1}, %{})
      :ok
  """
  @spec emit(atom(), map(), map()) :: :ok
  def emit(event_name, measurements, metadata) do
    if enabled?() do
      :telemetry.execute([:xsockets, event_name], measurements, metadata)
    end

    :ok
  end

  @doc false
  @spec enabled?() :: boolean()
  def enabled?() do
    :persistent_term.get(@enabled_key)
  rescue
    ArgumentError ->
      value = Config.get(:telemetry_enabled, true)
      :persistent_term.put(@enabled_key, value)
      value
  end

  @doc false
  @spec refresh_enabled!() :: :ok
  def refresh_enabled!() do
    :persistent_term.put(@enabled_key, Config.get(:telemetry_enabled, true))
    :ok
  end

  @doc """
  Attaches library log and counter handlers for `[:xsockets, ...]` events.

  Idempotent per handler id. Safe to call from a host application's start.
  """
  def attach_handlers() do
    handlers = [
      # Emitted by transports / acceptor / datagram server
      {[:xsockets, :socket_closed], &handle_socket_closed/4},
      {[:xsockets, :connection_accepted], &handle_connection_accepted/4},
      {[:xsockets, :connection_closed], &handle_connection_closed/4},
      {[:xsockets, :listener_started], &handle_listener_started/4},
      {[:xsockets, :udp_listener_started], &handle_listener_started/4},
      {[:xsockets, :udp_listener_stopped], &handle_listener_stopped/4},
      {[:xsockets, :udp_listener_created], &handle_listener_created/4},
      {[:xsockets, :tcp_listener_created], &handle_listener_created/4},
      {[:xsockets, :tls_listener_created], &handle_listener_created/4},
      {[:xsockets, :sctp_listener_created], &handle_listener_created/4},
      {[:xsockets, :sctp_listener_started], &handle_listener_started/4},
      {[:xsockets, :sctp_assoc_up], &handle_sctp_assoc_up/4},
      {[:xsockets, :sctp_assoc_down], &handle_sctp_assoc_down/4},
      {[:xsockets, :sctp_message_received], &handle_sctp_message_received/4},

      # Data path
      {[:xsockets, :message_sent], &handle_message_sent/4},
      {[:xsockets, :udp_packet_processed], &handle_packet_processed/4},
      {[:xsockets, :udp_sessions_evicted], &handle_cleanup/4},
      {[:xsockets, :oversized_packet], &handle_oversized_packet/4},
      {[:xsockets, :write_queue_overflow], &handle_write_queue_overflow/4},

      # Engine / pipeline
      {[:xsockets, :rate_limit_exceeded], &handle_rate_limit/4},
      {[:xsockets, :send_error], &handle_send_error/4},
      {[:xsockets, :frame_error], &handle_frame_error/4},
      {[:xsockets, :handler_error], &handle_handler_error/4},
      {[:xsockets, :tier_crashed], &handle_tier_crashed/4},
      {[:xsockets, :tier_pool_saturated], &handle_tier_pool_saturated/4},
      {[:xsockets, :tier_supervisor_unavailable], &handle_tier_pool_saturated/4},
      {[:xsockets, :tier_dropped], &handle_tier_dropped/4},

      # TLS / DTLS handshake
      {[:xsockets, :ssl_handshake_success], &handle_ssl_handshake/4},
      {[:xsockets, :ssl_handshake_error], &handle_ssl_error/4},
      {[:xsockets, :ssl_handshake_timeout], &handle_ssl_timeout/4}
    ]

    Enum.each(handlers, fn {event, handler} ->
      :telemetry.attach(
        "xsockets_#{Enum.join(event, "_")}",
        event,
        handler,
        %{}
      )
    end)

    Logger.info("XSockets telemetry handlers attached")
  end

  @doc """
  Detaches every handler whose id starts with `"xsockets_"`.
  """
  def detach_handlers() do
    :telemetry.list_handlers([])
    |> Enum.filter(fn handler ->
      is_binary(handler.id) and String.starts_with?(handler.id, "xsockets_")
    end)
    |> Enum.each(&:telemetry.detach(&1.id))

    Logger.info("XSockets telemetry handlers detached")
  end

  @doc """
  Snapshot of process-local counters kept in `:persistent_term`.

  Keys include connection counts, bytes sent/received, rate-limit hits, and
  SSL handshake totals. `last_updated` is `System.system_time(:second)`.

  Takes no parameters; counters are global to the VM via `:persistent_term`.
  """
  def get_metrics() do
    %{
      total_connections: get_counter(:total_connections),
      active_udp_connections: get_counter(:active_udp_connections),
      active_tcp_connections: get_counter(:active_tcp_connections),
      active_sctp_connections:
        max(get_counter(:sctp_connections) - get_counter(:sctp_assocs_closed), 0),
      messages_sent: get_counter(:messages_sent),
      packets_processed: get_counter(:packets_processed),
      bytes_sent: get_counter(:bytes_sent),
      bytes_received: get_counter(:bytes_received),
      rate_limit_hits: get_counter(:rate_limit_hits),
      ssl_handshake_successes: get_counter(:ssl_handshake_successes),
      ssl_handshake_errors: get_counter(:ssl_handshake_errors),
      send_errors: get_counter(:send_errors),
      sctp_messages_received: get_counter(:sctp_messages_received),
      sctp_bytes_received: get_counter(:sctp_bytes_received),
      last_updated: System.system_time(:second)
    }
  end

  # Event Handlers

  defp handle_socket_closed(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:"#{protocol}_sockets_closed")

    Logger.debug("Socket closed: #{protocol}", metadata)
  end

  defp handle_connection_accepted(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:connections_accepted)
    increment_counter(:total_connections)
    increment_counter(:"#{protocol}_connections_accepted")

    Logger.debug("Connection accepted: #{protocol}", metadata)
  end

  defp handle_connection_closed(_name, _measurements, metadata, _config) do
    increment_counter(:connections_closed)
    Logger.debug("Connection closed", metadata)
  end

  defp handle_message_sent(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    protocol = Map.get(measurements, :protocol, :unknown)

    increment_counter(:messages_sent)
    increment_counter(:bytes_sent, bytes)
    increment_counter(:"#{protocol}_bytes_sent", bytes)

    if bytes > 1024 * 1024 do
      Logger.debug("Large message sent: #{bytes} bytes via #{protocol}", metadata)
    end
  end

  defp handle_packet_processed(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    increment_counter(:packets_processed)
    increment_counter(:bytes_received, bytes)

    Logger.debug("UDP packet processed: #{bytes} bytes", metadata)
  end

  defp handle_sctp_assoc_up(_name, _measurements, metadata, _config) do
    increment_counter(:sctp_connections)
    Logger.debug("SCTP association up", metadata)
  end

  defp handle_sctp_assoc_down(_name, _measurements, _metadata, _config) do
    increment_counter(:sctp_assocs_closed)
  end

  defp handle_sctp_message_received(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    increment_counter(:sctp_messages_received)
    increment_counter(:sctp_bytes_received, bytes)
    Logger.debug("SCTP message received: #{bytes} bytes", metadata)
  end

  defp handle_write_queue_overflow(_name, _measurements, metadata, _config) do
    increment_counter(:write_queue_overflows)
    Logger.warning("Write queue overflow", metadata)
  end

  defp handle_rate_limit(_name, _measurements, metadata, _config) do
    increment_counter(:rate_limit_hits)

    ip = Map.get(metadata, :ip, "unknown")
    Logger.info("Rate limit exceeded for IP: #{inspect(ip)}")
  end

  defp handle_ssl_handshake(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:ssl_handshake_successes)
    increment_counter(:"#{protocol}_handshake_successes")

    Logger.debug("SSL handshake successful: #{protocol}", metadata)
  end

  defp handle_ssl_error(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    error = Map.get(measurements, :error, :unknown)
    increment_counter(:ssl_handshake_errors)

    Logger.warning("SSL handshake failed: #{protocol} - #{inspect(error)}", metadata)
  end

  defp handle_ssl_timeout(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:ssl_handshake_timeouts)

    Logger.warning("SSL handshake timeout: #{protocol}", metadata)
  end

  defp handle_send_error(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    error = Map.get(measurements, :error, :unknown)
    increment_counter(:send_errors)
    increment_counter(:"#{protocol}_send_errors")

    Logger.warning("Send error: #{protocol} - #{inspect(error)}", metadata)
  end

  defp handle_frame_error(_name, _measurements, metadata, _config) do
    increment_counter(:frame_errors)
    Logger.warning("Frame error: #{inspect(metadata)}")
  end

  defp handle_handler_error(_name, _measurements, metadata, _config) do
    increment_counter(:handler_errors)
    Logger.warning("Handler error: #{inspect(metadata)}")
  end

  defp handle_tier_crashed(_name, _measurements, metadata, _config) do
    increment_counter(:tier_crashes)
    Logger.warning("Tier crashed: #{inspect(metadata)}")
  end

  defp handle_tier_pool_saturated(_name, _measurements, metadata, _config) do
    increment_counter(:tier_pool_saturated)
    Logger.info("Tier pool saturated: #{inspect(metadata)}")
  end

  defp handle_tier_dropped(_name, _measurements, metadata, _config) do
    increment_counter(:tier_dropped)
    Logger.info("Tier dropped: #{inspect(metadata)}")
  end

  defp handle_cleanup(_name, measurements, metadata, _config) do
    count = Map.get(measurements, :count, 0)
    increment_counter(:connections_cleaned, count)

    Logger.debug("Cleaned up #{count} stale connections", metadata)
  end

  defp handle_oversized_packet(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    increment_counter(:oversized_packets)
    Logger.warning("Oversized packet: #{bytes} bytes", metadata)
  end

  defp handle_listener_started(_name, _measurements, metadata, _config) do
    ip = Map.get(metadata, :ip, "unknown")
    port = Map.get(metadata, :port, "unknown")
    ssl = Map.get(metadata, :ssl, false)

    listener_type = if ssl, do: "secure", else: "plain"
    Logger.info("#{listener_type} listener started at #{inspect(ip)}:#{port}")

    increment_counter(:listeners_started)
  end

  defp handle_listener_stopped(_name, _measurements, _metadata, _config) do
    increment_counter(:listeners_stopped)
  end

  defp handle_listener_created(_name, _measurements, metadata, _config) do
    increment_counter(:listeners_created)
    Logger.debug("Listener socket created", metadata)
  end

  # Counter management using persistent_term for performance

  defp increment_counter(key, amount \\ 1) do
    :atomics.add(counter_ref(key), 1, amount)
  end

  defp get_counter(key) do
    :atomics.get(counter_ref(key), 1)
  rescue
    ArgumentError -> 0
  end

  defp counter_ref(key) do
    counter_key = {__MODULE__, :counter, key}

    case :persistent_term.get(counter_key, nil) do
      nil ->
        ref = :atomics.new(1, signed: false)
        :persistent_term.put(counter_key, ref)
        ref

      ref ->
        ref
    end
  end

  @doc """
  Erases this module's `:persistent_term` counters.

      iex> XSockets.Telemetry.reset_counters()
      :ok
  """
  def reset_counters() do
    :persistent_term.erase(@enabled_key)
    :ok
  end

  @doc """
  Coarse health atom from current counters.

  Returns `:healthy`, `:degraded` (SSL error rate), `:unhealthy` (send error
  rate), or `:under_attack` (rate-limit hits).

  Takes no parameters; reads counters via `get_metrics/0`.
  """
  def health_status() do
    metrics = get_metrics()

    error_rate = safe_divide(metrics.send_errors, metrics.messages_sent)
    ssl_error_rate = safe_divide(metrics.ssl_handshake_errors, metrics.ssl_handshake_successes)

    cond do
      # > 10% error rate
      error_rate > 0.1 -> :unhealthy
      # > 20% SSL error rate
      ssl_error_rate > 0.2 -> :degraded
      # High rate limiting
      metrics.rate_limit_hits > 100 -> :under_attack
      true -> :healthy
    end
  end

  defp safe_divide(_numerator, 0), do: 0.0
  defp safe_divide(numerator, denominator), do: numerator / denominator
end
