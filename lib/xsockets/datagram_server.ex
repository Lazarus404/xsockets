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

defmodule XSockets.DatagramServer do
  @moduledoc """
  Datagram listener that drains every whole packet from each inbound datagram.

  ## What problem this solves

  UDP (and similar datagram) listeners deliver one or more logical messages per
  datagram. A single GenServer owns the listen socket, arms active-once reads,
  and runs the shared `Engine` drain loop so coalesced packets in one datagram
  are all dispatched before the next receive. Multi-tier pipelines can keep
  per-peer session state; otherwise each datagram is framed independently.

  Default transport is `Transport.UDP`. Use `socket/1` and `endpoint/1` when a
  reply must leave a different local endpoint (for example RFC 5780).

  Stream write backpressure (`{:busy, state}` / write queues) differs here:
  datagram listeners always re-arm `{active, N}` rather than stalling the socket.
  On outbound send failure, `Config.datagram_write_on_error/0` is `:retry_peer`
  (keep the peer queue and retry) or `:drop` (clear the queue). Plain
  `{:busy, state}` without iodata is a no-op on UDP.

  ## RFCs

  - [RFC 768](https://www.rfc-editor.org/rfc/rfc768) - UDP
  - Common host use: [RFC 8489](https://www.rfc-editor.org/rfc/rfc8489) STUN,
    [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) TURN,
    [RFC 5780](https://www.rfc-editor.org/rfc/rfc5780) NAT discovery
  """
  use GenServer
  require Logger

  alias XSockets.{Config, Conn, Engine, Pipeline, Pipeline.Tier, Telemetry}

  @max_packet_size 64 * 1024

  # System.monotonic_time/1 is relative to an arbitrary, possibly-negative
  # origin, so 0 is not a safe "unknown" sentinel: on a VM where `now` is
  # itself negative, `now - 0` reads as *fresher* than any real entry and a
  # malformed entry would never age out. Use a value far below any realistic
  # monotonic time so a malformed entry always looks maximally stale and gets
  # reclaimed by the next sweep instead of leaking.
  @min_last_seen -9_223_372_036_854_775_808

  @doc """
  Opens a datagram listener.

  ## Parameters

  `opts` is a keyword list:

    * `:ip` / `:port` - bind address (required)
    * `:transport` - transport module (default `Transport.UDP`)
    * `:pipeline` - pipeline module, or omit and pass `:accumulator` + `:handler`
    * `:listen_opts` - extra options forwarded to `listen/3`
    * `:assigns` - map copied onto each datagram's `Conn`
    * `:handler_state` - initial handler state for stateless datagrams
    * `:tick_interval_ms` - optional drain tick (also enables per-peer sessions
      when the pipeline has more than `:root`)
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Bound port of a running server (useful when `:port` was `0`).

  ## Parameters

    * `pid` - server pid from `start_link/1`
  """
  def port(pid), do: GenServer.call(pid, :port)

  @doc """
  Returns the listen socket owned by `pid`.

  Used when a reply must be sent from a different local endpoint than the
  socket that received the datagram (RFC 5780 CHANGE-REQUEST).
  """
  @spec socket(pid()) :: term()
  def socket(pid), do: GenServer.call(pid, :socket)

  @doc """
  Returns `{server_ip, server_port}` for the bound listener.
  """
  @spec endpoint(pid()) :: {:inet.ip_address(), :inet.port_number()}
  def endpoint(pid), do: GenServer.call(pid, :endpoint)

  @doc false
  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    transport_mod = Keyword.get(opts, :transport, XSockets.Transport.UDP)
    ip = Keyword.fetch!(opts, :ip)
    port = Keyword.fetch!(opts, :port)
    listen_opts = Keyword.get(opts, :listen_opts, [])
    pipeline = resolve_pipeline(opts)
    tick_interval_ms = Keyword.get(opts, :tick_interval_ms)
    peer_sessions? = peer_sessions?(tick_interval_ms, pipeline)

    case transport_mod.listen(ip, port, listen_opts) do
      {:ok, socket} ->
        :ok = transport_mod.setopts(socket, Config.active_socket_opts())

        {:ok, server_ip, server_port} = local_endpoint(transport_mod, socket)
        Telemetry.emit(:udp_listener_started, %{}, %{ip: server_ip, port: server_port})

        if tick_interval_ms do
          schedule_tick(tick_interval_ms)
        end

        if peer_sessions? do
          schedule_sweep(Config.udp_session_sweep_ms())
        end

        init_opts = Keyword.take(opts, [:handler_state])

        stateless =
          unless peer_sessions? do
            {accs, states} = Pipeline.fresh_session(pipeline, init_opts)
            %{accs: accs, states: states}
          end

        {:ok,
         %{
           transport: transport_mod,
           socket: socket,
           pipeline: pipeline,
           server_ip: server_ip,
           server_port: server_port,
           assigns: Keyword.get(opts, :assigns, %{}),
           init_opts: init_opts,
           tick_interval_ms: tick_interval_ms,
           peer_sessions?: peer_sessions?,
           sessions: if(peer_sessions?, do: %{}, else: nil),
           session_monitors: %{},
           stateless: stateless,
           write_queues: %{},
           write_retry_ms: 10,
           write_retry_ref: nil
         }}

      {:error, reason} = error ->
        Logger.error("DatagramServer failed to open socket: #{inspect(reason)}")
        error
    end
  end

  @doc false
  @impl true
  def handle_call(:port, _from, state) do
    {:reply, state.server_port, state}
  end

  @doc false
  @impl true
  def handle_call(:socket, _from, state) do
    {:reply, state.socket, state}
  end

  @doc false
  @impl true
  def handle_call(:endpoint, _from, state) do
    {:reply, {state.server_ip, state.server_port}, state}
  end

  @doc false
  @impl true
  def handle_cast({:send, data, ip, port}, state) do
    _ = state.transport.send(state.socket, data, {ip, port})
    {:noreply, state}
  end

  @doc false
  @impl true
  def handle_cast(:stop, state), do: {:stop, :normal, state}

  @doc false
  @impl true
  def handle_info(:flush_datagram_writes, state) do
    state = flush_all_peer_writes(%{state | write_retry_ref: nil})
    rearm(state)
  end

  @doc false
  @impl true
  def handle_info(:tick, state) do
    sessions =
      Enum.reduce(state.sessions, %{}, fn {peer, entry}, sessions ->
        conn = peer_conn(state, peer)
        accs = entry.accs
        states = entry.states
        tier_sessions = entry.tier_sessions
        last_seen = entry_last_seen(entry)

        {accs, states, tier_sessions, _action} =
          Engine.drain(
            state.pipeline,
            :root,
            conn,
            accs,
            states,
            tier_sessions,
            state.transport,
            self()
          )

        Map.put(
          sessions,
          peer,
          new_entry(accs, states, tier_sessions, last_seen, Map.get(entry, :write_queue, :queue.new()))
        )
      end)

    schedule_tick_if_needed(state)
    {:noreply, %{state | sessions: sessions}}
  end

  @doc false
  @impl true
  def handle_info(:sweep, %{sessions: sessions} = state) when is_map(sessions) do
    now = System.monotonic_time(:millisecond)
    idle_ms = Config.udp_session_idle_ms()
    max_sessions = Config.max_udp_sessions()

    peers_to_evict = peers_to_evict(sessions, now, idle_ms, max_sessions)

    state =
      Enum.reduce(peers_to_evict, state, fn peer, state ->
        evict_peer(state, peer)
      end)

    if peers_to_evict != [] do
      Telemetry.emit(:udp_sessions_evicted, %{count: length(peers_to_evict)}, %{})
    end

    schedule_sweep(Config.udp_session_sweep_ms())
    {:noreply, state}
  end

  @doc false
  @impl true
  def handle_info(:sweep, state), do: {:noreply, state}

  @doc false
  @impl true
  def handle_info({:tier_close, _tier_key, reason}, state) do
    {:stop, reason, state}
  end

  @doc false
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.session_monitors, ref) do
      {{peer, tier, ^pid}, monitors} ->
        Telemetry.emit(:tier_crashed, %{}, %{tier: tier, reason: inspect(reason), peer: peer})

        sessions =
          case state.sessions do
            nil ->
              nil

            sessions ->
              case Map.fetch(sessions, peer) do
                {:ok, %{tier_sessions: tier_sessions} = entry} ->
                  if Map.get(tier_sessions, tier) == pid do
                    Map.put(sessions, peer, %{
                      entry
                      | tier_sessions: Map.delete(tier_sessions, tier)
                    })
                  else
                    sessions
                  end

                _ ->
                  sessions
              end
          end

        {:noreply, %{state | session_monitors: monitors, sessions: sessions}}

      {nil, _} ->
        {:noreply, state}
    end
  end

  @doc false
  @impl true
  def handle_info(msg, state) do
    state = maybe_piggyback_datagram_writes(state)

    case state.transport.handle_message(msg, state.socket) do
      {:data, chunk, {client_ip, client_port}} ->
        if byte_size(chunk) > @max_packet_size do
          Telemetry.emit(:oversized_packet, %{bytes: byte_size(chunk)}, %{
            ip: client_ip,
            port: client_port
          })

          rearm(state)
        else
          state = process_datagram(state, chunk, client_ip, client_port)
          state = drain_pending_datagrams(state, Config.udp_active_n() - 1)
          rearm(state)
        end

      :ignore ->
        rearm(state)

      other ->
        Logger.debug("DatagramServer ignored message: #{inspect(other)}")
        rearm(state)
    end
  end

  @doc false
  @impl true
  def terminate(_reason, state) do
    state.transport.close(state.socket)
    Telemetry.emit(:udp_listener_stopped, %{}, %{})
    :ok
  end

  defp process_datagram(state, chunk, client_ip, client_port) do
    conn = peer_conn(state, {client_ip, client_port})
    peer = {client_ip, client_port}
    now = System.monotonic_time(:millisecond)
    meta = %{from: peer, received_at: now}

    old_peer_entry = if state.sessions, do: Map.get(state.sessions, peer), else: nil

    {sessions, accs, states, tier_sessions, _stateless} =
      if state.sessions do
        case old_peer_entry do
          %{accs: accs, states: states, tier_sessions: tier_sessions} ->
            {accs, states, tier_sessions} =
              refresh_root_acc(state.pipeline, accs, states, tier_sessions)

            {state.sessions, accs, states, tier_sessions, nil}

          nil ->
            {accs, states} = Pipeline.fresh_session(state.pipeline, state.init_opts)
            {state.sessions, accs, states, %{}, nil}
        end
      else
        %{accs: accs, states: states} = state.stateless
        {accs, states} = refresh_root_acc_pair(state.pipeline, accs, states)
        {state.sessions, accs, states, %{}, %{accs: accs, states: states}}
      end

    {accs, states, tier_sessions, action} =
      Engine.push_and_drain(
        state.pipeline,
        chunk,
        meta,
        conn,
        accs,
        states,
        tier_sessions,
        state.transport,
        self()
      )

    Telemetry.emit(:udp_packet_processed, %{bytes: byte_size(chunk)}, %{
      ip: client_ip,
      port: client_port
    })

    prior_tier_sessions =
      case old_peer_entry do
        %{tier_sessions: tier_sessions} -> tier_sessions
        _ -> %{}
      end

    write_queue = peer_write_queue(state, peer, old_peer_entry)
    {write_queue, state} = flush_datagram_writes(state, write_queue)

    {write_queue, state} = apply_datagram_action(state, action, peer, write_queue)

    state =
      state
      |> monitor_new_tier_sessions(peer, tier_sessions, prior_tier_sessions)
      |> then(fn state ->
        cond do
          action == :close and is_map(state.sessions) ->
            put_peer_write_queue(
              %{state | sessions: Map.delete(sessions, peer)},
              peer,
              :queue.new()
            )

          state.sessions ->
            put_peer_write_queue(
              %{
                state
                | sessions:
                    Map.put(
                      sessions,
                      peer,
                      new_entry(accs, states, tier_sessions, now, write_queue)
                    )
              },
              peer,
              write_queue
            )

          true ->
            put_peer_write_queue(
              %{state | stateless: %{accs: accs, states: states}},
              peer,
              write_queue
            )
        end
      end)

    state
  end

  defp peer_write_queue(state, peer, old_peer_entry) do
    case old_peer_entry do
      %{write_queue: q} -> q
      _ -> Map.get(state.write_queues, peer, :queue.new())
    end
  end

  defp put_peer_write_queue(state, peer, write_queue) do
    write_queues =
      if :queue.is_empty(write_queue) do
        Map.delete(state.write_queues, peer)
      else
        Map.put(state.write_queues, peer, write_queue)
      end

    %{state | write_queues: write_queues}
  end

  defp apply_datagram_action(state, {:busy, out}, peer, write_queue) do
    max = Config.write_queue_max()

    write_queue =
      if :queue.len(write_queue) >= max do
        Telemetry.emit(:write_queue_overflow, %{}, %{peer: peer})
        write_queue
      else
        :queue.in({out, peer}, write_queue)
      end

    flush_datagram_writes(state, write_queue)
  end

  defp apply_datagram_action(state, _action, _peer, write_queue), do: {write_queue, state}

  @dg_write_retry_steps [10, 25, 50, 100]

  defp maybe_piggyback_datagram_writes(state) do
    if map_size(state.write_queues) == 0 and not has_session_writes?(state) do
      state
    else
      flush_all_peer_writes(state)
    end
  end

  defp has_session_writes?(%{sessions: sessions}) when is_map(sessions) do
    Enum.any?(sessions, fn {_peer, entry} ->
      case entry do
        %{write_queue: q} -> not :queue.is_empty(q)
        _ -> false
      end
    end)
  end

  defp has_session_writes?(_), do: false

  defp flush_all_peer_writes(state) do
    peers =
      MapSet.new(Map.keys(state.write_queues))
      |> MapSet.union(session_write_peers(state))

    Enum.reduce(peers, state, fn peer, state ->
      q = peer_write_queue(state, peer, session_entry(state, peer))
      {q, state} = flush_datagram_writes(state, q)

      state =
        case state.sessions do
          %{^peer => entry} = sessions ->
            %{state | sessions: Map.put(sessions, peer, %{entry | write_queue: q})}

          _ ->
            state
        end

      put_peer_write_queue(state, peer, q)
    end)
  end

  defp session_write_peers(%{sessions: sessions}) when is_map(sessions) do
    sessions
    |> Enum.filter(fn {_p, e} -> match?(%{write_queue: q} when not is_nil(q), e) end)
    |> Enum.filter(fn {_p, %{write_queue: q}} -> not :queue.is_empty(q) end)
    |> Enum.map(&elem(&1, 0))
    |> MapSet.new()
  end

  defp session_write_peers(_), do: MapSet.new()

  defp session_entry(%{sessions: sessions}, peer) when is_map(sessions), do: Map.get(sessions, peer)
  defp session_entry(_, _), do: nil

  defp flush_datagram_writes(state, write_queue) do
    case :queue.out(write_queue) do
      {:empty, q} ->
        {q, state}

      {{:value, {data, peer}}, rest} ->
        case state.transport.send(state.socket, data, peer) do
          :ok ->
            flush_datagram_writes(state, rest)

          {:error, _} ->
            case Config.datagram_write_on_error() do
              :drop ->
                Telemetry.emit(:send_error, %{}, %{
                  peer: peer,
                  dropped: :queue.len(rest) + 1
                })

                {:queue.new(), state}

              :retry_peer ->
                Telemetry.emit(:send_error, %{}, %{peer: peer, retry: true})
                q = :queue.in_r({data, peer}, rest)
                {q, schedule_datagram_write_retry(state)}
            end
        end
    end
  end

  defp schedule_datagram_write_retry(%{write_retry_ref: ref} = state) when is_reference(ref),
    do: state

  defp schedule_datagram_write_retry(state) do
    ms = state.write_retry_ms
    ref = Process.send_after(self(), :flush_datagram_writes, ms)
    next = Enum.find(@dg_write_retry_steps, List.last(@dg_write_retry_steps), &(&1 > ms))
    %{state | write_retry_ref: ref, write_retry_ms: next}
  end

  defp drain_pending_datagrams(state, 0), do: state

  defp drain_pending_datagrams(state, budget) when budget > 0 do
    receive do
      msg ->
        case state.transport.handle_message(msg, state.socket) do
          {:data, chunk, {client_ip, client_port}} when byte_size(chunk) <= @max_packet_size ->
            drain_pending_datagrams(
              process_datagram(state, chunk, client_ip, client_port),
              budget - 1
            )

          _ ->
            send(self(), msg)
            state
        end
    after
      0 -> state
    end
  end

  defp refresh_root_acc_pair(pipeline, accs, states) do
    %Tier{accumulator: root_mod, accumulator_opts: root_opts} =
      Pipeline.tier_spec(pipeline, :root)

    {Map.put(accs, :root, root_mod.init(root_opts)), states}
  end

  defp new_entry(accs, states, tier_sessions, last_seen, write_queue) do
    %{
      accs: accs,
      states: states,
      tier_sessions: tier_sessions,
      last_seen: last_seen,
      write_queue: write_queue
    }
  end

  defp entry_last_seen(%{last_seen: last_seen}) when is_integer(last_seen), do: last_seen
  defp entry_last_seen(_), do: @min_last_seen

  defp peer_sessions?(tick_interval_ms, pipeline) do
    not is_nil(tick_interval_ms) or Pipeline.multi_tier?(pipeline)
  end

  defp refresh_root_acc(pipeline, accs, states, tier_sessions) do
    %Tier{accumulator: root_mod, accumulator_opts: root_opts} =
      Pipeline.tier_spec(pipeline, :root)

    {Map.put(accs, :root, root_mod.init(root_opts)), states, tier_sessions}
  end

  defp monitor_new_tier_sessions(state, peer, new_sessions, old_sessions) do
    Enum.reduce(new_sessions, state, fn {tier, pid}, state ->
      case Map.get(old_sessions, tier) do
        ^pid ->
          state

        _ ->
          ref = Process.monitor(pid)

          monitors = Map.put(state.session_monitors, ref, {peer, tier, pid})
          %{state | session_monitors: monitors}
      end
    end)
  end

  defp peers_to_evict(sessions, now, idle_ms, max_sessions) do
    idle_peers =
      sessions
      |> Enum.filter(fn {_peer, entry} -> now - entry_last_seen(entry) >= idle_ms end)
      |> Enum.map(fn {peer, _} -> peer end)

    idle_set = MapSet.new(idle_peers)

    over_capacity =
      if map_size(sessions) - length(idle_peers) > max_sessions do
        excess = map_size(sessions) - length(idle_peers) - max_sessions

        sessions
        |> Enum.reject(fn {peer, _} -> MapSet.member?(idle_set, peer) end)
        |> Enum.sort_by(fn {_peer, entry} -> entry_last_seen(entry) end)
        |> Enum.take(excess)
        |> Enum.map(fn {peer, _} -> peer end)
      else
        []
      end

    Enum.uniq(idle_peers ++ over_capacity)
  end

  defp evict_peer(state, peer) do
    case Map.pop(state.sessions, peer) do
      {nil, _} ->
        state

      {entry, sessions} ->
        tier_sessions = Map.get(entry, :tier_sessions, %{})

        {to_drop, to_keep} =
          Enum.split_with(state.session_monitors, fn {_ref, {p, _tier, _pid}} -> p == peer end)

        for {ref, _} <- to_drop, do: Process.demonitor(ref, [:flush])

        for {_tier, pid} <- tier_sessions do
          if Process.alive?(pid), do: Process.exit(pid, :shutdown)
        end

        %{state | sessions: sessions, session_monitors: Map.new(to_keep)}
    end
  end

  defp peer_conn(state, {client_ip, client_port}) do
    %Conn{
      listener: self(),
      socket: state.socket,
      client_ip: client_ip,
      client_port: client_port,
      server_ip: state.server_ip,
      server_port: state.server_port,
      assigns: state.assigns
    }
  end

  defp resolve_pipeline(opts) do
    case Keyword.get(opts, :pipeline) do
      nil ->
        accumulator = Keyword.fetch!(opts, :accumulator)
        handler = Keyword.fetch!(opts, :handler)
        Pipeline.resolve({accumulator, handler})

      pipeline_mod when is_atom(pipeline_mod) ->
        Pipeline.resolve(pipeline_mod)
    end
  end

  defp rearm(state) do
    _ = state.transport.setopts(state.socket, Config.active_socket_opts())
    {:noreply, state}
  end

  defp schedule_tick_if_needed(%{tick_interval_ms: nil}), do: :ok

  defp schedule_tick_if_needed(%{tick_interval_ms: interval}) when is_integer(interval) do
    schedule_tick(interval)
  end

  defp schedule_tick(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule_sweep(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :sweep, interval)
  end

  defp local_endpoint(transport_mod, socket) do
    case transport_mod.sockname(socket) do
      {:ok, {ip, port}} -> {:ok, ip, port}
      _ -> {:ok, Config.server_ip(), 0}
    end
  end
end
