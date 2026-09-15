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

defmodule XSockets.Connection do
  @moduledoc """
  Per-connection process: owns accumulator state and fully drains after each read.

  Started by `Acceptor` via `SockSupervisor`. One process per accepted stream
  (TCP/TLS). Datagrams use `DatagramServer` instead.

  **Internal API.** Started by the library acceptor; host applications normally
  configure pipelines on listeners rather than starting this GenServer directly.

  ## What problem this solves

  Stream sockets need a dedicated process to hold per-connection accumulator and
  handler state, re-arm `{active, N}` reads, and drain the full pipeline after
  every chunk so partial frames never stall the acceptor. When outbound sends
  block or the handler returns `{:busy, state}`, a bounded write queue defers
  re-arm until the queue drains. Failed sends retry via piggyback flush on the
  next connection message plus a single coalesced adaptive backoff timer.

  ## RFCs

  - No connection-process RFC; common host use includes STUN/TURN over TCP/TLS
    ([RFC 8489](https://www.rfc-editor.org/rfc/rfc8489),
    [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656)) and
    [RFC 8446](https://www.rfc-editor.org/rfc/rfc8446) TLS
  """
  use GenServer

  alias XSockets.{Config, Conn, Engine, Pipeline, Pipeline.Tier, Telemetry}

  @doc """
  Starts a connection process for an already-accepted `socket`.

  ## Parameters

  `opts` is a keyword list:

    * `:transport` - `XSockets.Transport` module (required)
    * `:socket` - accepted client socket (required)
    * `:pipeline` - pipeline module, or omit and pass `:accumulator` + `:handler`
    * `:listener` - acceptor pid
    * `:assigns` - map copied onto `Conn`
    * `:handler_state` - initial handler state when `handle_connect/1` is absent
    * `:tick_interval_ms` - optional drain tick for reorder timeouts
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Temporary child spec so a dead connection is not restarted.

  ## Parameters

    * `opts` - same keyword list as `start_link/1`
  """
  def child_spec(opts) do
    %{
      id: {__MODULE__, opts},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  @doc false
  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    transport_mod = Keyword.fetch!(opts, :transport)
    socket = Keyword.fetch!(opts, :socket)
    pipeline = resolve_pipeline(opts)

    {:ok, server_ip, server_port} = local_endpoint(transport_mod, socket)
    {:ok, client_ip, client_port} = peer_endpoint(transport_mod, socket)

    conn = %Conn{
      listener: Keyword.get(opts, :listener),
      socket: socket,
      client_ip: client_ip,
      client_port: client_port,
      server_ip: server_ip,
      server_port: server_port,
      assigns: Keyword.get(opts, :assigns, %{})
    }

    {accs, states} = Pipeline.init_session(pipeline, conn, opts)
    tick_interval_ms = Keyword.get(opts, :tick_interval_ms)

    if tick_interval_ms do
      schedule_tick(tick_interval_ms)
    end

    # Acceptor-owned until controlling_process/2; it arms the socket after transfer.
    unless Keyword.get(opts, :listener) do
      :ok = transport_mod.setopts(socket, Config.active_socket_opts())
    end

    {:ok,
     %{
       transport: transport_mod,
       socket: socket,
       pipeline: pipeline,
       conn: conn,
       accs: accs,
       states: states,
       tier_sessions: %{},
       session_monitors: %{},
       tick_interval_ms: tick_interval_ms,
       write_queue: :queue.new(),
       write_retry_ms: 10,
       write_retry_ref: nil
     }}
  end

  @doc false
  @impl true
  def handle_cast({:send, data, ip, port}, state) do
    to = if ip, do: {ip, port}, else: nil
    state = state |> maybe_piggyback_flush() |> enqueue_write(data, to) |> flush_writes()
    maybe_rearm_after_writes(state)
  end

  @doc false
  @impl true
  def handle_cast(:stop, state), do: {:stop, :normal, state}

  @doc false
  @impl true
  def handle_info(:flush_writes, state) do
    state = flush_writes(%{state | write_retry_ref: nil})
    maybe_rearm_after_writes(state)
  end

  @doc false
  @impl true
  def handle_info({:tier_write_busy, out}, state) do
    state =
      state
      |> maybe_piggyback_flush()
      |> enqueue_write(out, reply_to(state.conn))
      |> flush_writes()

    maybe_rearm_after_writes(state)
  end

  @doc false
  @impl true
  def handle_info(:tick, state) do
    state = maybe_piggyback_flush(state)

    {accs, states, tier_sessions, action} =
      Engine.drain(
        state.pipeline,
        :root,
        state.conn,
        state.accs,
        state.states,
        state.tier_sessions,
        state.transport,
        self()
      )

    state =
      state
      |> Map.merge(%{accs: accs, states: states})
      |> apply_tier_sessions(tier_sessions)
      |> apply_drain_action(action)
      |> flush_writes()

    schedule_tick_if_needed(state)
    finish_inbound(state, action)
  end

  @doc false
  @impl true
  def handle_info({:tier_close, _tier_key, reason}, state) do
    safe_disconnect_all(state.pipeline, state.states, reason)
    {:stop, reason, state}
  end

  @doc false
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    state = maybe_piggyback_flush(state)

    case Map.pop(state.session_monitors, ref) do
      {{tier, ^pid}, monitors} ->
        Telemetry.emit(:tier_crashed, %{}, %{tier: tier, reason: inspect(reason)})

        {:noreply,
         %{
           state
           | tier_sessions: Map.delete(state.tier_sessions, tier),
             session_monitors: monitors
         }}

      {nil, _} ->
        {:noreply, state}
    end
  end

  @doc false
  @impl true
  def handle_info(msg, state) do
    state = maybe_piggyback_flush(state)

    case state.transport.handle_message(msg, state.socket) do
      {:data, chunk, from} ->
        conn = update_from(state.conn, from)
        meta = %{from: from, received_at: System.monotonic_time(:millisecond)}

        {accs, states, tier_sessions, action} =
          Engine.push_and_drain(
            state.pipeline,
            chunk,
            meta,
            conn,
            state.accs,
            state.states,
            state.tier_sessions,
            state.transport,
            self()
          )

        state =
          state
          |> Map.merge(%{accs: accs, states: states, conn: conn})
          |> apply_tier_sessions(tier_sessions)
          |> apply_drain_action(action)

        state = drain_pending_stream(state, Config.udp_active_n() - 1)
        state = flush_writes(state)
        finish_inbound(state, action)

      {:closed, reason} ->
        safe_disconnect_all(state.pipeline, state.states, reason)
        {:stop, reason, state}

      {:icmp, _} ->
        rearm(state, :ok)

      :ignore ->
        rearm(state, :ok)

      _ ->
        rearm(state, :ok)
    end
  end

  @doc false
  @impl true
  def terminate(reason, state) do
    safe_disconnect_all(state.pipeline, state.states, reason)
    state.transport.close(state.socket)
    :ok
  end

  defp finish_inbound(state, :close), do: {:stop, :normal, state}

  defp finish_inbound(state, _action) do
    state = maybe_resume_drain(state)
    rearm(state, :ok)
  end

  defp maybe_rearm_after_writes(state) do
    state = maybe_resume_drain(state)
    rearm(state, :ok)
  end

  defp rearm(state, _action) do
    if :queue.is_empty(state.write_queue) do
      _ = state.transport.setopts(state.socket, Config.active_socket_opts())
    end

    {:noreply, state}
  end

  defp apply_drain_action(state, {:busy, out}) do
    enqueue_write(state, out, reply_to(state.conn))
  end

  defp apply_drain_action(state, _action), do: state

  defp reply_to(%Conn{client_ip: ip, client_port: port}) when not is_nil(ip), do: {ip, port}
  defp reply_to(_), do: nil

  defp enqueue_write(state, data, to) do
    max = Config.write_queue_max()

    if :queue.len(state.write_queue) >= max do
      Telemetry.emit(:write_queue_overflow, %{}, %{})
      state
    else
      %{state | write_queue: :queue.in({data, to}, state.write_queue)}
    end
  end

  @write_retry_steps [10, 25, 50, 100]

  defp maybe_piggyback_flush(state) do
    if :queue.is_empty(state.write_queue), do: state, else: flush_writes(state)
  end

  defp flush_writes(state) do
    case :queue.out(state.write_queue) do
      {:empty, _} ->
        clear_write_retry(%{state | write_retry_ms: 10})

      {{:value, {data, to}}, rest} ->
        case state.transport.send(state.socket, data, to) do
          :ok ->
            flush_writes(%{state | write_queue: rest, write_retry_ms: 10})

          {:error, _reason} ->
            state
            |> Map.put(:write_queue, :queue.in_r({data, to}, rest))
            |> schedule_write_retry()
        end
    end
  end

  defp schedule_write_retry(%{write_retry_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_write_retry(state) do
    ms = state.write_retry_ms
    ref = Process.send_after(self(), :flush_writes, ms)
    next_ms = next_write_retry_ms(ms)
    %{state | write_retry_ref: ref, write_retry_ms: next_ms}
  end

  defp next_write_retry_ms(ms) do
    Enum.find(@write_retry_steps, List.last(@write_retry_steps), fn step -> step > ms end)
  end

  defp clear_write_retry(%{write_retry_ref: ref} = state) when is_reference(ref) do
    _ = Process.cancel_timer(ref)
    %{state | write_retry_ref: nil}
  end

  defp clear_write_retry(state), do: %{state | write_retry_ref: nil}

  defp maybe_resume_drain(state) do
    if :queue.is_empty(state.write_queue) do
      {accs, states, tier_sessions, action} =
        Engine.drain(
          state.pipeline,
          :root,
          state.conn,
          state.accs,
          state.states,
          state.tier_sessions,
          state.transport,
          self()
        )

      state
      |> Map.merge(%{accs: accs, states: states})
      |> apply_tier_sessions(tier_sessions)
      |> apply_drain_action(action)
      |> flush_writes()
    else
      state
    end
  end

  defp drain_pending_stream(state, 0), do: state

  defp drain_pending_stream(state, budget) when budget > 0 do
    receive do
      msg ->
        case state.transport.handle_message(msg, state.socket) do
          {:data, chunk, from} ->
            conn = update_from(state.conn, from)
            meta = %{from: from, received_at: System.monotonic_time(:millisecond)}

            {accs, states, tier_sessions, action} =
              Engine.push_and_drain(
                state.pipeline,
                chunk,
                meta,
                conn,
                state.accs,
                state.states,
                state.tier_sessions,
                state.transport,
                self()
              )

            state =
              state
              |> Map.merge(%{accs: accs, states: states, conn: conn})
              |> apply_tier_sessions(tier_sessions)
              |> apply_drain_action(action)

            case action do
              :close -> state
              _ -> drain_pending_stream(state, budget - 1)
            end

          _ ->
            send(self(), msg)
            state
        end
    after
      0 -> state
    end
  end

  defp apply_tier_sessions(state, tier_sessions) do
    new_tiers = Map.keys(tier_sessions) -- Map.keys(state.tier_sessions)

    monitors =
      Enum.reduce(new_tiers, state.session_monitors, fn tier, monitors ->
        pid = Map.fetch!(tier_sessions, tier)
        ref = Process.monitor(pid)
        Map.put(monitors, ref, {tier, pid})
      end)

    %{state | tier_sessions: tier_sessions, session_monitors: monitors}
  end

  defp schedule_tick_if_needed(%{tick_interval_ms: nil}), do: :ok

  defp schedule_tick_if_needed(%{tick_interval_ms: interval}) when is_integer(interval) do
    schedule_tick(interval)
  end

  defp schedule_tick(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
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

  defp local_endpoint(transport_mod, socket) do
    case transport_mod.sockname(socket) do
      {:ok, {ip, port}} -> {:ok, ip, port}
      _ -> {:ok, Config.server_ip(), 0}
    end
  end

  defp peer_endpoint(transport_mod, socket) do
    case transport_mod.peername(socket) do
      {:ok, {ip, port}} -> {:ok, ip, port}
      _ -> {:ok, nil, nil}
    end
  end

  defp update_from(%Conn{} = conn, {ip, port}) do
    %{conn | client_ip: ip, client_port: port}
  end

  defp update_from(conn, _), do: conn

  defp safe_disconnect_all(pipeline, states, reason) do
    Enum.each(states, fn {tier_key, handler_state} ->
      %Tier{handler: handler_mod} = Pipeline.tier_spec(pipeline, tier_key)

      if function_exported?(handler_mod, :handle_disconnect, 2) do
        handler_mod.handle_disconnect(reason, handler_state)
      end
    end)

    Telemetry.emit(:connection_closed, %{}, %{reason: reason})
  catch
    _, _ -> :ok
  end
end
