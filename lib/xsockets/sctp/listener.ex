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

defmodule XSockets.Sctp.Listener do
  @moduledoc """
  Optional SCTP-over-IP association owner (`:gen_sctp`).

  ## What problem this solves

  SCTP associations are not driven by `Acceptor.accept/2`. One GenServer owns the
  listen socket, tracks associations, feeds each payload through `Engine`, and
  replies with the real association id (stream `0` by default).

  Requires OS/OTP SCTP support (`Transport.SCTP.available?/0`). When unavailable,
  `start_link/1` returns `{:error, :sctp_not_supported}`.

  ## RFCs

  - [RFC 4960](https://www.rfc-editor.org/rfc/rfc4960) - SCTP association lifecycle
  """
  use GenServer
  require Logger

  alias XSockets.{Config, Conn, Engine, Pipeline, Pipeline.Tier, Telemetry}
  alias XSockets.Transport.SCTP

  @doc """
  Opens an SCTP listener.

  Same opts shape as `DatagramServer.start_link/1` (`:ip`, `:port`, `:pipeline` or
  `:accumulator` + `:handler`, `:listen_opts`, `:assigns`, `:handler_state`).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Bound port of a running listener (useful when `:port` was `0`).
  """
  @spec port(pid()) :: :inet.port_number()
  def port(pid), do: GenServer.call(pid, :port)

  @doc false
  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    ip = Keyword.fetch!(opts, :ip)
    port = Keyword.fetch!(opts, :port)
    listen_opts = Keyword.get(opts, :listen_opts, [])
    pipeline = resolve_pipeline(opts)

    case SCTP.listen(ip, port, listen_opts) do
      {:ok, socket} ->
        :ok = SCTP.setopts(socket, active: :once)

        {:ok, server_ip, server_port} = local_endpoint(socket)

        Telemetry.emit(:sctp_listener_started, %{}, %{ip: server_ip, port: server_port})

        {:ok,
         %{
           socket: socket,
           pipeline: pipeline,
           server_ip: server_ip,
           server_port: server_port,
           assigns: Keyword.get(opts, :assigns, %{}),
           init_opts: Keyword.take(opts, [:handler_state]),
           assocs: %{}
         }}

      {:error, reason} = error ->
        Logger.error("Sctp.Listener failed to open socket: #{inspect(reason)}")
        error
    end
  end

  @doc false
  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.server_port, state}

  @doc false
  @impl true
  def handle_cast(:stop, state), do: {:stop, :normal, state}

  @doc false
  @impl true
  def handle_info(msg, state) do
    case SCTP.decode_message(msg) do
      {:assoc_up, assoc_id, peer} ->
        {:noreply, state |> assoc_up(assoc_id, peer) |> rearm()}

      {:assoc_down, assoc_id, reason} ->
        {:noreply, state |> assoc_down(assoc_id, reason) |> rearm()}

      {:data, chunk, peer, meta} ->
        {:noreply, handle_data(state, chunk, peer, meta)}

      {:closed, reason} ->
        Enum.each(state.assocs, fn {_id, entry} ->
          safe_disconnect(state.pipeline, entry.states, reason)
        end)

        {:stop, reason, %{state | assocs: %{}}}

      :ignore ->
        {:noreply, rearm(state)}
    end
  end

  @doc false
  @impl true
  def terminate(_reason, state) do
    SCTP.close(state.socket)
    :ok
  end

  defp assoc_up(state, assoc_id, peer) do
    {accs, states} = Pipeline.fresh_session(state.pipeline, state.init_opts)

    entry = %{
      peer: peer,
      accs: accs,
      states: states,
      tier_sessions: %{},
      stream: 0
    }

    Telemetry.emit(:sctp_assoc_up, %{}, %{assoc_id: assoc_id, peer: peer})
    %{state | assocs: Map.put(state.assocs, assoc_id, entry)}
  end

  defp assoc_down(state, assoc_id, reason) do
    case Map.pop(state.assocs, assoc_id) do
      {nil, _} ->
        state

      {entry, assocs} ->
        safe_disconnect(state.pipeline, entry.states, reason)
        Telemetry.emit(:sctp_assoc_down, %{}, %{assoc_id: assoc_id, reason: reason})
        %{state | assocs: assocs}
    end
  end

  defp handle_data(state, chunk, peer, meta) do
    assoc_id = Map.get(meta, :assoc_id)
    stream = Map.get(meta, :stream, 0)

    {state, assoc_id, entry} =
      cond do
        assoc_id && Map.has_key?(state.assocs, assoc_id) ->
          {state, assoc_id, Map.fetch!(state.assocs, assoc_id)}

        assoc_id ->
          state = assoc_up(state, assoc_id, peer)
          {state, assoc_id, Map.fetch!(state.assocs, assoc_id)}

        true ->
          # No anc assoc id: single-assoc fallback key 0
          state =
            if Map.has_key?(state.assocs, 0) do
              state
            else
              assoc_up(state, 0, peer)
            end

          {state, 0, Map.fetch!(state.assocs, 0)}
      end

    entry = %{entry | peer: peer, stream: stream}
    conn = build_conn(state, peer, assoc_id, stream)
    packet_meta = Map.merge(meta, %{from: peer, received_at: System.monotonic_time(:millisecond)})

    {accs, states, tier_sessions, action} =
      Engine.push_and_drain(
        state.pipeline,
        chunk,
        packet_meta,
        conn,
        entry.accs,
        entry.states,
        entry.tier_sessions,
        SCTP,
        self()
      )

    Telemetry.emit(:sctp_message_received, %{bytes: byte_size(chunk)}, %{assoc_id: assoc_id})

    state =
      case action do
        {:busy, out} ->
          _ = SCTP.send(state.socket, out, %{assoc_id: assoc_id, stream: stream})
          put_assoc(state, assoc_id, %{entry | accs: accs, states: states, tier_sessions: tier_sessions})

        :close ->
          safe_disconnect(state.pipeline, states, :normal)
          %{state | assocs: Map.delete(state.assocs, assoc_id)}

        _ ->
          put_assoc(state, assoc_id, %{entry | accs: accs, states: states, tier_sessions: tier_sessions})
      end

    rearm(state)
  end

  defp put_assoc(state, assoc_id, entry) do
    %{state | assocs: Map.put(state.assocs, assoc_id, entry)}
  end

  defp build_conn(state, {client_ip, client_port}, assoc_id, stream) do
    %Conn{
      listener: self(),
      socket: state.socket,
      client_ip: client_ip,
      client_port: client_port,
      server_ip: state.server_ip,
      server_port: state.server_port,
      assigns:
        Map.merge(state.assigns, %{
          sctp_assoc_id: assoc_id,
          sctp_stream: stream
        })
    }
  end

  # SCTP drivers often reject `{active, N}`; prefer once/true.
  defp rearm(state) do
    _ = SCTP.setopts(state.socket, active: :once)
    state
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

  defp local_endpoint(socket) do
    case SCTP.sockname(socket) do
      {:ok, {ip, port}} -> {:ok, ip, port}
      _ -> {:ok, Config.server_ip(), 0}
    end
  end

  defp safe_disconnect(pipeline, states, reason) do
    Enum.each(states, fn {tier_key, handler_state} ->
      %Tier{handler: handler_mod} = Pipeline.tier_spec(pipeline, tier_key)

      if function_exported?(handler_mod, :handle_disconnect, 2) do
        _ = handler_mod.handle_disconnect(reason, handler_state)
      end
    end)
  catch
    _, _ -> :ok
  end
end
