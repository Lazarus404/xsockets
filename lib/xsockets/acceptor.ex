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

defmodule XSockets.Acceptor do
  @moduledoc """
  Accept loop for connection-oriented transports (TCP, TLS, DTLS).

  ## What problem this solves

  TCP, TLS, and DTLS listeners need a long-lived process that accepts client
  connections and hands each socket to a dedicated `Connection` under
  `SockSupervisor`. This GenServer owns listener metadata (`port/1`) and runs
  one or more linked accept workers (`:accept_workers`, default `1`) against the
  same listen socket so DTLS/TLS accept timeouts cannot stall `port/1`. If an
  accept worker exits unexpectedly, it is replaced so the listener stays up.
  After accept it transfers socket ownership, then re-arms `{active, :once}` on
  the child so TLS 1.3 application data is not delivered to the acceptor.

  `Transport.SCTP` associations are not accepted here; use `Sctp.Listener`.

  ## RFCs

  - No accept-loop RFC; OTP process supervision applies
  - [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) - TURN over TCP/TLS (common host use)
  """
  use GenServer
  require Logger

  alias XSockets.{Config, SockSupervisor, Telemetry}

  @accept_timeout 1_000

  @doc """
  Listens and starts accepting.

  ## Parameters

  `opts` is a keyword list:

    * `:transport` - `XSockets.Transport` module (required)
    * `:ip` / `:port` - bind address (required)
    * `:pipeline` - pipeline module, or omit and pass `:accumulator` + `:handler`
    * `:listen_opts` - extra options forwarded to `listen/3`
    * `:assigns` - map copied onto each `Conn`
    * `:accept_timeout` - accept wait in milliseconds (default `1000`)
    * `:accept_workers` - concurrent accept loops (default `1`)
    * `:connection_supervisor` - `SockSupervisor` name or pid (default `SockSupervisor`)
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Bound port of a running acceptor (useful when `:port` was `0`).

  ## Parameters

    * `pid` - acceptor pid from `start_link/1`
  """
  def port(pid), do: GenServer.call(pid, :port)

  @doc false
  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    transport_mod = Keyword.fetch!(opts, :transport)
    ip = Keyword.fetch!(opts, :ip)
    port = Keyword.fetch!(opts, :port)
    listen_opts = Keyword.get(opts, :listen_opts, [])
    accept_timeout = Keyword.get(opts, :accept_timeout, @accept_timeout)
    accept_workers = max(1, Keyword.get(opts, :accept_workers, 1))

    case transport_mod.listen(ip, port, listen_opts) do
      {:ok, listen_sock} ->
        :ok = transport_mod.setopts(listen_sock, active: false)

        bound_port =
          case transport_mod.sockname(listen_sock) do
            {:ok, {_, p}} -> p
            _ -> port
          end

        parent = self()

        workers =
          for _ <- 1..accept_workers do
            spawn_accept_worker(parent, transport_mod, listen_sock, accept_timeout)
          end

        Enum.each(workers, &send(&1, :go))

        Telemetry.emit(:listener_started, %{}, %{
          transport: transport_mod,
          ip: ip,
          port: bound_port,
          accept_workers: accept_workers
        })

        {:ok,
         %{
           transport: transport_mod,
           listen: listen_sock,
           bound_port: bound_port,
           workers: workers,
           handler: Keyword.get(opts, :handler),
           accumulator: Keyword.get(opts, :accumulator),
           pipeline: Keyword.get(opts, :pipeline),
           assigns: Keyword.get(opts, :assigns, %{}),
           accept_timeout: accept_timeout,
           connection_supervisor: Keyword.get(opts, :connection_supervisor, SockSupervisor)
         }}

      {:error, reason} = error ->
        Logger.error("Acceptor failed to listen: #{inspect(reason)}")
        error
    end
  end

  @doc false
  @impl true
  def handle_call(:port, _from, state) do
    {:reply, state.bound_port, state}
  end

  @doc false
  @impl true
  def handle_info({:accepted_sock, client_sock}, state) do
    start_connection(state, client_sock)
    {:noreply, state}
  end

  def handle_info({:accept_error, reason}, state) do
    Logger.warning("Accept failed: #{inspect(reason)}")
    {:noreply, state}
  end

  def handle_info({:accept_worker_failed, reason}, state) do
    Logger.error("Accept worker failed to start: #{inspect(reason)}")
    {:stop, {:accept_worker_failed, reason}, state}
  end

  def handle_info({:EXIT, worker, reason}, %{workers: workers} = state) do
    if worker in workers do
      if reason in [:normal, :shutdown] do
        {:noreply, %{state | workers: List.delete(workers, worker)}}
      else
        case respawn_accept_worker(state, worker, reason) do
          {:ok, state} -> {:noreply, state}
          {:error, stop_reason} -> {:stop, stop_reason, state}
        end
      end
    else
      {:noreply, state}
    end
  end

  def handle_info({:EXIT, _, _}, state), do: {:noreply, state}

  @doc false
  @impl true
  def terminate(_reason, state) do
    Enum.each(state.workers || [], fn worker ->
      if is_pid(worker) and Process.alive?(worker), do: Process.exit(worker, :shutdown)
    end)

    if Map.has_key?(state, :listen) and state.listen do
      state.transport.close(state.listen)
    end

    :ok
  end

  defp spawn_accept_worker(parent, transport_mod, listen_sock, accept_timeout) do
    spawn_link(fn ->
      receive do
        :go -> accept_loop(parent, transport_mod, listen_sock, accept_timeout)
      after
        5_000 ->
          send(parent, {:accept_worker_failed, :start_timeout})
      end
    end)
  end

  defp respawn_accept_worker(state, dead_worker, reason) do
    Logger.warning("Accept worker exited (#{inspect(reason)}); respawning")

    try do
      new_worker =
        spawn_accept_worker(self(), state.transport, state.listen, state.accept_timeout)

      send(new_worker, :go)

      Telemetry.emit(:accept_worker_restarted, %{}, %{
        reason: inspect(reason),
        transport: state.transport
      })

      workers =
        state.workers
        |> Enum.map(fn
          ^dead_worker -> new_worker
          other -> other
        end)

      {:ok, %{state | workers: workers}}
    rescue
      error ->
        Logger.error("Failed to respawn accept worker: #{inspect(error)}")
        {:error, {:accept_worker_respawn_failed, error}}
    end
  end

  defp accept_loop(parent, transport_mod, listen_sock, timeout) do
    case transport_mod.accept(listen_sock, timeout) do
      {:ok, client_sock} ->
        case maybe_give_control(transport_mod, client_sock, parent) do
          :ok ->
            send(parent, {:accepted_sock, client_sock})

          {:error, reason} ->
            transport_mod.close(client_sock)
            send(parent, {:accept_error, {:controlling_process, reason}})
        end

      {:error, :timeout} ->
        :ok

      {:error, reason} ->
        send(parent, {:accept_error, reason})
    end

    accept_loop(parent, transport_mod, listen_sock, timeout)
  end

  defp maybe_give_control(transport_mod, listen_sock, worker) do
    if function_exported?(transport_mod, :controlling_process, 2) do
      transport_mod.controlling_process(listen_sock, worker)
    else
      :ok
    end
  end

  defp start_connection(state, client_sock) do
    child_opts =
      [
        transport: state.transport,
        socket: client_sock,
        listener: self(),
        assigns: state.assigns
      ]
      |> maybe_put(:pipeline, state.pipeline)
      |> maybe_put_legacy_tier(state)

    case SockSupervisor.start_connection(state.connection_supervisor, child_opts) do
      {:ok, pid} ->
        transfer_control(state.transport, client_sock, pid)

      {:error, reason} ->
        Logger.warning("Failed to start connection: #{inspect(reason)}")
        state.transport.close(client_sock)
    end
  end

  defp transfer_control(transport_mod, client_sock, pid) do
    if function_exported?(transport_mod, :controlling_process, 2) do
      case transport_mod.controlling_process(client_sock, pid) do
        :ok ->
          _ = transport_mod.setopts(client_sock, Config.active_socket_opts())
          :ok

        {:error, reason} ->
          Logger.warning("Failed to transfer socket control: #{inspect(reason)}")
          transport_mod.close(client_sock)
      end
    else
      _ = transport_mod.setopts(client_sock, Config.active_socket_opts())
      :ok
    end
  end

  defp maybe_put(keyword, _key, nil), do: keyword
  defp maybe_put(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp maybe_put_legacy_tier(keyword, %{pipeline: nil, handler: handler, accumulator: accumulator})
       when not is_nil(handler) and not is_nil(accumulator) do
    Keyword.merge(keyword, handler: handler, accumulator: accumulator)
  end

  defp maybe_put_legacy_tier(keyword, %{pipeline: pipeline}) when not is_nil(pipeline),
    do: keyword

  defp maybe_put_legacy_tier(keyword, _state), do: keyword
end
