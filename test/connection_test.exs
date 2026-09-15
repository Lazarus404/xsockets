defmodule XSockets.ConnectionTest do
  use ExUnit.Case, async: false

  alias XSockets.SockSupervisor

  @table :xsockets_connection_probe

  setup do
    if :ets.whereis(@table) != :undefined, do: :ets.delete(@table)
    :ets.new(@table, [:named_table, :public, :set])
    on_exit(fn -> if :ets.whereis(@table) != :undefined, do: :ets.delete(@table) end)
    :ok
  end

  defmodule StubTransport do
    @behaviour XSockets.Transport

    def listen(_ip, _port, _opts), do: {:ok, :listen}
    def accept(_sock, _timeout), do: {:error, :connectionless}
    def send(_sock, _data, _from), do: :ok
    def setopts(_sock, _opts), do: :ok
    def sockname(_sock), do: {:ok, {{127, 0, 0, 1}, 1}}
    def peername(_sock), do: {:ok, {{127, 0, 0, 1}, 2}}
    def close(_sock), do: :ok
    def framing(), do: :stream

    def handle_message(:icmp, _sock),
      do: {:icmp, %{type: 3, code: 4, error_data: 1280, peer: {{8, 8, 8, 8}, 3478}}}

    def handle_message(:weird, _sock), do: {:other, :not_in_contract}
    def handle_message(_msg, _sock), do: :ignore
  end

  defmodule QueueProbeTransport do
    @behaviour XSockets.Transport
    @table :xsockets_connection_probe

    def listen(_ip, _port, _opts), do: {:ok, :listen}
    def accept(_sock, _timeout), do: {:error, :connectionless}

    def send(_sock, data, _to) do
      case :ets.lookup(@table, :state) do
        [{:state, %{fail_sends: n} = st}] when n > 0 ->
          :ets.insert(@table, {:state, %{st | fail_sends: n - 1}})
          {:error, :eagain}

        [{:state, st}] ->
          :ets.insert(@table, {:state, %{st | sent: [IO.iodata_to_binary(data) | st.sent]}})
          :ok

        [] ->
          :ok
      end
    end

    def setopts(_sock, opts) do
      case :ets.lookup(@table, :state) do
        [{:state, st}] ->
          :ets.insert(@table, {:state, %{st | setopts: [opts | st.setopts]}})

        _ ->
          :ok
      end

      :ok
    end

    def sockname(_sock), do: {:ok, {{127, 0, 0, 1}, 1}}
    def peername(_sock), do: {:ok, {{127, 0, 0, 1}, 2}}
    def close(_sock), do: :ok
    def framing(), do: :stream

    def handle_message({:data, bin}, _sock), do: {:data, bin, nil}
    def handle_message(_msg, _sock), do: :ignore
  end

  defmodule NoopHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(_packet, _meta, _conn, state), do: {:ok, state}
  end

  defmodule ReplyHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(packet, _meta, _conn, state), do: {:reply, packet, state}
  end

  test "ignores icmp and unknown handle_message tags without crashing" do
    assert {:ok, pid} =
             SockSupervisor.start_connection(
               transport: StubTransport,
               socket: :sock,
               handler: NoopHandler,
               accumulator: XSockets.Accumulator.Raw
             )

    send(pid, :icmp)
    send(pid, :weird)
    send(pid, :ignore_me)
    assert Process.alive?(pid)
    Process.exit(pid, :kill)
  end

  test "send failure enqueues write and flush delivers after retry" do
    :ets.insert(@table, {:state, %{fail_sends: 1, sent: [], setopts: []}})

    assert {:ok, pid} =
             SockSupervisor.start_connection(
               transport: QueueProbeTransport,
               socket: :sock,
               handler: ReplyHandler,
               accumulator: XSockets.Accumulator.Raw
             )

    send(pid, {:data, "hello"})

    assert eventually(fn ->
             [{:state, st}] = :ets.lookup(@table, :state)
             "hello" in st.sent
           end)

    Process.exit(pid, :kill)
  end

  test "write retry arms at most one timer; piggyback flush on inbound clears queue" do
    :ets.insert(@table, {:state, %{fail_sends: 100, sent: [], setopts: []}})

    assert {:ok, pid} =
             SockSupervisor.start_connection(
               transport: QueueProbeTransport,
               socket: :sock,
               handler: ReplyHandler,
               accumulator: XSockets.Accumulator.Raw
             )

    send(pid, {:data, "a"})
    send(pid, {:data, "b"})
    Process.sleep(5)

    {:messages, messages} = Process.info(pid, :messages)
    flush_timers = Enum.count(messages, &(&1 == :flush_writes))
    assert flush_timers <= 1

    :ets.insert(@table, {:state, %{fail_sends: 0, sent: [], setopts: []}})
    send(pid, {:data, "c"})

    assert eventually(fn ->
             [{:state, st}] = :ets.lookup(@table, :state)
             "a" in st.sent or "b" in st.sent or "c" in st.sent
           end)

    Process.exit(pid, :kill)
  end

  test "cast :send goes through the write queue" do
    :ets.insert(@table, {:state, %{fail_sends: 0, sent: [], setopts: []}})

    assert {:ok, pid} =
             SockSupervisor.start_connection(
               transport: QueueProbeTransport,
               socket: :sock,
               handler: NoopHandler,
               accumulator: XSockets.Accumulator.Raw
             )

    GenServer.cast(pid, {:send, "direct", nil, nil})

    assert eventually(fn ->
             [{:state, st}] = :ets.lookup(@table, :state)
             "direct" in st.sent
           end)

    Process.exit(pid, :kill)
  end

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, attempts - 1)
    end
  end
end
