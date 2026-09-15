defmodule XSockets.DatagramWritePolicyTest do
  use ExUnit.Case, async: false

  alias XSockets.DatagramServer

  @table :xsockets_dg_write_probe

  setup do
    if :ets.whereis(@table) != :undefined, do: :ets.delete(@table)
    :ets.new(@table, [:named_table, :public, :set])

    previous = Application.get_env(:xsockets, :datagram_write_on_error)

    on_exit(fn ->
      if :ets.whereis(@table) != :undefined, do: :ets.delete(@table)
      restore_env(:datagram_write_on_error, previous)
    end)

    :ok
  end

  defmodule ProbeTransport do
    @behaviour XSockets.Transport
    @table :xsockets_dg_write_probe

    def listen(_ip, _port, _opts), do: {:ok, :listen}
    def accept(_sock, _timeout), do: {:error, :connectionless}

    def send(_sock, data, peer) do
      case :ets.lookup(@table, :state) do
        [{:state, %{fail_sends: n} = st}] when n > 0 ->
          :ets.insert(@table, {:state, %{st | fail_sends: n - 1}})
          {:error, :eagain}

        [{:state, st}] ->
          :ets.insert(
            @table,
            {:state, %{st | sent: [{IO.iodata_to_binary(data), peer} | st.sent]}}
          )

          :ok

        [] ->
          :ok
      end
    end

    def setopts(_sock, _opts), do: :ok
    def sockname(_sock), do: {:ok, {{127, 0, 0, 1}, 9}}
    def peername(_sock), do: {:error, :enotconn}
    def close(_sock), do: :ok
    def framing(), do: :datagram

    def handle_message({:udp, _sock, ip, port, data}, _s), do: {:data, data, {ip, port}}
    def handle_message(_msg, _sock), do: :ignore
  end

  defmodule ReplyHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(packet, _meta, _conn, state), do: {:reply, packet, state}
  end

  test "retry_peer keeps queue and delivers after send recovers" do
    Application.put_env(:xsockets, :datagram_write_on_error, :retry_peer)
    :ets.insert(@table, {:state, %{fail_sends: 1, sent: []}})

    assert {:ok, server} =
             DatagramServer.start_link(
               transport: ProbeTransport,
               ip: {127, 0, 0, 1},
               port: 0,
               handler: ReplyHandler,
               accumulator: XSockets.Accumulator.Raw
             )

    send(server, {:udp, :listen, {127, 0, 0, 1}, 4000, "ping"})

    assert eventually(fn ->
             [{:state, st}] = :ets.lookup(@table, :state)
             Enum.any?(st.sent, fn {data, _} -> data == "ping" end)
           end)

    GenServer.stop(server)
  end

  test "drop clears queue on send error" do
    Application.put_env(:xsockets, :datagram_write_on_error, :drop)
    # Engine reply send fails once; queue flush fails once and drops.
    :ets.insert(@table, {:state, %{fail_sends: 2, sent: []}})

    assert {:ok, server} =
             DatagramServer.start_link(
               transport: ProbeTransport,
               ip: {127, 0, 0, 1},
               port: 0,
               handler: ReplyHandler,
               accumulator: XSockets.Accumulator.Raw
             )

    send(server, {:udp, :listen, {127, 0, 0, 1}, 4001, "ping"})
    Process.sleep(50)

    [{:state, st}] = :ets.lookup(@table, :state)
    refute Enum.any?(st.sent, fn {data, _} -> data == "ping" end)

    GenServer.stop(server)
  end

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() -> true
      attempts <= 0 -> false
      true ->
        Process.sleep(10)
        eventually(fun, attempts - 1)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:xsockets, key)
  defp restore_env(key, value), do: Application.put_env(:xsockets, key, value)
end
