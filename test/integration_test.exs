defmodule XSockets.IntegrationTest do
  use ExUnit.Case

  alias XSockets.{
    Accumulator,
    Acceptor,
    Config,
    Conn,
    Connection,
    DatagramServer,
    Engine,
    Handler,
    SockSupervisor,
    Spec,
    Telemetry,
    Transport
  }

  @test_ip {127, 0, 0, 1}

  test "public module surface" do
    assert Code.ensure_loaded?(Transport)
    assert Code.ensure_loaded?(Accumulator)
    assert Code.ensure_loaded?(Handler)
    assert Code.ensure_loaded?(Engine)
    assert Code.ensure_loaded?(Connection)
    assert Code.ensure_loaded?(Acceptor)
    assert Code.ensure_loaded?(DatagramServer)
    assert Code.ensure_loaded?(SockSupervisor)
  end

  test "Conn struct" do
    conn = %Conn{client_ip: @test_ip, client_port: 1234, assigns: %{foo: :bar}}
    assert conn.assigns.foo == :bar
  end

  test "Config helpers" do
    assert is_tuple(Config.server_ip())
    assert is_integer(Config.buffer_size())
  end

  test "Telemetry.emit does not raise" do
    assert :ok = Telemetry.emit(:test_event, %{count: 1}, %{})
  end

  test "Spec.resolve defaults empty opts" do
    assert {XSockets.Accumulator.Raw, []} = Spec.resolve(XSockets.Accumulator.Raw)
  end

  test "SockSupervisor starts connection child on a named supervisor" do
    name = :"SockSupervisor.EmbedTest.#{System.unique_integer([:positive])}"
    {:ok, sup} = SockSupervisor.start_link(name: name)

    {:ok, listen} = XSockets.Transport.TCP.listen(@test_ip, 0, [])
    {:ok, {_, port}} = XSockets.Transport.TCP.sockname(listen)

    parent = self()

    Task.start(fn ->
      {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
      Process.sleep(500)
      :gen_tcp.close(client)
      send(parent, :named_sup_client_connected)
    end)

    assert {:ok, accepted} = XSockets.Transport.TCP.accept(listen, 2_000)
    assert_receive :named_sup_client_connected, 2_000

    {:ok, agent} = Agent.start_link(fn -> 0 end)

    assert {:ok, pid} =
             SockSupervisor.start_connection(name,
               transport: XSockets.Transport.TCP,
               socket: accepted,
               handler: CountHandler,
               accumulator: XSockets.Accumulator.Raw,
               assigns: %{agent: agent}
             )

    assert is_pid(pid)
    refute Enum.any?(DynamicSupervisor.which_children(SockSupervisor), fn {_, child, _, _} ->
             child == pid
           end)

    Process.exit(pid, :kill)
    XSockets.Transport.TCP.close(listen)
    Agent.stop(agent)
    DynamicSupervisor.stop(sup)
  end

  test "SockSupervisor starts connection child" do
    {:ok, listen} = XSockets.Transport.TCP.listen(@test_ip, 0, [])
    {:ok, {_, port}} = XSockets.Transport.TCP.sockname(listen)

    parent = self()

    Task.start(fn ->
      {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
      Process.sleep(500)
      :gen_tcp.close(client)
      send(parent, :client_connected)
    end)

    assert {:ok, accepted} = XSockets.Transport.TCP.accept(listen, 2_000)
    assert_receive :client_connected, 2_000

    {:ok, agent} = Agent.start_link(fn -> 0 end)
    handler = __MODULE__.CountHandler

    assert {:ok, pid} =
             SockSupervisor.start_connection(
               transport: XSockets.Transport.TCP,
               socket: accepted,
               handler: handler,
               accumulator: XSockets.Accumulator.Raw,
               assigns: %{agent: agent}
             )

    assert is_pid(pid)
    Process.exit(pid, :kill)
    XSockets.Transport.TCP.close(listen)
    Agent.stop(agent)
  end

  defmodule CountHandler do
    @behaviour Handler

    @impl true
    def handle_connect(%Conn{assigns: %{agent: agent}}), do: {:ok, agent}

    @impl true
    def handle_packet(_packet, _meta, %Conn{assigns: %{agent: agent}}, _state) do
      Agent.update(agent, &(&1 + 1))
      {:ok, agent}
    end
  end
end
