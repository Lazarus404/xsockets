defmodule XSockets.Sctp.ListenerIntegrationTest do
  use ExUnit.Case, async: false

  alias XSockets.Transport.SCTP
  alias XSockets.TestSupport

  @moduletag :sctp

  @test_ip {127, 0, 0, 1}

  defmodule EchoHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(packet, _meta, _conn, state), do: {:reply, packet, state}
  end

  test "listen facade starts Sctp.Listener when SCTP is available" do
    if SCTP.available?() do
      {:ok, agent} = TestSupport.start_collector()

      assert {:ok, pid} =
               XSockets.listen(
                 transport: SCTP,
                 ip: @test_ip,
                 port: 0,
                 handler: TestSupport.CollectHandler,
                 accumulator: XSockets.Accumulator.Raw,
                 assigns: %{agent: agent}
               )

      assert is_pid(pid)
      assert is_integer(XSockets.Sctp.Listener.port(pid))
      GenServer.stop(pid)
      Agent.stop(agent)
    else
      assert {:error, :sctp_not_supported} =
               XSockets.listen(
                 transport: SCTP,
                 ip: @test_ip,
                 port: 0,
                 handler: TestSupport.CollectHandler,
                 accumulator: XSockets.Accumulator.Raw
               )
    end
  end

  test "association round-trip echo when SCTP is available" do
    if SCTP.available?() do
      assert {:ok, listener} =
               XSockets.listen(
                 transport: SCTP,
                 ip: @test_ip,
                 port: 0,
                 handler: EchoHandler,
                 accumulator: XSockets.Accumulator.Raw
               )

      port = XSockets.Sctp.Listener.port(listener)

      assert {:ok, client, assoc_id} = SCTP.connect_assoc(@test_ip, port, active: true)
      assert :ok = SCTP.send(client, "ping", %{assoc_id: assoc_id, stream: 0})

      assert_receive {:sctp, ^client, _ip, _port, {_anc, "ping"}}, 3_000

      SCTP.close(client)
      GenServer.stop(listener)
    else
      # Optional feature: host without SCTP (document via XSCTP_REQUIRE=1 in CI/docker).
      if System.get_env("XSCTP_REQUIRE") == "1" do
        flunk("SCTP required (XSCTP_REQUIRE=1) but Transport.SCTP.available?/0 is false")
      else
        assert true
      end
    end
  end
end
