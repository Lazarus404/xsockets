defmodule XSockets.TCPTest do
  use ExUnit.Case

  alias XSockets.{Acceptor, Transport.TCP}
  alias XSockets.TestSupport

  @test_ip {127, 0, 0, 1}

  describe "Transport.TCP" do
    test "listen, accept, send, and receive" do
      {:ok, listen} = TCP.listen(@test_ip, 0, [])
      {:ok, {_, port}} = TCP.sockname(listen)

      parent = self()

      Task.start(fn ->
        {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
        :ok = :gen_tcp.send(client, "ping")
        assert {:ok, "pong"} = :gen_tcp.recv(client, 4, 1000)
        :gen_tcp.close(client)
        send(parent, :client_done)
      end)

      assert {:ok, accepted} = TCP.accept(listen, 2_000)
      assert {:ok, data} = :gen_tcp.recv(accepted, 4, 1_000)
      assert data == "ping"
      assert :ok = TCP.send(accepted, "pong", nil)
      TCP.close(accepted)
      TCP.close(listen)
      assert_receive :client_done, 2_000
    end
  end

  describe "Connection drain regression" do
    test "drains multiple length-prefixed packets from one TCP read" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, acceptor} =
        Acceptor.start_link(
          transport: TCP,
          ip: @test_ip,
          port: 0,
          handler: TestSupport.CollectHandler,
          accumulator: {XSockets.Accumulator.LengthPrefixed, header_size: 2},
          assigns: %{agent: agent}
        )

      port = Acceptor.port(acceptor)
      payload = TestSupport.frame_many(["one", "two"])

      {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
      :ok = :gen_tcp.send(client, payload)
      :gen_tcp.close(client)

      assert eventually(fn -> TestSupport.packets(agent) == ["one", "two"] end)

      GenServer.stop(acceptor)
      Agent.stop(agent)
    end

    test "reorders length-prefixed packets by explicit key before dispatch" do
      {:ok, agent} = TestSupport.start_collector()

      keys = %{"one" => 1, "two" => 2, "three" => 3}

      {:ok, acceptor} =
        Acceptor.start_link(
          transport: TCP,
          ip: @test_ip,
          port: 0,
          handler: TestSupport.CollectHandler,
          accumulator: {
            XSockets.Accumulator.Reorder,
            inner: XSockets.Accumulator.LengthPrefixed,
            inner_opts: [header_size: 2],
            key_fun: fn packet, _meta -> Map.fetch!(keys, packet) end
          },
          assigns: %{agent: agent}
        )

      port = Acceptor.port(acceptor)
      payload = TestSupport.frame_many(["two", "one", "three"])

      {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
      :ok = :gen_tcp.send(client, payload)
      :gen_tcp.close(client)

      assert eventually(fn -> TestSupport.packets(agent) == ["one", "two", "three"] end)

      GenServer.stop(acceptor)
      Agent.stop(agent)
    end

    test "two-tier pipeline descends from root to inner over TCP" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, acceptor} =
        Acceptor.start_link(
          transport: TCP,
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.TwoTierPipeline,
          assigns: %{agent: agent}
        )

      port = Acceptor.port(acceptor)
      payload = TestSupport.frame("inner:piped")

      {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
      :ok = :gen_tcp.send(client, payload)
      :gen_tcp.close(client)

      assert eventually(fn -> TestSupport.packets(agent) == ["piped"] end)

      GenServer.stop(acceptor)
      Agent.stop(agent)
    end
  end

  defp eventually(fun, attempts \\ 20) do
    if fun.() do
      :ok
    else
      if attempts > 0 do
        Process.sleep(50)
        eventually(fun, attempts - 1)
      else
        flunk("condition not met")
      end
    end
  end
end
