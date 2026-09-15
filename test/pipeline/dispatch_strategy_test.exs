defmodule XSockets.DispatchStrategyTest do
  use ExUnit.Case, async: true

  alias XSockets.{Acceptor, Transport.TCP}

  alias XSockets.PipelineSupport.{
    ScratchPoolSupervisor,
    IsolatedPoolPipeline,
    IsolatedTaskPipeline
  }

  @test_ip {127, 0, 0, 1}

  test "task dispatch is non-blocking and delivers to inner handler" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    {:ok, acceptor} =
      Acceptor.start_link(
        transport: TCP,
        ip: @test_ip,
        port: 0,
        pipeline: XSockets.PipelineSupport.AsyncTwoTierPipeline,
        assigns: %{agent: agent}
      )

    port = Acceptor.port(acceptor)
    payload = XSockets.TestSupport.frame("inner:tasked")

    {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
    :ok = :gen_tcp.send(client, payload)
    :gen_tcp.close(client)

    assert eventually(fn -> XSockets.TestSupport.packets(agent) == ["tasked"] end)

    GenServer.stop(acceptor)
    Agent.stop(agent)
  end

  test "pool saturation drops the packet and emits telemetry" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    ref = :telemetry_test.attach_event_handlers(self(), [[:xsockets, :tier_pool_saturated]])

    # A dedicated, already-saturated supervisor, so this never touches the
    # globally-named Pool supervisor other tests running concurrently rely on.
    {:ok, sup} = ScratchPoolSupervisor.start_link(max_children: 0)

    try do
      {:ok, acceptor} =
        Acceptor.start_link(
          transport: TCP,
          ip: @test_ip,
          port: 0,
          pipeline: IsolatedPoolPipeline,
          assigns: %{agent: agent}
        )

      port = Acceptor.port(acceptor)
      payload = XSockets.TestSupport.frame("inner:pooled")

      {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
      :ok = :gen_tcp.send(client, payload)
      :gen_tcp.close(client)

      assert eventually(fn -> XSockets.TestSupport.packets(agent) == [] end)

      assert_receive {[:xsockets, :tier_pool_saturated], ^ref, _, metadata}
      assert metadata.tier == :inner
      assert metadata.dropped == true

      GenServer.stop(acceptor)
    after
      Supervisor.stop(sup)
    end

    Agent.stop(agent)
  end

  test "missing task supervisor drops the packet and emits telemetry" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xsockets, :tier_supervisor_unavailable]
      ])

    # ScratchTaskSupervisor is deliberately never started for this pipeline, so
    # Engine hits the {:error, :no_supervisor} path without touching the
    # globally-named Task supervisor other tests running concurrently rely on.
    {:ok, acceptor} =
      Acceptor.start_link(
        transport: TCP,
        ip: @test_ip,
        port: 0,
        pipeline: IsolatedTaskPipeline,
        assigns: %{agent: agent}
      )

    port = Acceptor.port(acceptor)
    payload = XSockets.TestSupport.frame("inner:tasked")

    {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
    :ok = :gen_tcp.send(client, payload)
    :gen_tcp.close(client)

    assert eventually(fn -> XSockets.TestSupport.packets(agent) == [] end)

    assert_receive {[:xsockets, :tier_supervisor_unavailable], ^ref, _, metadata}
    assert metadata.tier == :inner
    assert metadata.dropped == true

    GenServer.stop(acceptor)
    Agent.stop(agent)
  end

  test "task dispatch delivers multiple packets in order" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    {:ok, acceptor} =
      Acceptor.start_link(
        transport: TCP,
        ip: @test_ip,
        port: 0,
        pipeline: XSockets.PipelineSupport.AsyncTwoTierPipeline,
        assigns: %{agent: agent}
      )

    port = Acceptor.port(acceptor)

    {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])

    for label <- ["one", "two", "three"] do
      payload = XSockets.TestSupport.frame("inner:#{label}")
      :ok = :gen_tcp.send(client, payload)
      Process.sleep(10)
    end

    assert eventually(fn ->
             XSockets.TestSupport.packets(agent) == ["one", "two", "three"]
           end)

    :gen_tcp.close(client)

    GenServer.stop(acceptor)
    Agent.stop(agent)
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

  test "pool saturation with on_overflow inline_fallback still delivers" do
    {:ok, agent} = XSockets.TestSupport.start_collector()
    ref = :telemetry_test.attach_event_handlers(self(), [[:xsockets, :tier_pool_saturated]])
    {:ok, sup} = ScratchPoolSupervisor.start_link(max_children: 0)

    try do
      {:ok, acceptor} =
        Acceptor.start_link(
          transport: TCP,
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.InlineFallbackPoolPipeline,
          assigns: %{agent: agent}
        )

      port = Acceptor.port(acceptor)
      payload = XSockets.TestSupport.frame("inner:fallback")

      {:ok, client} = :gen_tcp.connect(@test_ip, port, [:binary, active: false])
      :ok = :gen_tcp.send(client, payload)
      :gen_tcp.close(client)

      assert eventually(fn -> XSockets.TestSupport.packets(agent) == ["fallback"] end)

      assert_receive {[:xsockets, :tier_pool_saturated], ^ref, _, metadata}
      assert metadata.tier == :inner
      assert metadata.dropped == false
      assert metadata.fallback == :inline

      GenServer.stop(acceptor)
    after
      Supervisor.stop(sup)
    end

    Agent.stop(agent)
  end
end
