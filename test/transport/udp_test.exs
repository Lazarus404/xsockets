defmodule XSockets.UDPTest do
  use ExUnit.Case, async: false

  alias XSockets.{DatagramServer, Transport.UDP}
  alias XSockets.TestSupport

  @test_ip {127, 0, 0, 1}

  describe "Transport.UDP" do
    test "listen and send datagram" do
      {:ok, receiver} = UDP.listen(@test_ip, 0, [])
      {:ok, {_, port}} = UDP.sockname(receiver)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      assert :ok = UDP.send(sender, "hello", {@test_ip, port})

      assert {:ok, {ip, _sender_port, data}} = :gen_udp.recv(receiver, 5, 1_000)
      assert data == "hello"
      assert ip == @test_ip

      :gen_udp.close(sender)
      UDP.close(receiver)
    end

    @tag :reuseport
    test "reuseport binds multiple listeners on one port when supported" do
      {:ok, s1} = UDP.listen(@test_ip, 0, reuseport: true)
      {:ok, {_, port}} = UDP.sockname(s1)

      case UDP.listen(@test_ip, port, reuseport: true) do
        {:ok, s2} ->
          {:ok, sender} = :gen_udp.open(0, [:binary, active: false])
          assert :ok = UDP.send(sender, "shard", {@test_ip, port})

          assert recv_on_either(s1, s2, 5)

          :gen_udp.close(sender)
          UDP.close(s2)

        {:error, reason} ->
          assert reason in [:eaddrinuse, :einval, :enoprotoopt]
      end

      UDP.close(s1)
    end
  end

  describe "DatagramServer drain regression" do
    test "drains multiple length-prefixed packets from one UDP datagram" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          handler: TestSupport.CollectHandler,
          accumulator: {XSockets.Accumulator.LengthPrefixed, header_size: 2},
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      payload = TestSupport.frame_many(["alpha", "beta"])
      :ok = :gen_udp.send(sender, @test_ip, port, payload)

      Process.sleep(100)
      assert TestSupport.packets(agent) == ["alpha", "beta"]

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "processes a burst of UDP datagrams when udp_active_n > 1" do
      previous = Application.get_env(:xsockets, :udp_active_n)

      on_exit(fn ->
        if previous do
          Application.put_env(:xsockets, :udp_active_n, previous)
        else
          Application.delete_env(:xsockets, :udp_active_n)
        end
      end)

      Application.put_env(:xsockets, :udp_active_n, 8)
      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          handler: TestSupport.CollectHandler,
          accumulator: XSockets.Accumulator.Raw,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      for i <- 1..20 do
        :ok = :gen_udp.send(sender, @test_ip, port, "pkt-#{i}")
      end

      Process.sleep(200)
      packets = TestSupport.packets(agent)
      assert length(packets) == 20
      assert MapSet.new(packets) == MapSet.new(for(i <- 1..20, do: "pkt-#{i}"))

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "two-tier pipeline descends from root to inner over UDP" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.TwoTierPipeline,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      payload = TestSupport.frame("inner:datagram")
      :ok = :gen_udp.send(sender, @test_ip, port, payload)

      Process.sleep(100)
      assert TestSupport.packets(agent) == ["datagram"]

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "two-tier pool dispatch descends from root to inner over UDP" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.PoolTwoTierPipeline,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      payload = TestSupport.frame("inner:pool-udp")
      :ok = :gen_udp.send(sender, @test_ip, port, payload)

      assert eventually(fn -> TestSupport.packets(agent) == ["pool-udp"] end)

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "multi-tier UDP retains inner reorder state across datagrams" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.UdpReorderPipeline,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      :ok = :gen_udp.send(sender, @test_ip, port, TestSupport.frame(<<1>>))
      :ok = :gen_udp.send(sender, @test_ip, port, TestSupport.frame(<<3>>))
      :ok = :gen_udp.send(sender, @test_ip, port, TestSupport.frame(<<2>>))

      Process.sleep(100)
      assert TestSupport.packets(agent) == [<<1>>, <<2>>, <<3>>]

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "truncated datagram does not corrupt the next datagram from the same peer" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.TwoTierPipeline,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      # Incomplete length-prefixed frame (claims 50 bytes, delivers 3)
      :ok = :gen_udp.send(sender, @test_ip, port, <<0, 50, 1, 2, 3>>)
      :ok = :gen_udp.send(sender, @test_ip, port, TestSupport.frame("inner:ok"))

      Process.sleep(100)
      assert TestSupport.packets(agent) == ["ok"]

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "evicts idle peer sessions and preserves other peers' tier session monitors" do
      original_idle = Application.get_env(:xsockets, :udp_session_idle_ms)
      original_sweep = Application.get_env(:xsockets, :udp_session_sweep_ms)

      on_exit(fn ->
        restore_env(:xsockets, :udp_session_idle_ms, original_idle)
        restore_env(:xsockets, :udp_session_sweep_ms, original_sweep)
      end)

      Application.put_env(:xsockets, :udp_session_idle_ms, 10_000)
      Application.put_env(:xsockets, :udp_session_sweep_ms, 60_000)

      ref =
        :telemetry_test.attach_event_handlers(self(), [[:xsockets, :udp_sessions_evicted]])

      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.AsyncTwoTierPipeline,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender_a} = :gen_udp.open(0, [:binary, active: false])
      {:ok, sender_b} = :gen_udp.open(0, [:binary, active: false])

      :ok = :gen_udp.send(sender_a, @test_ip, port, TestSupport.frame("inner:peer-a"))
      :ok = :gen_udp.send(sender_b, @test_ip, port, TestSupport.frame("inner:peer-b"))

      assert eventually(fn ->
               state = server_state(server)

               map_size(state.sessions) == 2 and
                 Enum.all?(state.sessions, fn {_peer, entry} ->
                   match?(%{inner: pid} when is_pid(pid), entry.tier_sessions)
                 end)
             end)

      {peer_a, peer_b} = two_peers(server)
      pid_a = peer_tier_session_pid(server, peer_a, :inner)
      pid_b = peer_tier_session_pid(server, peer_b, :inner)

      assert Process.alive?(pid_a)
      assert Process.alive?(pid_b)

      :sys.replace_state(server, fn state ->
        now = System.monotonic_time(:millisecond)
        entry = Map.fetch!(state.sessions, peer_a)
        idle_ms = Application.fetch_env!(:xsockets, :udp_session_idle_ms)

        %{
          state
          | sessions: Map.put(state.sessions, peer_a, %{entry | last_seen: now - idle_ms - 1})
        }
      end)

      send(server, :sweep)

      assert eventually(fn ->
               state = server_state(server)
               not Map.has_key?(state.sessions, peer_a) and not Process.alive?(pid_a)
             end)

      refute monitors_reference_peer?(server, peer_a)

      assert_receive {[:xsockets, :udp_sessions_evicted], ^ref, %{count: 1}, _}

      Process.exit(pid_b, :kill)

      assert eventually(fn ->
               entry = Map.get(server_state(server).sessions, peer_b)
               entry == nil or not Map.has_key?(entry.tier_sessions, :inner)
             end)

      :ok =
        :gen_udp.send(
          sender_b,
          @test_ip,
          port,
          TestSupport.frame("inner:after-evict")
        )

      assert eventually(fn ->
               packets = TestSupport.packets(agent)
               MapSet.new(packets) == MapSet.new(["peer-a", "peer-b", "after-evict"])
             end)

      :gen_udp.close(sender_a)
      :gen_udp.close(sender_b)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "a stale :DOWN does not orphan a live replacement tier session" do
      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.AsyncTwoTierPipeline,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      :ok = :gen_udp.send(sender, @test_ip, port, TestSupport.frame("inner:first"))

      assert eventually(fn -> map_size(server_state(server).sessions) == 1 end)

      peer = server_state(server).sessions |> Map.keys() |> List.first()
      {client_ip, client_port} = peer

      assert eventually(fn ->
               match?(%{inner: pid} when is_pid(pid), peer_entry(server, peer).tier_sessions)
             end)

      pid1 = peer_tier_session_pid(server, peer, :inner)

      # Freeze the listener so the next datagram queues in its mailbox without
      # being processed yet, then kill pid1. This reproduces the race: a
      # datagram for this peer is already enqueued *ahead of* the :DOWN that
      # will result from killing pid1, so when the listener resumes it starts
      # a replacement session (pid2) for :inner before it ever sees pid1's
      # :DOWN.
      :sys.suspend(server)

      send(
        server,
        {:udp, :fake_socket, client_ip, client_port, TestSupport.frame("inner:second")}
      )

      Process.exit(pid1, :kill)
      assert eventually(fn -> not Process.alive?(pid1) end)

      :sys.resume(server)

      assert eventually(fn ->
               case peer_entry(server, peer).tier_sessions do
                 %{inner: pid2} -> is_pid(pid2) and pid2 != pid1
                 _ -> false
               end
             end)

      pid2 = peer_tier_session_pid(server, peer, :inner)
      assert Process.alive?(pid2)

      # The critical assertion: once the now-stale :DOWN for pid1 is processed,
      # it must not delete pid2's live entry out from under it.
      assert eventually(fn -> Map.get(peer_entry(server, peer).tier_sessions, :inner) == pid2 end)
      refute monitors_reference_pid?(server, pid1)
      assert monitors_reference_pid?(server, pid2)

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end

    test "sweep reclaims a session with a malformed (missing last_seen) entry" do
      original_idle = Application.get_env(:xsockets, :udp_session_idle_ms)
      original_sweep = Application.get_env(:xsockets, :udp_session_sweep_ms)

      on_exit(fn ->
        restore_env(:xsockets, :udp_session_idle_ms, original_idle)
        restore_env(:xsockets, :udp_session_sweep_ms, original_sweep)
      end)

      # An idle window a well-formed entry would never breach on its own; only
      # a malformed entry (whose age can't be read) should be reclaimed here.
      Application.put_env(:xsockets, :udp_session_idle_ms, 10_000)
      Application.put_env(:xsockets, :udp_session_sweep_ms, 60_000)

      {:ok, agent} = TestSupport.start_collector()

      {:ok, server} =
        DatagramServer.start_link(
          ip: @test_ip,
          port: 0,
          pipeline: XSockets.PipelineSupport.TwoTierPipeline,
          assigns: %{agent: agent}
        )

      port = DatagramServer.port(server)
      {:ok, sender} = :gen_udp.open(0, [:binary, active: false])

      :ok = :gen_udp.send(sender, @test_ip, port, TestSupport.frame("inner:hello"))

      assert eventually(fn -> map_size(server_state(server).sessions) == 1 end)

      peer = server_state(server).sessions |> Map.keys() |> List.first()

      :sys.replace_state(server, fn state ->
        entry = Map.fetch!(state.sessions, peer)
        %{state | sessions: Map.put(state.sessions, peer, Map.delete(entry, :last_seen))}
      end)

      send(server, :sweep)

      assert eventually(fn -> not Map.has_key?(server_state(server).sessions, peer) end)

      :gen_udp.close(sender)
      GenServer.stop(server)
      Agent.stop(agent)
    end
  end

  defp restore_env(app, key, value) do
    if value do
      Application.put_env(app, key, value)
    else
      Application.delete_env(app, key)
    end
  end

  defp server_state(server), do: :sys.get_state(server)

  defp two_peers(server) do
    [peer_a, peer_b] =
      server
      |> server_state()
      |> Map.fetch!(:sessions)
      |> Map.keys()
      |> Enum.sort()

    {peer_a, peer_b}
  end

  defp peer_tier_session_pid(server, peer, tier) do
    server
    |> server_state()
    |> Map.fetch!(:sessions)
    |> Map.fetch!(peer)
    |> Map.fetch!(:tier_sessions)
    |> Map.fetch!(tier)
  end

  defp monitors_reference_peer?(server, peer) do
    server
    |> server_state()
    |> Map.fetch!(:session_monitors)
    |> Enum.any?(fn {_ref, {p, _tier, _pid}} -> p == peer end)
  end

  defp monitors_reference_pid?(server, pid) do
    server
    |> server_state()
    |> Map.fetch!(:session_monitors)
    |> Enum.any?(fn {_ref, {_peer, _tier, p}} -> p == pid end)
  end

  defp peer_entry(server, peer) do
    server
    |> server_state()
    |> Map.fetch!(:sessions)
    |> Map.fetch!(peer)
  end

  defp recv_on_either(s1, s2, len) do
    case :gen_udp.recv(s1, len, 500) do
      {:ok, {_, _, "shard"}} -> true
      _ -> match?({:ok, {_, _, "shard"}}, :gen_udp.recv(s2, len, 500))
    end
  end

  defp eventually(fun, attempts \\ 60) do
    if fun.() do
      true
    else
      if attempts > 0 do
        Process.sleep(50)
        eventually(fun, attempts - 1)
      else
        flunk("condition not met after retries")
      end
    end
  end
end
