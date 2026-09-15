defmodule XSockets.EnginePolicyTest do
  use ExUnit.Case, async: false

  alias XSockets.{Accumulator.Raw, Conn, Engine, Pipeline}
  alias XSockets.PipelineSupport.RootOnlyPipeline

  @conn %Conn{
    client_ip: {127, 0, 0, 1},
    client_port: 40_000,
    assigns: %{agent: nil}
  }

  setup do
    {:ok, agent} = Agent.start(fn -> [] end)

    on_exit(fn ->
      try do
        Agent.stop(agent)
      catch
        :exit, _ -> :ok
      end
    end)

    {:ok, agent: agent, conn: %{@conn | assigns: %{agent: agent}}}
  end

  test "engine FixedWindow rate limit applies to datagram transports only", %{conn: conn, agent: agent} do
    previous_max = Application.get_env(:xsockets, :max_requests_per_window)
    previous_rl = Application.get_env(:xsockets, :engine_rate_limit)
    previous_enabled = Application.get_env(:xsockets, :rate_limit_enabled)
    previous_limiter = Application.get_env(:xsockets, :rate_limiter)

    try do
      Application.put_env(:xsockets, :max_requests_per_window, 1)
      Application.put_env(:xsockets, :engine_rate_limit, true)
      Application.put_env(:xsockets, :rate_limit_enabled, true)
      Application.put_env(:xsockets, :rate_limiter, XSockets.RateLimit.FixedWindow)

      ip = {203, 0, 113, 50}
      conn = %{conn | client_ip: ip}
      pipeline = Pipeline.resolve(RootOnlyPipeline)

      {accs, states} = Pipeline.fresh_session(pipeline, [])
      {_a, _s, _t, :ok} = push(pipeline, "first", conn, accs, states, PolicyDatagramTransport)
      assert "first" in Agent.get(agent, & &1)

      Agent.update(agent, fn _ -> [] end)
      {accs2, states2} = Pipeline.fresh_session(pipeline, [])

      {_a, _s, _t, :ok} = push(pipeline, "stream-ok", conn, accs2, states2, PolicyStreamTransport)
      assert "stream-ok" in Agent.get(agent, & &1)

      Agent.update(agent, fn _ -> [] end)
      {accs3, states3} = Pipeline.fresh_session(pipeline, [])

      {_a, _s, _t, :ok} =
        push(pipeline, "datagram-drop", conn, accs3, states3, PolicyDatagramTransport)

      assert Agent.get(agent, & &1) == []
    after
      restore_env(:xsockets, :max_requests_per_window, previous_max)
      restore_env(:xsockets, :engine_rate_limit, previous_rl)
      restore_env(:xsockets, :rate_limit_enabled, previous_enabled)
      restore_env(:xsockets, :rate_limiter, previous_limiter)
    end
  end

  test "PerConnection rate limiter gates stream transports", %{conn: conn, agent: agent} do
    previous_max = Application.get_env(:xsockets, :max_requests_per_window)
    previous_rl = Application.get_env(:xsockets, :engine_rate_limit)
    previous_enabled = Application.get_env(:xsockets, :rate_limit_enabled)
    previous_limiter = Application.get_env(:xsockets, :rate_limiter)

    try do
      Application.put_env(:xsockets, :max_requests_per_window, 1)
      Application.put_env(:xsockets, :engine_rate_limit, true)
      Application.put_env(:xsockets, :rate_limit_enabled, true)
      Application.put_env(:xsockets, :rate_limiter, XSockets.RateLimit.PerConnection)

      conn = %{conn | client_ip: {198, 51, 100, 7}, client_port: 55_001}
      pipeline = Pipeline.resolve(RootOnlyPipeline)

      {accs, states} = Pipeline.fresh_session(pipeline, [])
      {_a, _s, _t, :ok} = push(pipeline, "one", conn, accs, states, PolicyStreamTransport)
      assert "one" in Agent.get(agent, & &1)

      Agent.update(agent, fn _ -> [] end)
      {accs2, states2} = Pipeline.fresh_session(pipeline, [])
      {_a, _s, _t, :ok} = push(pipeline, "two", conn, accs2, states2, PolicyStreamTransport)
      assert Agent.get(agent, & &1) == []
    after
      restore_env(:xsockets, :max_requests_per_window, previous_max)
      restore_env(:xsockets, :engine_rate_limit, previous_rl)
      restore_env(:xsockets, :rate_limit_enabled, previous_enabled)
      restore_env(:xsockets, :rate_limiter, previous_limiter)
    end
  end

  test "rate limit table sweep removes stale windows" do
    table = XSockets.RateLimit.Table.ensure!()
    window = XSockets.Config.get(:rate_limit_window, 60_000)
    current = div(System.monotonic_time(:millisecond), window)
    :ets.insert(table, {{203, 0, 113, 99}, current - 1, 1})
    assert :ets.lookup(table, {203, 0, 113, 99}) != []
    assert XSockets.RateLimit.Table.sweep() >= 1
    assert :ets.lookup(table, {203, 0, 113, 99}) == []
  end

  test "send_reply applies Config.send_timeout_ms via setopts", %{conn: conn} do
    previous = Application.get_env(:xsockets, :send_timeout_ms)

    try do
      Application.put_env(:xsockets, :send_timeout_ms, 1234)
      pipeline = Pipeline.resolve({Raw, PolicyReplyHandler})
      {accs, states} = Pipeline.fresh_session(pipeline, [])
      conn = %{conn | socket: :sock}
      Process.put(:xsockets_setopts, [])

      {_a, _s, _t, :ok} = push(pipeline, "hi", conn, accs, states, PolicyRecordingTransport)

      assert {:setopts, :sock, [send_timeout: 1234]} in Process.get(:xsockets_setopts, [])
    after
      restore_env(:xsockets, :send_timeout_ms, previous)
      Process.delete(:xsockets_setopts)
    end
  end

  test "handler {:busy, state} stops drain without further pops", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve({Raw, PolicyBusyHandler})
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    {accs, states, _sessions, :busy} = push(pipeline, "a", conn, accs, states, PolicyStreamTransport)
    {_a, states, _sessions, :busy} = push(pipeline, "b", conn, accs, states, PolicyStreamTransport)

    assert Agent.get(agent, & &1) |> Enum.reverse() == ["a", "b"]
    assert states[:root] == :busy_flag
  end

  test "send_reply failure returns {:busy, out} and stops drain", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve({Raw, PolicyReplyHandler})
    {accs, states} = Pipeline.fresh_session(pipeline, [])
    conn = %{conn | socket: :sock}

    {_a, _s, _t, {:busy, "fail-me"}} =
      push(pipeline, "fail-me", conn, accs, states, PolicyFailingTransport)

    assert Agent.get(agent, & &1) == []
  end

  defp push(pipeline, chunk, conn, accs, states, transport) do
    Engine.push_and_drain(
      pipeline,
      chunk,
      %{},
      conn,
      accs,
      states,
      %{},
      transport,
      self()
    )
  end

  defp restore_env(app, key, value) do
    case value do
      nil -> Application.delete_env(app, key)
      value -> Application.put_env(app, key, value)
    end
  end
end

defmodule PolicyStreamTransport do
  @behaviour XSockets.Transport

  @impl true
  def listen(_ip, _port, _opts), do: {:ok, :fake}
  @impl true
  def accept(_socket, _timeout), do: {:ok, :fake}
  @impl true
  def send(_socket, _data, _to), do: :ok
  @impl true
  def setopts(_socket, _opts), do: :ok
  @impl true
  def peername(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def sockname(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def close(_socket), do: :ok
  @impl true
  def framing(), do: :stream
  @impl true
  def handle_message(_msg, _socket), do: :ignore
end

defmodule PolicyDatagramTransport do
  @behaviour XSockets.Transport

  @impl true
  def listen(_ip, _port, _opts), do: {:ok, :fake}
  @impl true
  def accept(_socket, _timeout), do: {:ok, :fake}
  @impl true
  def send(_socket, _data, _to), do: :ok
  @impl true
  def setopts(_socket, _opts), do: :ok
  @impl true
  def peername(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def sockname(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def close(_socket), do: :ok
  @impl true
  def framing(), do: :datagram
  @impl true
  def handle_message(_msg, _socket), do: :ignore
end

defmodule PolicyRecordingTransport do
  @behaviour XSockets.Transport

  @impl true
  def listen(_ip, _port, _opts), do: {:ok, :fake}
  @impl true
  def accept(_socket, _timeout), do: {:ok, :fake}
  @impl true
  def send(_socket, _data, _to), do: :ok

  @impl true
  def setopts(socket, opts) do
    Process.put(:xsockets_setopts, [{:setopts, socket, opts} | Process.get(:xsockets_setopts, [])])
    :ok
  end

  @impl true
  def peername(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def sockname(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def close(_socket), do: :ok
  @impl true
  def framing(), do: :stream
  @impl true
  def handle_message(_msg, _socket), do: :ignore
end

defmodule PolicyFailingTransport do
  @behaviour XSockets.Transport

  @impl true
  def listen(_ip, _port, _opts), do: {:ok, :fake}
  @impl true
  def accept(_socket, _timeout), do: {:ok, :fake}
  @impl true
  def send(_socket, _data, _to), do: {:error, :eagain}
  @impl true
  def setopts(_socket, _opts), do: :ok
  @impl true
  def peername(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def sockname(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def close(_socket), do: :ok
  @impl true
  def framing(), do: :stream
  @impl true
  def handle_message(_msg, _socket), do: :ignore
end

defmodule PolicyReplyHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_packet(packet, _meta, _conn, state), do: {:reply, packet, state}
end

defmodule PolicyBusyHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_packet(packet, _meta, %{assigns: %{agent: agent}}, _state) do
    Agent.update(agent, &[packet | &1])
    {:busy, :busy_flag}
  end
end
