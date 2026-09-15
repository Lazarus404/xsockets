defmodule XSockets.TierSessionTest do
  use ExUnit.Case, async: true

  alias XSockets.{Conn, Pipeline, TierSession}
  alias XSockets.PipelineSupport.{AsyncTwoTierPipeline, InnerCloseSessionPipeline}

  @conn %Conn{
    client_ip: {127, 0, 0, 1},
    client_port: 40_000,
    assigns: %{}
  }

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  test "inner handler state persists across pushes" do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    on_exit(fn -> if Process.alive?(agent), do: Agent.stop(agent) end)

    pipeline = Pipeline.resolve(AsyncTwoTierPipeline)
    conn = %{@conn | assigns: %{agent: agent}}

    {:ok, session} =
      TierSession.start_link(
        pipeline: pipeline,
        tier_key: :inner,
        conn: conn,
        owner: self(),
        transport: TierSessionFakeTransport
      )

    GenServer.cast(session, {:push, "one", %{}})
    GenServer.cast(session, {:push, "two", %{}})

    assert eventually(fn ->
             Agent.get(agent, & &1) |> Enum.reverse() == ["one", "two"]
           end)
  end

  test "close propagates to owner" do
    pipeline = Pipeline.resolve(InnerCloseSessionPipeline)

    {:ok, session} =
      TierSession.start_link(
        pipeline: pipeline,
        tier_key: :inner,
        conn: @conn,
        owner: self(),
        transport: TierSessionFakeTransport
      )

    GenServer.cast(session, {:push, "close-me", %{}})

    assert_receive {:tier_close, :inner, :normal}, 1_000

    assert eventually(fn -> not Process.alive?(session) end)
  end

  test "session crash does not kill owner" do
    pipeline = Pipeline.resolve(AsyncTwoTierPipeline)

    {:ok, session} =
      TierSession.start_link(
        pipeline: pipeline,
        tier_key: :inner,
        conn: @conn,
        owner: self(),
        transport: TierSessionFakeTransport
      )

    Process.exit(session, :kill)
    assert_receive {:EXIT, ^session, :killed}, 1_000
    assert Process.alive?(self())
  end

  defp eventually(fun, attempts \\ 20) do
    if fun.() do
      :ok
    else
      if attempts > 0 do
        Process.sleep(20)
        eventually(fun, attempts - 1)
      else
        flunk("condition not met")
      end
    end
  end
end

defmodule TierSessionFakeTransport do
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
