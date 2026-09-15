unless Code.ensure_loaded?(Benchee) do
  Mix.raise("Run `mix deps.get` in xsockets before `mix run bench/pipeline_bench.exs`")
end

alias XSockets.{Accumulator.LengthPrefixed, Conn, Engine, Handler, Pipeline}

defmodule BenchCountHandler do
  @behaviour Handler

  @impl true
  def handle_packet(_packet, _meta, _conn, count), do: {:ok, (count || 0) + 1}
end

defmodule BenchPipeline do
  use Pipeline

  tier :root,
    accumulator: {LengthPrefixed, header_size: 2},
    handler: BenchCountHandler
end

defmodule BenchTransport do
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

pipeline = Pipeline.resolve(BenchPipeline)

conn = %Conn{
  client_ip: {127, 0, 0, 1},
  client_port: 40_000,
  assigns: %{}
}

packet = <<0, 4, ?t, ?e, ?s, ?t>>

engine_fun = fn ->
  {accs, states} = Pipeline.fresh_session(pipeline, [])

  {_accs, _states, _sessions, _action} =
    Engine.push_and_drain(
      pipeline,
      packet,
      %{},
      conn,
      accs,
      states,
      %{},
      BenchTransport,
      self()
    )
end

baseline_fun = fn ->
  acc = LengthPrefixed.init(header_size: 2) |> LengthPrefixed.push(packet, %{})

  case LengthPrefixed.pop(acc) do
    {:ok, _body, _meta, _acc} -> :ok
    other -> other
  end
end

Benchee.run(
  %{
    "pipeline_engine" => engine_fun,
    "hand_rolled_framing" => baseline_fun
  },
  time: 2,
  warmup: 1,
  memory_time: 1,
  formatters: [
    {Benchee.Formatters.Console, comparison: true}
  ]
)
