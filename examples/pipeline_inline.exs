# Two-tier inline pipeline: root descends into :inner.
#
#   mix run examples/pipeline_inline.exs
#
# Uses listen/1 + pipeline: (advanced path; serve stays single-tier).

defmodule RootHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, _conn, state) do
    IO.inspect({:root, packet})
    {:descend, :inner, packet, state}
  end
end

defmodule InnerHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_packet(packet, _meta, _conn, state) do
    IO.inspect({:inner, packet})
    {:reply, <<byte_size(packet)::16, packet::binary>>, state}
  end
end

defmodule EchoPipeline do
  use XSockets.Pipeline

  tier :root,
    accumulator: {XSockets.Accumulator.LengthPrefixed, header_size: 2},
    handler: RootHandler,
    dispatch: :inline

  tier :inner,
    accumulator: XSockets.Accumulator.Raw,
    handler: InnerHandler,
    dispatch: :inline
end

{:ok, pid} =
  XSockets.listen(
    transport: XSockets.Transport.TCP,
    ip: {127, 0, 0, 1},
    port: 3478,
    pipeline: EchoPipeline
  )

IO.puts("pipeline inline TCP on 127.0.0.1:#{XSockets.Acceptor.port(pid)}")
Process.sleep(:infinity)
