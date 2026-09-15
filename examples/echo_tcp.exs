# TCP echo with default :length_prefixed framing (2-byte big-endian length).
#
#   mix run examples/echo_tcp.exs
#
# Send with a length prefix, e.g.:
#   <<4::16, "ping">>

defmodule EchoHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, _conn, state) do
    IO.inspect({:packet, packet})
    # Accumulator strips the header; re-prefix for a framed echo.
    {:reply, <<byte_size(packet)::16, packet::binary>>, state}
  end
end

{:ok, pid} =
  XSockets.serve(EchoHandler,
    transport: :tcp,
    ip: {127, 0, 0, 1},
    port: 3478
  )

IO.puts("TCP length-prefixed echo on 127.0.0.1:#{XSockets.Acceptor.port(pid)}")
Process.sleep(:infinity)
