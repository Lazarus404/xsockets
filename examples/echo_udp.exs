# Minimal UDP echo via XSockets.serve/2.
#
#   mix run examples/echo_udp.exs
#
# Binds 127.0.0.1:3478 with default :raw framing.

defmodule EchoHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, conn, state) do
    IO.inspect({:packet, packet, conn.client_ip, conn.client_port})
    {:reply, packet, state}
  end
end

{:ok, pid} =
  XSockets.serve(EchoHandler,
    transport: :udp,
    ip: {127, 0, 0, 1},
    port: 3478
  )

IO.puts("UDP echo on 127.0.0.1:#{XSockets.DatagramServer.port(pid)}")
Process.sleep(:infinity)
