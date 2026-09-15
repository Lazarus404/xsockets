# Control plane vs high-rate media: Engine drain vs UDP.open_relay/2.
#
#   mix run examples/udp_relay.exs
#
# Request-shaped traffic belongs on XSockets.serve / listen + Handler.
# High-rate media should bypass the drain loop with open_relay/2 (host owns
# the socket GenServer and read loop).

defmodule ControlHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, conn, state) do
    IO.inspect({:control, packet, conn.client_ip})
    {:ok, state}
  end
end

{:ok, control} =
  XSockets.serve(ControlHandler,
    transport: :udp,
    ip: {127, 0, 0, 1},
    port: 3478
  )

{:ok, relay_sock} =
  XSockets.Transport.UDP.open_relay({127, 0, 0, 1}, port: 0)

{:ok, {_ip, relay_port}} = :inet.sockname(relay_sock)

IO.puts("""
control Engine UDP on :#{XSockets.DatagramServer.port(control)}
relay socket (host-owned) on :#{relay_port}

Do not push media through the Engine. open_relay/2 returns a raw :gen_udp
socket; your process reads and sends. Stop with Ctrl-C.
""")

Process.sleep(:infinity)
