# TCP server via serve/2 and outbound dial/2.
#
#   mix run examples/echo_client.exs
#
# Starts a length-prefixed echo server, dials a client Connection (same Engine),
# then sends one framed ping over a short-lived :gen_tcp socket.

defmodule ServerHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, _conn, state) do
    IO.inspect({:server, packet})
    {:reply, <<byte_size(packet)::16, packet::binary>>, state}
  end
end

defmodule ClientHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, _conn, state) do
    IO.inspect({:client_inbound, packet})
    {:ok, state}
  end
end

ip = {127, 0, 0, 1}

{:ok, acceptor} =
  XSockets.serve(ServerHandler, transport: :tcp, ip: ip, port: 0)

port = XSockets.Acceptor.port(acceptor)

{:ok, dial_pid} =
  XSockets.dial(ClientHandler, transport: :tcp, ip: ip, port: port)

IO.puts("dial Connection #{inspect(dial_pid)} -> 127.0.0.1:#{port}")

{:ok, sock} = :gen_tcp.connect(ip, port, [:binary, active: false])
:ok = :gen_tcp.send(sock, <<4::16, "ping">>)
{:ok, reply} = :gen_tcp.recv(sock, 0, 2000)
IO.inspect({:gen_tcp_reply, reply})
:gen_tcp.close(sock)

GenServer.stop(dial_pid)
GenServer.stop(acceptor)
