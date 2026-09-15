# TLS echo with runtime self-signed certs (no committed PEM files).
#
#   mix run examples/echo_tls.exs
#
# Requires openssl on PATH. Default framing is :length_prefixed.

defmodule EchoHandler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, _conn, state) do
    IO.inspect({:packet, packet})
    {:reply, <<byte_size(packet)::16, packet::binary>>, state}
  end
end

dir = System.tmp_dir!() |> Path.join("xsockets-echo-tls-#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)
certfile = Path.join(dir, "server.crt")
keyfile = Path.join(dir, "server.key")

{_, 0} =
  System.cmd(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-keyout",
      keyfile,
      "-out",
      certfile,
      "-days",
      "1",
      "-nodes",
      "-subj",
      "/CN=localhost"
    ],
    stderr_to_stdout: true
  )

listen_opts =
  XSockets.Transport.TLS.security_opts(:server)
  |> Keyword.merge(certfile: certfile, keyfile: keyfile)

{:ok, pid} =
  XSockets.serve(EchoHandler,
    transport: :tls,
    ip: {127, 0, 0, 1},
    port: 3478,
    listen_opts: listen_opts
  )

IO.puts("TLS length-prefixed echo on 127.0.0.1:#{XSockets.Acceptor.port(pid)}")
Process.sleep(:infinity)
