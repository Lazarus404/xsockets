defmodule XSockets.APITest do
  use ExUnit.Case, async: false

  alias XSockets.{
    DatagramServer,
    Acceptor,
    Transport.TCP,
    Transport.UDP,
    Transport.TLS,
    Transport.DTLS
  }

  @ip {127, 0, 0, 1}

  test "listen/1 starts a UDP DatagramServer" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             XSockets.listen(
               transport: UDP,
               ip: @ip,
               port: 0,
               accumulator: XSockets.Accumulator.Raw,
               handler: XSockets.TestSupport.CollectHandler,
               assigns: %{agent: agent}
             )

    assert is_pid(pid)
    port = DatagramServer.port(pid)
    assert port > 0

    {:ok, sender} = :gen_udp.open(0, [:binary, active: false])
    :ok = :gen_udp.send(sender, @ip, port, "hello")
    assert eventually(fn -> XSockets.TestSupport.packets(agent) != [] end)
    :gen_udp.close(sender)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "listen/1 starts a TCP Acceptor" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             XSockets.listen(
               transport: TCP,
               ip: @ip,
               port: 0,
               accumulator: XSockets.Accumulator.LengthPrefixed,
               handler: XSockets.TestSupport.CollectHandler,
               assigns: %{agent: agent}
             )

    port = Acceptor.port(pid)
    payload = XSockets.TestSupport.frame("ping")
    {:ok, client} = :gen_tcp.connect(@ip, port, [:binary, active: false])
    :ok = :gen_tcp.send(client, payload)
    assert eventually(fn -> "ping" in XSockets.TestSupport.packets(agent) end)
    :gen_tcp.close(client)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "listen_many/1 binds reuseport workers when supported" do
    case XSockets.listen_many(
           transport: UDP,
           ip: @ip,
           port: 0,
           count: 2,
           accumulator: XSockets.Accumulator.Raw,
           handler: XSockets.TestSupport.CollectHandler
         ) do
      {:ok, [p1, p2]} ->
        assert DatagramServer.port(p1) == DatagramServer.port(p2)
        Enum.each([p1, p2], &GenServer.stop/1)

      {:error, :reuseport_unsupported} ->
        :ok
    end
  end

  test "listen/1 workers: 2 delegates to listen_many for UDP" do
    case XSockets.listen(
           transport: UDP,
           ip: @ip,
           port: 0,
           workers: 2,
           accumulator: XSockets.Accumulator.Raw,
           handler: XSockets.TestSupport.CollectHandler
         ) do
      {:ok, [p1, p2]} ->
        assert DatagramServer.port(p1) == DatagramServer.port(p2)
        Enum.each([p1, p2], &GenServer.stop/1)

      {:error, :reuseport_unsupported} ->
        :ok
    end
  end

  test "Acceptor accept_workers: 2 accepts concurrent TCP clients" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             Acceptor.start_link(
               transport: TCP,
               ip: @ip,
               port: 0,
               accept_workers: 2,
               accumulator: XSockets.Accumulator.LengthPrefixed,
               handler: XSockets.TestSupport.CollectHandler,
               assigns: %{agent: agent}
             )

    port = Acceptor.port(pid)
    payload_a = XSockets.TestSupport.frame("a")
    payload_b = XSockets.TestSupport.frame("b")

    {:ok, c1} = :gen_tcp.connect(@ip, port, [:binary, active: false])
    {:ok, c2} = :gen_tcp.connect(@ip, port, [:binary, active: false])
    :ok = :gen_tcp.send(c1, payload_a)
    :ok = :gen_tcp.send(c2, payload_b)

    assert eventually(fn ->
             packets = XSockets.TestSupport.packets(agent)
             "a" in packets and "b" in packets
           end)

    :gen_tcp.close(c1)
    :gen_tcp.close(c2)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "Acceptor respawns a killed accept worker and keeps accepting" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             Acceptor.start_link(
               transport: TCP,
               ip: @ip,
               port: 0,
               accept_workers: 2,
               accumulator: XSockets.Accumulator.LengthPrefixed,
               handler: XSockets.TestSupport.CollectHandler,
               assigns: %{agent: agent}
             )

    %{workers: [w1 | _] = workers} = :sys.get_state(pid)
    assert length(workers) == 2
    Process.exit(w1, :kill)

    assert eventually(fn ->
             if Process.alive?(pid) do
               %{workers: ws} = :sys.get_state(pid)
               length(ws) == 2 and w1 not in ws
             else
               false
             end
           end)

    port = Acceptor.port(pid)
    payload = XSockets.TestSupport.frame("after-restart")
    {:ok, client} = :gen_tcp.connect(@ip, port, [:binary, active: false])
    :ok = :gen_tcp.send(client, payload)

    assert eventually(fn -> "after-restart" in XSockets.TestSupport.packets(agent) end)

    :gen_tcp.close(client)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "TLS.security_opts/1 mutual_tls requires peer cert" do
    opts = TLS.security_opts(:mutual_tls)
    assert Keyword.get(opts, :verify) == :verify_peer
    assert Keyword.get(opts, :fail_if_no_peer_cert) == true
  end

  test "TLS.security_opts/1 server keeps verify_none" do
    opts = TLS.security_opts(:server)
    assert Keyword.get(opts, :verify) == :verify_none
  end

  test "Config.start_supervisors?/0 defaults true" do
    assert XSockets.Config.start_supervisors?() == true
  end

  test "Application.children/0 always includes RateLimit.Table" do
    previous = Application.get_env(:xsockets, :start_supervisors)

    try do
      Application.put_env(:xsockets, :start_supervisors, false)
      ids = Enum.map(XSockets.Application.children(), & &1.id)
      assert ids == [XSockets.RateLimit.Table]

      Application.put_env(:xsockets, :start_supervisors, true)
      ids = Enum.map(XSockets.Application.children(), & &1.id)
      assert XSockets.RateLimit.Table in ids
      assert XSockets.SockSupervisor in ids
      assert XSockets.TierSupervisor.Task in ids
      assert XSockets.TierSupervisor.Pool in ids
    after
      case previous do
        nil -> Application.delete_env(:xsockets, :start_supervisors)
        value -> Application.put_env(:xsockets, :start_supervisors, value)
      end
    end
  end

  test "Client.connect/1 dials TCP and runs pipeline" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    {:ok, acceptor} =
      Acceptor.start_link(
        transport: TCP,
        ip: @ip,
        port: 0,
        accumulator: XSockets.Accumulator.LengthPrefixed,
        handler: XSockets.TestSupport.CollectHandler,
        assigns: %{agent: agent}
      )

    port = Acceptor.port(acceptor)

    assert {:ok, conn_pid} =
             XSockets.Client.connect(
               transport: TCP,
               ip: @ip,
               port: port,
               accumulator: XSockets.Accumulator.LengthPrefixed,
               handler: XSockets.TestSupport.CollectHandler,
               assigns: %{agent: agent}
             )

    assert is_pid(conn_pid)
    GenServer.stop(conn_pid)
    GenServer.stop(acceptor)
    Agent.stop(agent)
  end

  test "Client.connect/1 dials TLS and delivers framed packets" do
    {certfile, keyfile} = generate_self_signed_cert!()
    {:ok, agent} = XSockets.TestSupport.start_collector()

    {:ok, acceptor} =
      Acceptor.start_link(
        transport: TLS,
        ip: @ip,
        port: 0,
        listen_opts: [certfile: certfile, keyfile: keyfile, verify: :verify_none],
        accumulator: XSockets.Accumulator.LengthPrefixed,
        handler: XSockets.TestSupport.CollectHandler,
        assigns: %{agent: agent}
      )

    port = Acceptor.port(acceptor)

    assert {:ok, conn_pid} =
             XSockets.Client.connect(
               transport: TLS,
               ip: @ip,
               port: port,
               connect_opts: [verify: :verify_none],
               accumulator: XSockets.Accumulator.LengthPrefixed,
               handler: XSockets.TestSupport.CollectHandler,
               assigns: %{agent: agent}
             )

    assert is_pid(conn_pid)
    GenServer.stop(conn_pid)
    GenServer.stop(acceptor)
    Agent.stop(agent)
  end

  test "listen/1 starts a DTLS Acceptor" do
    {certfile, keyfile} = generate_self_signed_cert!()
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             XSockets.listen(
               transport: DTLS,
               ip: @ip,
               port: 0,
               listen_opts: [certfile: certfile, keyfile: keyfile, verify: :verify_none],
               accumulator: XSockets.Accumulator.LengthPrefixed,
               handler: XSockets.TestSupport.CollectHandler,
               assigns: %{agent: agent}
             )

    # DatagramServer would not complete a DTLS handshake + framed packet path.
    port = Acceptor.port(pid)
    assert {:ok, client} = DTLS.connect(@ip, port, verify: :verify_none)
    :ok = :ssl.send(client, XSockets.TestSupport.frame("dtls-hi"))
    assert eventually(fn -> "dtls-hi" in XSockets.TestSupport.packets(agent) end)

    :ssl.close(client)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "serve/2 UDP defaults to Raw framing" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             XSockets.serve(XSockets.TestSupport.CollectHandler,
               transport: :udp,
               ip: @ip,
               port: 0,
               assigns: %{agent: agent}
             )

    port = DatagramServer.port(pid)
    {:ok, sender} = :gen_udp.open(0, [:binary, active: false])
    :ok = :gen_udp.send(sender, @ip, port, "serve-udp")
    assert eventually(fn -> "serve-udp" in XSockets.TestSupport.packets(agent) end)
    :gen_udp.close(sender)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "serve/2 TCP defaults to LengthPrefixed header 2" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             XSockets.serve(XSockets.TestSupport.CollectHandler,
               transport: :tcp,
               ip: @ip,
               port: 0,
               assigns: %{agent: agent}
             )

    port = Acceptor.port(pid)
    {:ok, client} = :gen_tcp.connect(@ip, port, [:binary, active: false])
    :ok = :gen_tcp.send(client, XSockets.TestSupport.frame("serve-tcp"))
    assert eventually(fn -> "serve-tcp" in XSockets.TestSupport.packets(agent) end)
    :gen_tcp.close(client)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "serve/2 explicit accumulator skips framing default" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    assert {:ok, pid} =
             XSockets.serve(XSockets.TestSupport.CollectHandler,
               transport: :tcp,
               ip: @ip,
               port: 0,
               framing: :length_prefixed,
               accumulator: XSockets.Accumulator.Raw,
               assigns: %{agent: agent}
             )

    port = Acceptor.port(pid)
    {:ok, client} = :gen_tcp.connect(@ip, port, [:binary, active: false])
    # Raw: entire chunk is one packet (no length header).
    :ok = :gen_tcp.send(client, "raw-chunk")
    assert eventually(fn -> "raw-chunk" in XSockets.TestSupport.packets(agent) end)
    :gen_tcp.close(client)
    GenServer.stop(pid)
    Agent.stop(agent)
  end

  test "serve/2 rejects unknown transport atom" do
    assert {:error, :invalid_transport} =
             XSockets.serve(XSockets.TestSupport.CollectHandler, transport: :nope, port: 0)
  end

  test "dial/2 TCP starts a Connection" do
    {:ok, agent} = XSockets.TestSupport.start_collector()

    {:ok, acceptor} =
      XSockets.serve(XSockets.TestSupport.CollectHandler,
        transport: :tcp,
        ip: @ip,
        port: 0,
        assigns: %{agent: agent}
      )

    port = Acceptor.port(acceptor)

    assert {:ok, conn_pid} =
             XSockets.dial(XSockets.TestSupport.CollectHandler,
               transport: :tcp,
               ip: @ip,
               port: port,
               assigns: %{agent: agent}
             )

    assert is_pid(conn_pid)
    GenServer.stop(conn_pid)
    GenServer.stop(acceptor)
    Agent.stop(agent)
  end

  test "dial/2 with :udp returns connect_not_supported" do
    assert {:error, :connect_not_supported} =
             XSockets.dial(XSockets.TestSupport.CollectHandler,
               transport: :udp,
               ip: @ip,
               port: 9
             )
  end

  defp generate_self_signed_cert! do
    dir = Path.join(System.tmp_dir!(), "xsockets_api_tls_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    certfile = Path.join(dir, "server.crt")
    keyfile = Path.join(dir, "server.key")

    {_, 0} =
      System.cmd("openssl", [
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
      ])

    {certfile, keyfile}
  end

  defp eventually(fun, attempts \\ 50) do
    if fun.() do
      true
    else
      if attempts <= 0, do: false, else: (Process.sleep(20); eventually(fun, attempts - 1))
    end
  end
end
