defmodule XSockets.TLSTest do
  use ExUnit.Case

  alias XSockets.{Acceptor, Transport.TLS}
  alias XSockets.TestSupport

  @test_ip {127, 0, 0, 1}

  setup do
    previous_ssl = Application.get_env(:xsockets, :ssl_options)
    previous_certs = Application.get_env(:xsockets, :certs)

    on_exit(fn ->
      restore_env(:xsockets, :ssl_options, previous_ssl)
      restore_env(:xsockets, :certs, previous_certs)
      File.rm_rf(Path.join(System.tmp_dir!(), "xsockets_tls_test"))
    end)

    :ok
  end

  test "listen without certificates returns error" do
    Application.delete_env(:xsockets, :certs)
    assert {:error, :no_certificates_configured} = TLS.listen(@test_ip, 0, [])
  end

  test "listen with certs only in opts does not require application env" do
    {certfile, keyfile} = generate_self_signed_cert!()

    Application.delete_env(:xsockets, :certs)

    assert {:ok, sock} =
             TLS.listen(@test_ip, 0, certfile: certfile, keyfile: keyfile, verify: :verify_none)

    :ssl.close(sock)
  end

  test "security_opts excludes legacy TLS versions and weak ciphers" do
    opts = TLS.security_opts()

    assert :"tlsv1.2" in Keyword.get(opts, :versions)
    assert :"tlsv1.3" in Keyword.get(opts, :versions)
    refute :tlsv1 in Keyword.get(opts, :versions)
    refute :"tlsv1.1" in Keyword.get(opts, :versions)
    assert Keyword.get(opts, :honor_cipher_order) == true

    for suite <- Keyword.get(opts, :ciphers) do
      name =
        case suite do
          %{cipher: cipher} -> cipher
          {cipher, _, _, _} -> cipher
          {cipher, _, _} -> cipher
          cipher when is_atom(cipher) -> cipher
        end
        |> Atom.to_string()
        |> String.downcase()

      refute String.contains?(name, "des")
      refute String.contains?(name, "rc4")
      refute String.contains?(name, "null")
      refute String.contains?(name, "cbc")
    end

    tls13 = :ssl.cipher_suites(:exclusive, :"tlsv1.3")
    ciphers = Keyword.get(opts, :ciphers)
    assert Enum.any?(tls13, &(&1 in ciphers))
  end

  test "TLS 1.3 client can handshake against default listen ciphers" do
    {certfile, keyfile} = generate_self_signed_cert!()

    assert {:ok, listen} =
             TLS.listen(@test_ip, 0, certfile: certfile, keyfile: keyfile, verify: :verify_none)

    {:ok, {_ip, port}} = :ssl.sockname(listen)
    parent = self()

    spawn(fn ->
      send(parent, {:accepted, TLS.accept(listen, 5_000)})
    end)

    assert {:ok, client} = :ssl.connect(@test_ip, port, [verify: :verify_none], 5_000)
    assert {:ok, info} = :ssl.connection_information(client, [:protocol])
    assert Keyword.get(info, :protocol) in [:"tlsv1.2", :"tlsv1.3"]
    :ssl.close(client)
    assert_receive {:accepted, {:ok, _server}}, 5_000
    :ssl.close(listen)
  end

  test "acceptor delivers first TLS record to the connection, not itself" do
    {certfile, keyfile} = generate_self_signed_cert!()
    {:ok, agent} = TestSupport.start_collector()

    {:ok, acceptor} =
      Acceptor.start_link(
        transport: TLS,
        ip: @test_ip,
        port: 0,
        listen_opts: [certfile: certfile, keyfile: keyfile, verify: :verify_none],
        handler: TestSupport.CollectHandler,
        accumulator: {XSockets.Accumulator.LengthPrefixed, header_size: 2},
        assigns: %{agent: agent}
      )

    ref = Process.monitor(acceptor)
    port = Acceptor.port(acceptor)

    assert {:ok, client} = :ssl.connect(@test_ip, port, [verify: :verify_none], 5_000)
    :ok = :ssl.send(client, TestSupport.frame("hello"))

    assert eventually(fn -> TestSupport.packets(agent) == ["hello"] end)
    refute_receive {:DOWN, ^ref, :process, ^acceptor, _}, 200

    :ssl.close(client)
    GenServer.stop(acceptor)
    Agent.stop(agent)
  end

  test "listen normalizes charlist TLS versions from ssl_options" do
    {certfile, keyfile} = generate_self_signed_cert!()

    Application.put_env(:xsockets, :ssl_options,
      versions: [~c"tlsv1.2", ~c"tlsv1.3"],
      verify: :verify_none
    )

    Application.put_env(:xsockets, :certs, certfile: certfile, keyfile: keyfile)

    assert {:ok, sock} = TLS.listen(@test_ip, 0, [])
    :ssl.close(sock)
  end

  defp generate_self_signed_cert! do
    dir = Path.join(System.tmp_dir!(), "xsockets_tls_test")
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

  defp eventually(fun, attempts \\ 20) do
    if fun.() do
      :ok
    else
      if attempts > 0 do
        Process.sleep(50)
        eventually(fun, attempts - 1)
      else
        flunk("condition not met")
      end
    end
  end

  defp restore_env(app, key, value) do
    case value do
      nil -> Application.delete_env(app, key)
      value -> Application.put_env(app, key, value)
    end
  end
end
