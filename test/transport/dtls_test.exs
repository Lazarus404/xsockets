defmodule XSockets.DTLSTest do
  use ExUnit.Case

  alias XSockets.Transport.DTLS

  @test_ip {127, 0, 0, 1}

  setup do
    previous_ssl = Application.get_env(:xsockets, :ssl_options)
    previous_certs = Application.get_env(:xsockets, :certs)

    on_exit(fn ->
      restore_env(:xsockets, :ssl_options, previous_ssl)
      restore_env(:xsockets, :certs, previous_certs)
      File.rm_rf(Path.join(System.tmp_dir!(), "xsockets_dtls_test"))
    end)

    :ok
  end

  test "listen without certificates returns error" do
    Application.delete_env(:xsockets, :certs)
    assert {:error, :no_certificates_configured} = DTLS.listen(@test_ip, 0, [])
  end

  test "listen with certs only in opts does not require application env" do
    {certfile, keyfile} = generate_self_signed_cert!()

    Application.delete_env(:xsockets, :certs)

    assert {:ok, sock} =
             DTLS.listen(@test_ip, 0, certfile: certfile, keyfile: keyfile, verify: :verify_none)

    :ssl.close(sock)
  end

  test "listen uses DTLS versions even when ssl_options specifies TLS versions" do
    {certfile, keyfile} = generate_self_signed_cert!()

    Application.put_env(:xsockets, :ssl_options,
      versions: [~c"tlsv1.2", ~c"tlsv1.3"],
      verify: :verify_none
    )

    Application.put_env(:xsockets, :certs, certfile: certfile, keyfile: keyfile)

    assert {:ok, sock} = DTLS.listen(@test_ip, 0, [])
    :ssl.close(sock)
  end

  test "connect/3 is exported for outbound DTLS dial" do
    assert function_exported?(DTLS, :connect, 3)
  end

  test "connect/3 handshakes against a DTLS listener" do
    {certfile, keyfile} = generate_self_signed_cert!()

    assert {:ok, listen} =
             DTLS.listen(@test_ip, 0, certfile: certfile, keyfile: keyfile, verify: :verify_none)

    {:ok, {_ip, port}} = :ssl.sockname(listen)
    parent = self()

    spawn(fn ->
      send(parent, {:accepted, DTLS.accept(listen, 5_000)})
    end)

    assert {:ok, client} = DTLS.connect(@test_ip, port, verify: :verify_none)
    assert {:ok, info} = :ssl.connection_information(client, [:protocol])
    assert Keyword.get(info, :protocol) in [:"dtlsv1.2", :"dtlsv1.3", :dtlsv1]
    :ssl.close(client)
    assert_receive {:accepted, {:ok, _server}}, 5_000
    :ssl.close(listen)
  end

  test "close/1 emits socket_closed with protocol dtls" do
    {certfile, keyfile} = generate_self_signed_cert!()

    assert {:ok, sock} =
             DTLS.listen(@test_ip, 0, certfile: certfile, keyfile: keyfile, verify: :verify_none)

    ref = :telemetry_test.attach_event_handlers(self(), [[:xsockets, :socket_closed]])
    assert :ok = DTLS.close(sock)
    assert_receive {[:xsockets, :socket_closed], ^ref, %{protocol: :dtls}, _}
  end

  defp generate_self_signed_cert! do
    dir = Path.join(System.tmp_dir!(), "xsockets_dtls_test")
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

  defp restore_env(app, key, value) do
    case value do
      nil -> Application.delete_env(app, key)
      value -> Application.put_env(app, key, value)
    end
  end
end
