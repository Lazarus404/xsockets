defmodule XSockets.RFC8656Test do
  @moduledoc """
  RFC 8656 *transport* holes exercised at the socket layer (IPv6 listen paths).
  No STUN/TURN message semantics.

  Skipped here: ICMP error-queue (needs OS recverr), ECN/DSCP/flow-label send
  options, TLS IPv6 (same :inet6 path as UDP/TCP).
  """
  use ExUnit.Case, async: false

  alias XSockets.Transport.{UDP, TCP}

  @v4 {127, 0, 0, 1}
  @v6 {0, 0, 0, 0, 0, 0, 0, 1}

  describe "IPv6 listen (RFC 8656 5 / 13)" do
    test "OTP rejects {:inet6, true} (regression: use bare :inet6)" do
      # Linux CI used to pass `{:inet6, true}` via Keyword; OTP wants `:inet6`.
      assert catch_exit(:gen_udp.open(0, [{:inet6, true}, {:ip, @v6}, :binary])) == :badarg
    end

    test "UDP.listen on an IPv6 loopback address succeeds" do
      assert {:ok, sock} = UDP.listen(@v6, 0, [])
      assert {:ok, {ip, _port}} = UDP.sockname(sock)
      assert tuple_size(ip) == 8
      UDP.close(sock)
    end

    test "TCP.listen on an IPv6 loopback address succeeds" do
      assert {:ok, sock} = TCP.listen(@v6, 0, [])
      assert {:ok, {ip, _port}} = TCP.sockname(sock)
      assert tuple_size(ip) == 8
      TCP.close(sock)
    end
  end

  describe "ICMP error parse (RFC 8656 15)" do
    test "parse_icmp_error accepts tuple and map shapes" do
      peer = {{8, 8, 8, 8}, 3478}

      assert {:icmp, %{type: 3, code: 4, peer: ^peer}} =
               UDP.handle_message({:udp_error, nil, {:icmp, 3, 4, 1280, peer}}, nil)

      assert {:icmp, %{type: 3, code: 4, peer: ^peer}} =
               UDP.handle_message(
                 {:udp_error, nil, %{type: 3, code: 4, info: 1280, peer: peer}},
                 nil
               )
    end
  end

  describe "DONT-FRAGMENT helper (RFC 8656 14)" do
    test "set_dont_fragment returns :ok or {:error, :not_supported}" do
      {:ok, sock} = UDP.listen(@v4, 0, [])

      assert apply(UDP, :set_dont_fragment, [sock]) in [:ok, {:error, :not_supported}]

      UDP.close(sock)
    end
  end

  describe "outbound TCP connect (RFC 6062 / RFC 8656 5)" do
    test "connect/3 opens a client socket to a listening peer" do
      {:ok, listen} = TCP.listen(@v4, 0, [])
      {:ok, {_, port}} = TCP.sockname(listen)

      parent = self()

      Task.start(fn ->
        {:ok, accepted} = TCP.accept(listen, 2_000)
        send(parent, {:accepted, accepted})
      end)

      assert {:ok, client} = apply(TCP, :connect, [@v4, port, []])
      assert_receive {:accepted, _accepted}, 2_000
      TCP.close(client)
      TCP.close(listen)
    end
  end
end
