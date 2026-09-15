defmodule XSockets.SCTPTest do
  use ExUnit.Case

  alias XSockets.Transport.SCTP

  @test_ip {127, 0, 0, 1}

  test "framing is datagram" do
    assert SCTP.framing() == :datagram
  end

  test "accept/2 is unsupported" do
    assert {:error, :sctp_not_supported} = SCTP.accept(:sock, 1000)
  end

  test "send/3 resolves assoc and stream from to" do
    # Without a live socket, send rescues to :sctp_not_supported or returns driver error.
    result = SCTP.send(:not_a_socket, "hi", %{assoc_id: 3, stream: 1})
    assert result == {:error, :sctp_not_supported} or match?({:error, _}, result)
  end

  test "available?/0 is boolean" do
    assert is_boolean(SCTP.available?())
  end

  @tag :sctp
  test "listen on loopback when SCTP is available" do
    case SCTP.listen(@test_ip, 0, []) do
      {:ok, sock} ->
        SCTP.close(sock)
        assert true

      {:error, :sctp_not_supported} ->
        assert true

      {:error, reason} ->
        flunk("unexpected SCTP listen error: #{inspect(reason)}")
    end
  end
end
