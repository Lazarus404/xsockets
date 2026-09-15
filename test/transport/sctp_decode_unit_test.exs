defmodule XSockets.Transport.SCTPDecodeUnitTest do
  use ExUnit.Case, async: true

  alias XSockets.Transport.SCTP

  test "handle_message normalizes sctp payload (legacy 6-tuple)" do
    data = "payload"

    assert {:data, ^data, {{127, 0, 0, 1}, 5000}} =
             SCTP.handle_message({:sctp, :sock, {127, 0, 0, 1}, 5000, [], data}, :sock)
  end

  test "handle_message normalizes OTP 5-tuple {anc, data}" do
    msg = {:sctp, :sock, {127, 0, 0, 1}, 9, {[], "hi"}}
    assert {:data, "hi", {{127, 0, 0, 1}, 9}} = SCTP.handle_message(msg, :sock)
  end

  test "decode_message includes assoc meta from anc" do
    anc = [{:sctp_sndrcvinfo, 2, 0, 0, 0, 0, 0, 0, 0, 42}]

    assert {:data, "hi", {{127, 0, 0, 1}, 9}, %{assoc_id: 42, stream: 2}} =
             SCTP.decode_message({:sctp, :sock, {127, 0, 0, 1}, 9, {anc, "hi"}})
  end

  test "decode_message maps assoc_change comm_up" do
    event = {:sctp_assoc_change, :comm_up, 0, 10, 10, 7}

    assert {:assoc_up, 7, {{1, 2, 3, 4}, 100}} =
             SCTP.decode_message({:sctp, :sock, {1, 2, 3, 4}, 100, {[], event}})
  end

  test "decode_message maps assoc_change comm_lost" do
    event = {:sctp_assoc_change, :comm_lost, 0, 0, 0, 7}

    assert {:assoc_down, 7, :comm_lost} =
             SCTP.decode_message({:sctp, :sock, {1, 2, 3, 4}, 100, {[], event}})
  end

  test "assoc events are ignore on Transport.handle_message" do
    event = {:sctp_assoc_change, :comm_up, 0, 10, 10, 7}

    assert :ignore =
             SCTP.handle_message({:sctp, :sock, {1, 2, 3, 4}, 100, {[], event}}, :sock)
  end
end
