defmodule XSockets.Sctp.DcepTest do
  use ExUnit.Case, async: true

  alias XSockets.Sctp.Dcep

  test "open encode decode round trip" do
    open = %Dcep.Open{
      reliability: :reliable,
      order: :ordered,
      label: "spike",
      protocol: "",
      priority: 0,
      param: 0
    }

    assert {:ok, ^open} = Dcep.decode(Dcep.encode(open))
  end

  test "ack encode decode" do
    assert {:ok, %Dcep.Ack{}} = Dcep.decode(Dcep.encode(%Dcep.Ack{}))
  end
end
