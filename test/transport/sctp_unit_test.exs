defmodule XSockets.SCTPUnitTest do
  use ExUnit.Case, async: true

  alias XSockets.Transport.SCTP

  test "module exports transport callbacks" do
    behaviours = SCTP.__info__(:attributes)[:behaviour] || []
    assert XSockets.Transport in behaviours
  end
end
