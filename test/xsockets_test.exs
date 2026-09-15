defmodule XSockets.SmokeTest do
  use ExUnit.Case, async: true

  alias XSockets.Transport

  test "transport behaviour modules are loaded" do
    for mod <- [
          Transport.TCP,
          Transport.UDP,
          Transport.TLS,
          Transport.DTLS,
          Transport.SCTP
        ] do
      assert Code.ensure_loaded?(mod)
      assert function_exported?(mod, :listen, 3)
      assert function_exported?(mod, :handle_message, 2)
    end
  end
end
