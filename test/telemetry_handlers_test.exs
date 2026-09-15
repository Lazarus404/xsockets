defmodule XSockets.TelemetryHandlersTest do
  use ExUnit.Case, async: false

  alias XSockets.Telemetry

  setup do
    Telemetry.detach_handlers()
    on_exit(fn -> Telemetry.detach_handlers() end)
    :ok
  end

  test "attach_handlers/0 wires events that the library emits" do
    assert :ok = Telemetry.attach_handlers()

    ids =
      :telemetry.list_handlers([])
      |> Enum.map(& &1.id)
      |> MapSet.new()

    assert "xsockets_xsockets_listener_started" in ids
    assert "xsockets_xsockets_udp_listener_created" in ids
    assert "xsockets_xsockets_rate_limit_exceeded" in ids
    assert "xsockets_xsockets_socket_closed" in ids
    refute "xsockets_xsockets_socket_opened" in ids
    refute "xsockets_xsockets_tcp_listener_started" in ids
  end

  test "attached handlers increment counters for emitted events" do
    before = Telemetry.get_metrics().rate_limit_hits
    assert :ok = Telemetry.attach_handlers()
    assert :ok = Telemetry.emit(:rate_limit_exceeded, %{}, %{ip: {203, 0, 113, 9}})
    assert Telemetry.get_metrics().rate_limit_hits == before + 1
  end
end
