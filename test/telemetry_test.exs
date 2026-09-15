defmodule XSockets.TelemetryTest do
  use ExUnit.Case, async: false

  test "attach_handlers/0 attaches without error" do
    assert :ok = XSockets.Telemetry.attach_handlers()
  end

  test "emit/3 is a no-op when telemetry disabled" do
    previous = Application.get_env(:xsockets, :telemetry_enabled)

    try do
      Application.put_env(:xsockets, :telemetry_enabled, false)
      assert :ok = XSockets.Telemetry.emit(:message_sent, %{bytes: 1}, %{tier: :root})
    after
      if previous == nil do
        Application.delete_env(:xsockets, :telemetry_enabled)
      else
        Application.put_env(:xsockets, :telemetry_enabled, previous)
      end
    end
  end
end
