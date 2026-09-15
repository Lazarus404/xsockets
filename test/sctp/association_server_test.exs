defmodule XSockets.Sctp.Association.ServerTest do
  use ExUnit.Case, async: false

  alias XSockets.Sctp.Association
  alias XSockets.Sctp.Association.Server

  setup do
    if Association.available?() do
      :ok
    else
      {:skip, "ex_sctp NIF not loaded (need Rust + mix deps.compile)"}
    end
  end

  test "Server.connect/1 notifies owner with transmit events" do
    {:ok, pid} = Server.start_link(role: :active, owner: self())
    :ok = Server.connect(pid)

    assert_receive {:xsockets_sctp, events}, 1_000
    assert Enum.any?(events, &match?({:transmit, _}, &1))

    GenServer.stop(pid)
  end

  test "Server.open_channel/3 before connect notifies {:error, :not_connected}" do
    {:ok, pid} = Server.start_link(role: :passive, owner: self())
    :ok = Server.open_channel(pid, "early")

    assert_receive {:xsockets_sctp, [{:error, :not_connected}]}, 1_000

    GenServer.stop(pid)
  end
end
