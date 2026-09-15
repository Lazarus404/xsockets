defmodule XSockets.AccumulatorRawTest do
  use ExUnit.Case, async: true

  alias XSockets.Accumulator.Raw

  test "each push is immediately poppable" do
    acc = Raw.init([]) |> Raw.push("one", %{}) |> Raw.push("two", %{})

    assert {:ok, "one", %{}, acc} = Raw.pop(acc)
    assert {:ok, "two", %{}, acc} = Raw.pop(acc)
    assert {:more, _} = Raw.pop(acc)
  end

  test "max_size overflow surfaces on pop and draining continues" do
    acc =
      Raw.init(max_size: 2)
      |> Raw.push("one", %{})
      |> Raw.push("two", %{})
      |> Raw.push("three", %{})

    assert {:error, :buffer_overflow, acc} = Raw.pop(acc)
    assert {:ok, "two", %{}, acc} = Raw.pop(acc)
    assert {:ok, "three", %{}, acc} = Raw.pop(acc)
    assert {:more, _} = Raw.pop(acc)
  end
end
