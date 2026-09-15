defmodule XSockets.AccumulatorLengthPrefixedTest do
  use ExUnit.Case, async: true

  alias XSockets.Accumulator.LengthPrefixed

  defp init(opts \\ []), do: LengthPrefixed.init(Keyword.merge([header_size: 2], opts))

  test "waits for more bytes when header is incomplete" do
    acc = init() |> LengthPrefixed.push(<<0>>, %{})

    assert {:more, _} = LengthPrefixed.pop(acc)
  end

  test "extracts a whole packet across pushes" do
    acc =
      init()
      |> LengthPrefixed.push(<<0, 3, "AB">>, %{})
      |> LengthPrefixed.push("C", %{})

    assert {:ok, "ABC", _, acc} = LengthPrefixed.pop(acc)
    assert {:more, _} = LengthPrefixed.pop(acc)
  end

  test "extracts multiple packets from one buffer" do
    acc =
      init()
      |> LengthPrefixed.push(<<0, 2, "AB", 0, 3, "CDE">>, %{})

    assert {:ok, "AB", _, acc} = LengthPrefixed.pop(acc)
    assert {:ok, "CDE", _, acc} = LengthPrefixed.pop(acc)
    assert {:more, _} = LengthPrefixed.pop(acc)
  end

  test "returns buffer overflow when max_size exceeded" do
    acc = init(max_size: 4) |> LengthPrefixed.push(<<0, 0, 0, 0, 0>>, %{})

    assert {:error, :buffer_overflow, _} = LengthPrefixed.pop(acc)
  end
end
