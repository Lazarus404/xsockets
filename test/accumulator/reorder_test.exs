defmodule XSockets.AccumulatorReorderTest do
  use ExUnit.Case, async: true

  alias XSockets.Accumulator.{Raw, Reorder}
  alias XSockets.Config

  defp key_from_first_byte(packet, _meta), do: :binary.at(packet, 0)

  defp init(opts \\ []) do
    Reorder.init(
      Keyword.merge(
        [
          inner: Raw,
          key_fun: &key_from_first_byte/2
        ],
        opts
      )
    )
  end

  defp push(acc, keys) do
    Enum.reduce(keys, acc, fn key, acc ->
      Reorder.push(acc, <<key>>, %{})
    end)
  end

  defp pop_all(acc) do
    pop_all(acc, [])
  end

  defp pop_all(acc, packets) do
    case Reorder.pop(acc) do
      {:ok, packet, _meta, acc} -> pop_all(acc, [packet | packets])
      {:more, acc} -> {Enum.reverse(packets), acc}
    end
  end

  test "releases packets in ascending key order" do
    acc = init() |> push([1, 2, 3])

    assert {packets, _} = pop_all(acc)
    assert packets == [<<1>>, <<2>>, <<3>>]
  end

  test "buffers out-of-order packets until contiguous keys are available" do
    acc = init() |> push([2, 3, 1])

    assert {packets, _} = pop_all(acc)
    assert packets == [<<1>>, <<2>>, <<3>>]
  end

  test "passes through :unordered packets immediately" do
    acc =
      init(
        key_fun: fn
          <<0, _::binary>>, _ -> :unordered
          packet, meta -> key_from_first_byte(packet, meta)
        end
      )
      |> push([2])
      |> Reorder.push(<<0, "skip">>, %{})

    assert {:ok, <<0, "skip">>, _, acc} = Reorder.pop(acc)
    assert {packets, _} = pop_all(acc)
    assert packets == [<<2>>]
  end

  test "flush_oldest overflow releases the lowest pending key when window is full" do
    acc = init(window: 2, on_overflow: :flush_oldest) |> push([1, 5, 3])

    assert {:ok, <<1>>, _, acc} = Reorder.pop(acc)
    assert {:ok, <<3>>, _, acc} = Reorder.pop(acc)
    assert {:more, _} = Reorder.pop(acc)
  end

  test "drop_newest overflow ignores the newest packet" do
    acc = init(window: 1, on_overflow: :drop_newest) |> push([1, 2])

    assert {packets, acc} = pop_all(acc)
    assert packets == [<<1>>]
    assert {:more, _} = Reorder.pop(acc)
  end

  test "flush_all overflow releases every pending packet in key order" do
    acc = init(window: 2, on_overflow: :flush_all) |> push([1, 4, 2, 3])

    assert {packets, _} = pop_all(acc)
    assert packets == [<<1>>, <<2>>, <<3>>, <<4>>]
  end

  test "drops duplicate keys after in-order release without stalling" do
    acc = init() |> push([1, 2, 3])

    assert {packets, acc} = pop_all(acc)
    assert packets == [<<1>>, <<2>>, <<3>>]

    acc = acc |> push([2, 4])

    assert {packets, _} = pop_all(acc)
    assert packets == [<<4>>]
  end

  test "drops late keys below the floor after in-order release" do
    acc = init() |> push([1, 2, 3])

    assert {packets, acc} = pop_all(acc)
    assert packets == [<<1>>, <<2>>, <<3>>]

    acc = acc |> push([1])
    assert {:more, _} = Reorder.pop(acc)
  end

  test "still reorders gap-filling packets within the window" do
    acc = init() |> push([1, 3, 2])

    assert {packets, _} = pop_all(acc)
    assert packets == [<<1>>, <<2>>, <<3>>]
  end

  test "drops stale keys after a forced release has advanced next_key" do
    now = 1_000
    clock = fn _unit -> now end

    acc =
      init(max_delay_ms: 50, clock: clock, window: 16)
      |> push([1, 3])

    assert {:ok, <<1>>, _, acc} = Reorder.pop(acc)
    assert {:more, acc} = Reorder.pop(acc)

    acc = %{acc | clock: fn _unit -> now + 50 end}
    assert {:ok, <<3>>, _, acc} = Reorder.pop(acc)

    acc = acc |> push([1])
    assert {:more, _} = Reorder.pop(acc)
  end

  test "max_delay_ms forces release of the oldest held packet" do
    now = 1_000
    clock = fn _unit -> now end

    acc =
      init(max_delay_ms: 50, clock: clock)
      |> push([1, 3])

    assert {:ok, <<1>>, _, acc} = Reorder.pop(acc)
    assert {:more, acc} = Reorder.pop(acc)

    acc = %{acc | clock: fn _unit -> now + 50 end}

    assert {:ok, <<3>>, _, acc} = Reorder.pop(acc)
    assert {:more, _} = Reorder.pop(acc)
  end

  test "disabled reorder delegates to the inner accumulator" do
    acc =
      init(enabled: false)
      |> Reorder.push("hello", %{})
      |> Reorder.push(" world", %{})

    assert {:ok, "hello", _, acc} = Reorder.pop(acc)
    assert {:ok, " world", _, acc} = Reorder.pop(acc)
    assert {:more, _} = Reorder.pop(acc)
  end

  test "ready queue overflow surfaces buffer_overflow and continues draining" do
    acc =
      init(max_size: 2, key_fun: fn _packet, _meta -> :unordered end)
      |> Reorder.push("one", %{})
      |> Reorder.push("two", %{})
      |> Reorder.push("three", %{})

    assert {:error, :buffer_overflow, acc} = Reorder.pop(acc)
    assert {:ok, "two", _, acc} = Reorder.pop(acc)
    assert {:ok, "three", _, acc} = Reorder.pop(acc)
    assert {:more, _} = Reorder.pop(acc)
  end

  test "Config.reorder_opts/2 applies library, host, and explicit precedence" do
    original = Application.get_env(:xsockets, :reorder)
    original_host = Application.get_env(:xsockets, :config_app)
    original_host_reorder = Application.get_env(:xsockets_test_host, :reorder)

    on_exit(fn ->
      restore_env(:xsockets, :reorder, original)
      restore_env(:xsockets, :config_app, original_host)
      restore_env(:xsockets_test_host, :reorder, original_host_reorder)
    end)

    Application.put_env(:xsockets, :reorder, rtp: [window: 32, max_delay_ms: 250])
    Application.put_env(:xsockets, :config_app, :xsockets_test_host)
    Application.put_env(:xsockets_test_host, :reorder, rtp: [window: 40])

    assert Config.reorder_opts(nil, [])[:window] == 16
    assert Config.reorder_opts(nil, [])[:max_delay_ms] == 100
    assert Config.reorder_opts(nil, [])[:on_overflow] == :flush_oldest
    assert Config.reorder_opts(nil, [])[:enabled] == true

    rtp_opts = Config.reorder_opts(:rtp, [])
    assert rtp_opts[:window] == 40
    assert rtp_opts[:max_delay_ms] == 250
    assert rtp_opts[:on_overflow] == :flush_oldest

    override_opts = Config.reorder_opts(:rtp, window: 8, on_overflow: :flush_all)
    assert override_opts[:window] == 8
    assert override_opts[:max_delay_ms] == 250
    assert override_opts[:on_overflow] == :flush_all
  end

  defp restore_env(app, key, value) do
    if value do
      Application.put_env(app, key, value)
    else
      Application.delete_env(app, key)
    end
  end
end
