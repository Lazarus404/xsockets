defmodule XSockets.RateLimit.WindowTest do
  use ExUnit.Case, async: false

  alias XSockets.RateLimit.{Table, Window}

  setup do
    previous_max = Application.get_env(:xsockets, :max_requests_per_window)
    previous_enabled = Application.get_env(:xsockets, :rate_limit_enabled)

    on_exit(fn ->
      restore(:max_requests_per_window, previous_max)
      restore(:rate_limit_enabled, previous_enabled)
    end)

    :ok
  end

  test "bump rate-limits after max_requests_per_window" do
    Application.put_env(:xsockets, :rate_limit_enabled, true)
    Application.put_env(:xsockets, :max_requests_per_window, 1)

    key = {:window_test, System.unique_integer([:positive])}
    assert :ok = Window.bump(key)
    assert {:error, :rate_limited} = Window.bump(key)
  end

  test "bump is a no-op when rate limiting disabled" do
    Application.put_env(:xsockets, :rate_limit_enabled, false)
    Application.put_env(:xsockets, :max_requests_per_window, 1)

    key = {:window_disabled, System.unique_integer([:positive])}
    assert :ok = Window.bump(key)
    assert :ok = Window.bump(key)
  end

  test "concurrent bumps never store a count above max_requests" do
    Application.put_env(:xsockets, :rate_limit_enabled, true)
    max = 20
    Application.put_env(:xsockets, :max_requests_per_window, max)

    key = {:window_concurrent, System.unique_integer([:positive])}
    tasks = 200

    results =
      1..tasks
      |> Task.async_stream(fn _ -> Window.bump(key) end, max_concurrency: 50, ordered: false)
      |> Enum.map(fn {:ok, result} -> result end)

    ok_count = Enum.count(results, &(&1 == :ok))
    limited_count = Enum.count(results, &(&1 == {:error, :rate_limited}))

    assert ok_count == max
    assert limited_count == tasks - max

    table = Table.ensure!()
    assert [{^key, _window, count}] = :ets.lookup(table, key)
    assert count == max
  end

  defp restore(key, nil), do: Application.delete_env(:xsockets, key)
  defp restore(key, value), do: Application.put_env(:xsockets, key, value)
end
