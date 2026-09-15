defmodule XSockets.Sctp.AssociationTest do
  use ExUnit.Case, async: false

  alias XSockets.Sctp.Association

  setup do
    if Association.available?() do
      :ok
    else
      {:skip, "ex_sctp NIF not loaded (need Rust + mix deps.compile)"}
    end
  end

  test "active and passive associations exchange a data channel message" do
    active = Association.new(role: :active)
    passive = Association.new(role: :passive)

    {events, active} = Association.connect(active)
    {active, passive, _} = pipe(active, passive, events)
    {active, passive, _} = tick_until(active, passive, fn a, p ->
      a.state == :connected and p.state == :connected
    end)

    assert active.state == :connected
    assert passive.state == :connected

    {events, local_ref, active} = Association.open_channel(active, "spike")
    {active, passive, remote_events} = pipe(active, passive, events)

    {active, passive, remote_events2} =
      tick_until(active, passive, fn a, p ->
        local_open?(a) and remote_open?(p)
      end)

    remote_ref =
      (remote_events ++ remote_events2)
      |> Enum.find_value(fn
        {:channel_open, ref, "spike", _id, _meta} -> ref
        _ -> nil
      end)

    remote_ref =
      remote_ref ||
        Enum.find_value(passive.channels, fn {ref, ch} ->
          if ch.ready_state == :open and ch.label == "spike", do: ref
        end)

    assert is_reference(remote_ref)
    assert local_open?(active)

    {events, active} = Association.send(active, local_ref, "hello")
    {active, passive, evs} = pipe(active, passive, events)

    {_, _, msg} =
      tick_until(active, passive, fn _a, _p -> false end, evs, fn events ->
        Enum.find(events, &match?({:message, ^remote_ref, "hello"}, &1))
      end)

    assert {:message, ^remote_ref, "hello"} = msg

    {events, passive} = Association.send(passive, remote_ref, "echo:hello")
    {_passive, _active, evs} = pipe(passive, active, events)

    assert Enum.any?(evs, &match?({:message, ^local_ref, "echo:hello"}, &1)) or
             match?(
               {_, _, {:message, ^local_ref, "echo:hello"}},
               tick_until(passive, active, fn _a, _p -> false end, evs, fn events ->
                 Enum.find(events, &match?({:message, ^local_ref, "echo:hello"}, &1))
               end)
             )
  end

  test "channel_open event includes DCEP reliability meta for unordered rexmit" do
    active = Association.new(role: :active)
    passive = Association.new(role: :passive)

    {events, active} = Association.connect(active)
    {active, passive, _} = pipe(active, passive, events)

    {active, passive, _} =
      tick_until(active, passive, fn a, p ->
        a.state == :connected and p.state == :connected
      end)

    {events, _local_ref, active} =
      Association.open_channel(active, "state",
        ordered: false,
        reliability: :rexmit,
        param: 0
      )

    {active, passive, remote_events} = pipe(active, passive, events)

    {_active, _passive, remote_events2} =
      tick_until(active, passive, fn a, p ->
        local_open?(a) and remote_open?(p)
      end)

    meta =
      (remote_events ++ remote_events2)
      |> Enum.find_value(fn
        {:channel_open, _ref, "state", _id, meta} -> meta
        _ -> nil
      end)

    assert meta == %{reliability: :rexmit, order: :unordered, param: 0}
  end

  defp local_open?(assoc),
    do: Enum.any?(assoc.channels, fn {_r, ch} -> ch.ready_state == :open end)

  defp remote_open?(assoc),
    do: Enum.any?(assoc.channels, fn {_r, ch} -> ch.ready_state == :open end)

  defp pipe(from, to, events) do
    Enum.reduce(events, {from, to, []}, fn
      {:transmit, packets}, {a, b, acc} ->
        Enum.reduce(packets, {a, b, acc}, fn pkt, {aa, bb, acc2} ->
          {evs, bb2} = Association.handle_packet(bb, pkt)
          {bb3, aa2, nested} = pipe(bb2, aa, evs)
          {aa2, bb3, acc2 ++ evs ++ nested}
        end)

      {:timeout, ms}, {a, b, acc} when is_integer(ms) and ms > 0 ->
        Process.sleep(min(ms, 3))
        {evs, a2} = Association.handle_timeout(a)
        {a3, b2, nested} = pipe(a2, b, evs)
        {a3, b2, acc ++ nested}

      ev, {a, b, acc} ->
        {a, b, acc ++ [ev]}
    end)
  end

  defp tick_until(a, b, done?, seed_events \\ [], finder \\ fn _ -> nil end) do
    Enum.reduce_while(1..80, {a, b, seed_events, nil}, fn _, {aa, bb, acc, found} ->
      cond do
        found != nil ->
          {:halt, {aa, bb, found}}

        done?.(aa, bb) ->
          {:halt, {aa, bb, acc}}

        true ->
          {evs, aa} = Association.handle_timeout(aa)
          {aa, bb, nested} = pipe(aa, bb, evs)
          acc = acc ++ nested
          found = finder.(acc)

          {evs, bb} = Association.handle_timeout(bb)
          {bb, aa, nested} = pipe(bb, aa, evs)
          acc = acc ++ nested
          found = found || finder.(acc)

          Process.sleep(2)
          {:cont, {aa, bb, acc, found}}
      end
    end)
    |> case do
      {a, b, found} when not is_list(found) -> {a, b, found}
      {a, b, acc} -> {a, b, acc}
    end
  end
end
