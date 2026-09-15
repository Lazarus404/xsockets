defmodule XSockets.PipelineTest do
  use ExUnit.Case, async: true

  alias XSockets.{Accumulator.Raw, Pipeline, Spec}
  alias XSockets.PipelineSupport.{RootOnlyPipeline, TwoTierPipeline}

  test "resolve/1 from legacy accumulator and handler sugar" do
    pipeline = Pipeline.resolve({Raw, XSockets.TestSupport.CollectHandler})
    assert Map.has_key?(pipeline.tiers, :root)
    assert Pipeline.tier_spec(pipeline, :root).accumulator == Raw
  end

  test "resolve/1 from compiled pipeline module" do
    pipeline = Pipeline.resolve(TwoTierPipeline)
    assert Map.keys(pipeline.tiers) == [:root, :inner]

    assert Pipeline.tier_spec(pipeline, :root).accumulator ==
             XSockets.Accumulator.LengthPrefixed

    assert Pipeline.tier_spec(pipeline, :inner).accumulator == Raw
    assert Pipeline.tier_spec(pipeline, :inner).dispatch == :inline
  end

  test "compiled pipeline exposes tiers in declaration order" do
    assert TwoTierPipeline.__tiers__() == [:root, :inner]
  end

  test "init_session seeds root accumulator and handler state" do
    pipeline = Pipeline.resolve(RootOnlyPipeline)
    conn = %XSockets.Conn{assigns: %{}}

    {accs, states} = Pipeline.init_session(pipeline, conn, handler_state: :seed)

    assert map_size(accs) == 1
    assert map_size(states) == 1
    assert Map.has_key?(accs, :root)
    assert states.root == :seed
  end

  test "Spec.resolve still works for single-tier modules" do
    assert {Raw, []} = Spec.resolve(Raw)
  end

  test "compile error when :root tier is missing" do
    source = """
    defmodule XSockets.MissingRootPipeline do
      use XSockets.Pipeline

      tier :inner,
        accumulator: XSockets.Accumulator.Raw,
        handler: XSockets.TestSupport.CollectHandler
    end
    """

    assert_raise CompileError, ~r/:root tier/, fn ->
      Code.compile_string(source)
    end
  end

  test "compile error on duplicate tier names" do
    source = """
    defmodule XSockets.DuplicateTierPipeline do
      use XSockets.Pipeline

      tier :root,
        accumulator: XSockets.Accumulator.Raw,
        handler: XSockets.TestSupport.CollectHandler

      tier :root,
        accumulator: XSockets.Accumulator.Raw,
        handler: XSockets.TestSupport.CollectHandler
    end
    """

    assert_raise CompileError, ~r/duplicate pipeline tier/, fn ->
      Code.compile_string(source)
    end
  end
end
