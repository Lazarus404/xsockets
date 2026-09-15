### ----------------------------------------------------------------------
###
### Copyright (c) 2026 Jahred Love and Xirsys LLC <experts@xirsys.com>
###
### All rights reserved.
###
### XSockets is licensed by Xirsys under the Apache
### License, Version 2.0. (the "License");
###
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###      http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.
###
### See LICENSE for the full license text.
###
### ----------------------------------------------------------------------

defmodule XSockets.PipelineSupport do
  @moduledoc false

  alias XSockets.{Accumulator.LengthPrefixed, Accumulator.Raw, Conn}

  defmodule TwoTierPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: {LengthPrefixed, header_size: 2},
      handler: XSockets.PipelineSupport.DescendRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler
    )
  end

  defmodule AsyncTwoTierPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: {LengthPrefixed, header_size: 2},
      handler: XSockets.PipelineSupport.DescendRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler,
      dispatch: :task
    )
  end

  defmodule PoolTwoTierPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: {LengthPrefixed, header_size: 2},
      handler: XSockets.PipelineSupport.DescendRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler,
      dispatch: :pool,
      pool_size: 1
    )
  end

  defmodule ScratchTaskSupervisor do
    @moduledoc """
    A `TierSupervisor` under its own name, so tests that stop/saturate it don't
    disturb the globally-named `XSockets.TierSupervisor.Task` that other
    tests running concurrently depend on.
    """
    alias XSockets.TierSupervisor

    def start_link(opts \\ []) do
      max_children = Keyword.get(opts, :max_children, :infinity)
      TierSupervisor.start_link(name: __MODULE__, max_children: max_children)
    end
  end

  defmodule ScratchPoolSupervisor do
    @moduledoc """
    See `ScratchTaskSupervisor` - an isolated stand-in for
    `XSockets.TierSupervisor.Pool`.
    """
    alias XSockets.TierSupervisor

    def start_link(opts \\ []) do
      max_children = Keyword.get(opts, :max_children, :infinity)
      TierSupervisor.start_link(name: __MODULE__, max_children: max_children)
    end
  end

  defmodule IsolatedTaskPipeline do
    @moduledoc """
    Like `AsyncTwoTierPipeline`, but dispatches `:inner` via
    `ScratchTaskSupervisor` instead of the shared global one, so tests can stop
    or omit that supervisor without affecting other tests.
    """
    use XSockets.Pipeline

    tier(:root,
      accumulator: {LengthPrefixed, header_size: 2},
      handler: XSockets.PipelineSupport.DescendRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler,
      dispatch: :task,
      task_supervisor: XSockets.PipelineSupport.ScratchTaskSupervisor
    )
  end

  defmodule IsolatedPoolPipeline do
    @moduledoc """
    Like `PoolTwoTierPipeline`, but dispatches `:inner` via
    `ScratchPoolSupervisor` instead of the shared global one, so tests can
    saturate it without affecting other tests.
    """
    use XSockets.Pipeline

    tier(:root,
      accumulator: {LengthPrefixed, header_size: 2},
      handler: XSockets.PipelineSupport.DescendRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler,
      dispatch: :pool,
      pool_size: 1,
      pool_supervisor: XSockets.PipelineSupport.ScratchPoolSupervisor
    )
  end

  defmodule InlineFallbackPoolPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: {LengthPrefixed, header_size: 2},
      handler: XSockets.PipelineSupport.DescendRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler,
      dispatch: :pool,
      pool_size: 1,
      on_overflow: :inline_fallback,
      pool_supervisor: XSockets.PipelineSupport.ScratchPoolSupervisor
    )
  end

  defmodule AsyncClosePipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.CloseRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.CloseInnerHandler,
      dispatch: :task
    )
  end

  defmodule InnerCloseSessionPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.CloseInnerHandler,
      dispatch: :task
    )
  end

  defmodule ThreeTierPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.RootRelayHandler
    )

    tier(:middle,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.MiddleRelayHandler
    )

    tier(:leaf,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.LeafCollectHandler
    )
  end

  defmodule DescendRootHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(<<"inner:", payload::binary>>, _meta, _conn, state) do
      {:descend, :inner, payload, state}
    end

    @impl true
    def handle_packet(packet, _meta, %Conn{assigns: %{agent: agent}}, _state) do
      Agent.update(agent, fn packets -> ["root:" <> packet | packets] end)
      {:ok, agent}
    end
  end

  defmodule InnerCollectHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(packet, _meta, %Conn{assigns: %{agent: agent}}, _state) do
      Agent.update(agent, fn packets -> [packet | packets] end)
      {:ok, agent}
    end
  end

  defmodule RootRelayHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(<<"middle:", payload::binary>>, _meta, _conn, state) do
      {:descend, :middle, payload, state}
    end

    def handle_packet(_packet, _meta, _conn, state), do: {:ok, state}
  end

  defmodule MiddleRelayHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(<<"leaf:", payload::binary>>, _meta, _conn, state) do
      {:descend, :leaf, payload, state}
    end

    def handle_packet(_packet, _meta, _conn, state), do: {:ok, state}
  end

  defmodule LeafCollectHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(packet, _meta, %Conn{assigns: %{agent: agent}}, _state) do
      Agent.update(agent, fn packets -> [packet | packets] end)
      {:ok, agent}
    end
  end

  defmodule CrashInnerHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(_packet, _meta, _conn, _state) do
      raise "inner tier crash"
    end
  end

  defmodule CloseInnerHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(_packet, _meta, _conn, state) do
      {:close, state}
    end
  end

  defmodule CrashRootHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(_packet, _meta, _conn, state) do
      {:descend, :inner, "boom", state}
    end
  end

  defmodule CloseRootHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(_packet, _meta, _conn, state) do
      {:descend, :inner, "close-me", state}
    end
  end

  defmodule CrashPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.CrashRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.CrashInnerHandler
    )
  end

  defmodule ClosePipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.CloseRootHandler
    )

    tier(:inner,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.CloseInnerHandler
    )
  end

  defmodule RootOnlyPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.InnerCollectHandler
    )
  end

  defmodule PoisonAccumulator do
    @moduledoc false
    @behaviour XSockets.Accumulator

    alias XSockets.Accumulator.Raw

    @poison "poison"

    @impl true
    def init(opts), do: %{inner: Raw.init(opts)}

    @impl true
    def push(%{inner: inner} = acc, chunk, meta) do
      %{acc | inner: Raw.push(inner, chunk, meta)}
    end

    @impl true
    def pop(%{inner: inner} = acc) do
      case Raw.pop(inner) do
        {:ok, @poison, _meta, _inner} ->
          raise "poison pop"

        {:ok, packet, meta, inner} ->
          {:ok, packet, meta, %{acc | inner: inner}}

        {:more, inner} ->
          {:more, %{acc | inner: inner}}

        {:error, reason, inner} ->
          {:error, reason, %{acc | inner: inner}}
      end
    end
  end

  defmodule PoisonRootHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet("poison", _meta, _conn, state) do
      {:descend, :inner, "poison", state}
    end

    @impl true
    def handle_packet("good", _meta, _conn, state) do
      {:descend, :inner, "good", state}
    end

    def handle_packet(_packet, _meta, _conn, state), do: {:ok, state}
  end

  defmodule PoisonPipeline do
    @moduledoc false
    use XSockets.Pipeline

    tier(:root,
      accumulator: Raw,
      handler: XSockets.PipelineSupport.PoisonRootHandler
    )

    tier(:inner,
      accumulator: XSockets.PipelineSupport.PoisonAccumulator,
      handler: XSockets.PipelineSupport.InnerCollectHandler
    )
  end

  defmodule SeqRootHandler do
    @behaviour XSockets.Handler

    @impl true
    def handle_packet(<<seq::8>>, _meta, _conn, state) do
      {:descend, :inner, <<seq>>, state}
    end

    def handle_packet(_packet, _meta, _conn, state), do: {:ok, state}
  end

  defmodule UdpReorderPipeline do
    @moduledoc false
    use XSockets.Pipeline

    alias XSockets.Accumulator.{Raw, Reorder}

    tier(:root,
      accumulator: {XSockets.Accumulator.LengthPrefixed, header_size: 2},
      handler: XSockets.PipelineSupport.SeqRootHandler
    )

    tier(:inner,
      accumulator: {
        Reorder,
        inner: Raw, key_fun: fn packet, _meta -> :binary.at(packet, 0) end
      },
      handler: XSockets.PipelineSupport.InnerCollectHandler
    )
  end
end
