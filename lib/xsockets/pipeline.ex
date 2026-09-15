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

defmodule XSockets.Pipeline do
  @moduledoc """
  Declarative multi-tier pipeline specification.

      use XSockets.Pipeline

      tier :root,
        accumulator: {XSockets.Accumulator.LengthPrefixed, header_size: 2},
        handler: MyApp.Handlers.Root

      tier :inner,
        accumulator: XSockets.Accumulator.Raw,
        handler: MyApp.Handlers.Inner,
        dispatch: :pool,
        pool_size: 8

  Every pipeline must declare `:root`. Duplicate tier names fail at compile time.

  `Acceptor` / `DatagramServer` also accept the legacy `{accumulator, handler}`
  pair, which `resolve/1` turns into a single-tier pipeline.

  ## What problem this solves

  Host listeners peel protocol layers
  (for example STUN length framing, then TURN channel data) before handing work
  to handlers. A compile-time `tier/2` macro records each layer's accumulator,
  handler, and dispatch mode so acceptors resolve a single runtime `%Pipeline{}`.

  ## RFCs

  No STUN/TURN RFC; framing/dispatch infrastructure for host listeners.
  Host handlers implement [RFC 8489](https://www.rfc-editor.org/rfc/rfc8489) /
  [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) parsing above this layer.
  """

  alias XSockets.{Config, Conn, Pipeline.Tier, Spec}

  defstruct tiers: nil

  @typedoc """
  Compiled pipeline resolved from a module or legacy pair.

  ## Fields

  * `:tiers` - map of tier name atom to `%XSockets.Pipeline.Tier{}`
  """
  @type t :: %__MODULE__{tiers: %{atom() => Tier.t()}}

  @doc """
  Imports `tier/2` and compiles `__tiers__/0` / `__tier_spec__/1`.

  ## Parameters

    * `_opts` - unused `use` options (accepted for API stability)
  """
  defmacro __using__(_opts) do
    quote do
      @pipeline_tiers []
      import XSockets.Pipeline, only: [tier: 2]
      @before_compile XSockets.Pipeline
    end
  end

  @doc """
  Declares one named tier on the using module.

  ## Parameters

    * `name` - atom key (`:root` is required on every pipeline)
    * `opts` - `:accumulator` and `:handler` (required); optional `:dispatch`
      (`:inline` | `:task` | `:pool`), `:pool_size`, `:task_supervisor`,
      `:pool_supervisor`
  """
  defmacro tier(name, opts) do
    quote do
      @pipeline_tiers @pipeline_tiers ++ [unquote(name)]

      @doc """
      Returns the compiled `%XSockets.Pipeline.Tier{}` for `#{unquote(name)}`.
      """
      def __tier_spec__(unquote(name)) do
        tier_opts = unquote(opts)
        accumulator = Keyword.fetch!(tier_opts, :accumulator)
        handler = Keyword.fetch!(tier_opts, :handler)
        {accumulator_mod, accumulator_opts} = Spec.resolve(accumulator)

        %Tier{
          accumulator: accumulator_mod,
          accumulator_opts: accumulator_opts,
          handler: handler,
          dispatch: Keyword.get(tier_opts, :dispatch, :inline),
          pool_size: Keyword.get(tier_opts, :pool_size, nil),
          on_overflow: Keyword.get(tier_opts, :on_overflow, XSockets.Config.pool_on_overflow()),
          task_supervisor:
            Keyword.get(tier_opts, :task_supervisor, XSockets.TierSupervisor.Task),
          pool_supervisor:
            Keyword.get(tier_opts, :pool_supervisor, XSockets.TierSupervisor.Pool)
        }
      end
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    tiers = Module.get_attribute(env.module, :pipeline_tiers)

    unless :root in tiers do
      raise CompileError,
        description: "pipeline must declare a :root tier",
        file: env.file,
        line: env.line
    end

    duplicates = tiers -- Enum.uniq(tiers)

    unless duplicates == [] do
      raise CompileError,
        description: "duplicate pipeline tier #{inspect(hd(duplicates))}",
        file: env.file,
        line: env.line
    end

    quote do
      @doc "Returns the ordered list of tier names declared on this pipeline module."
      def __tiers__, do: unquote(tiers)
    end
  end

  @doc """
  Resolves a compiled pipeline module or `{accumulator, handler}` sugar
  into a runtime `%Pipeline{}`.

  ## Parameters

    * `pipeline` - a module that `use`s this one, or `{accumulator_spec, handler_mod}`

      iex> p = XSockets.Pipeline.resolve({XSockets.Accumulator.Raw, :handler})
      iex> XSockets.Pipeline.multi_tier?(p)
      false
      iex> spec = XSockets.Pipeline.tier_spec(p, :root)
      iex> spec.accumulator
      XSockets.Accumulator.Raw
      iex> spec.handler
      :handler
      iex> spec.dispatch
      :inline
  """
  @spec resolve(module() | {Spec.spec(), module()}) :: t()
  def resolve(pipeline_mod) when is_atom(pipeline_mod) do
    tiers =
      pipeline_mod.__tiers__()
      |> Map.new(fn name ->
        tier = apply(pipeline_mod, :__tier_spec__, [name])
        {name, normalize_tier(tier)}
      end)

    %__MODULE__{tiers: tiers}
  end

  def resolve({accumulator_spec, handler_mod}) when is_atom(handler_mod) do
    {accumulator_mod, accumulator_opts} = Spec.resolve(accumulator_spec)

    %__MODULE__{
      tiers: %{
        root: %Tier{
          accumulator: accumulator_mod,
          accumulator_opts: accumulator_opts,
          handler: handler_mod,
          dispatch: :inline,
          pool_size: nil,
          on_overflow: :drop
        }
      }
    }
  end

  @doc """
  Returns the `%Pipeline.Tier{}` for `key`.

  Raises if `key` was not declared.

  ## Parameters

    * `pipeline` - struct from `resolve/1`
    * `key` - tier name (`:root`, ...)
  """
  @spec tier_spec(t(), atom()) :: Tier.t()
  def tier_spec(%__MODULE__{tiers: tiers}, key) do
    Map.fetch!(tiers, key)
  end

  @doc """
  Returns `true` when the pipeline declares more than the mandatory `:root` tier.

  ## Parameters

    * `pipeline` - struct from `resolve/1`
  """
  @spec multi_tier?(t()) :: boolean()
  def multi_tier?(%__MODULE__{tiers: tiers}), do: map_size(tiers) > 1

  @doc """
  Initializes root-tier accumulator and handler state for a stream connection.

  Calls `handle_connect/1` when the root handler exports it.

  ## Parameters

    * `pipeline` - struct from `resolve/1`
    * `conn` - new connection context
    * `opts` - `:handler_state` used when `handle_connect/1` is absent
  """
  @spec init_session(t(), Conn.t(), keyword()) :: {map(), map()}
  def init_session(%__MODULE__{} = pipeline, %Conn{} = conn, opts) do
    %Tier{accumulator: root_acc_mod, accumulator_opts: root_acc_opts, handler: root_handler} =
      tier_spec(pipeline, :root)

    default_handler_state = Keyword.get(opts, :handler_state, nil)

    root_state =
      if function_exported?(root_handler, :handle_connect, 1) do
        case root_handler.handle_connect(conn) do
          {:ok, state} -> state
          _ -> default_handler_state
        end
      else
        default_handler_state
      end

    accs = %{root: root_acc_mod.init(root_acc_opts)}
    states = %{root: root_state}
    {accs, states}
  end

  @doc """
  Builds fresh root-tier session maps for a single datagram (stateless UDP).

  ## Parameters

    * `pipeline` - struct from `resolve/1`
    * `opts` - `:handler_state` stored under `:root`

      iex> p = XSockets.Pipeline.resolve({XSockets.Accumulator.Raw, :handler})
      iex> {accs, states} = XSockets.Pipeline.fresh_session(p, handler_state: :idle)
      iex> Map.keys(accs)
      [:root]
      iex> states
      %{root: :idle}
  """
  @spec fresh_session(t(), keyword()) :: {map(), map()}
  def fresh_session(%__MODULE__{} = pipeline, opts) do
    %Tier{accumulator: root_acc_mod, accumulator_opts: root_acc_opts} =
      tier_spec(pipeline, :root)

    handler_state = Keyword.get(opts, :handler_state, nil)

    {%{root: root_acc_mod.init(root_acc_opts)}, %{root: handler_state}}
  end

  @spec normalize_tier(Tier.t()) :: Tier.t()
  defp normalize_tier(%Tier{dispatch: :pool, pool_size: nil} = tier) do
    %{tier | pool_size: Config.tier_pool_size()}
  end

  defp normalize_tier(%Tier{} = tier), do: tier
end
