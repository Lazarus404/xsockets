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

defmodule XSockets do
  @moduledoc """
  Public facade for starting listeners and related helpers.

  ## Start here

  Prefer `serve/2` and `dial/2` for simple hosts: pick a transport, optional
  framing, and a handler. They normalize atom opts and call `listen/1` or
  `Client.connect/1`.

  Concepts:

  - **Transport** - UDP, TCP, TLS, or DTLS (how bytes move)
  - **Framing** - Accumulator that finds whole packets (`:raw`, `:length_prefixed`)
  - **Handler** - application logic per packet

  Use `listen/1` when you need full control (pipelines, SCTP, explicit modules).

  ## What problem this solves

  Hosts should not have to remember whether a transport needs `DatagramServer`,
  `Acceptor`, or `Sctp.Listener`. `listen/1` routes plain UDP to `DatagramServer`,
  TCP/TLS/DTLS to `Acceptor`, and SCTP to `Sctp.Listener` when available.
  `serve/2` / `dial/2` add atom transport and framing defaults on top.

  ## RFCs

  No STUN/TURN RFC; listener facade for host applications.
  """

  alias XSockets.{
    Accumulator,
    Acceptor,
    Client,
    DatagramServer,
    Transport
  }

  alias XSockets.Sctp.Listener

  @doc """
  Starts a listener with atom-friendly defaults.

  Sets `:handler` from the first argument. Defaults: `transport: :udp`,
  `ip: {0, 0, 0, 0}`, `port: 0`. Framing defaults to `:raw` for UDP/DTLS and
  `:length_prefixed` (2-byte header) for TCP/TLS unless `:accumulator` or
  `:pipeline` is already set.

  ## Options

    * `:transport` - `:udp` | `:tcp` | `:tls` | `:dtls`, or a `Transport.*` module
    * `:framing` - `:raw` | `:length_prefixed` | `{:length_prefixed, opts}`
    * `:ip`, `:port`, `:assigns`, `:listen_opts`, `:workers`, `:accept_workers`
    * `:accumulator` / `:pipeline` - escape hatches (skip framing default)

  Equivalent to `listen/1` after normalization. Returns
  `{:error, :invalid_transport}` or `{:error, :invalid_framing}` on bad atoms.
  """
  @spec serve(atom(), Keyword.t()) :: {:ok, pid() | [pid(), ...]} | {:error, term()}
  def serve(handler, opts \\ []) when is_atom(handler) and is_list(opts) do
    opts
    |> Keyword.put(:handler, handler)
    |> Keyword.put_new(:ip, {0, 0, 0, 0})
    |> Keyword.put_new(:port, 0)
    |> Keyword.put_new(:transport, :udp)
    |> normalize_on_ramp()
    |> case do
      {:ok, normalized} -> listen(normalized)
      {:error, _} = err -> err
    end
  end

  @doc """
  Dials out with atom-friendly defaults and starts a `Connection`.

  Sets `:handler` from the first argument. Default transport is `:tcp` with
  `:length_prefixed` framing unless `:accumulator` or `:pipeline` is set.
  Requires `:ip` and `:port`.

  UDP has no `connect/3`; `transport: :udp` yields `{:error, :connect_not_supported}`
  after normalization. TLS/DTLS need `:connect_opts` (and usually certs).

  Equivalent to `Client.connect/1` after normalization.
  """
  @spec dial(atom(), keyword()) :: {:ok, pid()} | {:error, term()}
  def dial(handler, opts \\ []) when is_atom(handler) and is_list(opts) do
    opts
    |> Keyword.put(:handler, handler)
    |> Keyword.put_new(:transport, :tcp)
    |> normalize_on_ramp()
    |> case do
      {:ok, normalized} -> Client.connect(normalized)
      {:error, _} = err -> err
    end
  end

  @doc """
  Starts a listener for `opts`.

  Required: `:transport`, `:ip`, `:port`, and either `:pipeline` or
  `:accumulator` + `:handler`.

  Plain UDP starts `DatagramServer`. With `workers: n` and `n > 1`, starts
  `n` reuseport workers via `listen_many/1` and returns `{:ok, [pid]}`.
  Connection-oriented transports (`TCP`, `TLS`, `DTLS`) start `Acceptor`
  (`:accept_workers` is forwarded).
  `Transport.SCTP` starts `Sctp.Listener` (optional; returns
  `{:error, :sctp_not_supported}` when the OS/OTP stack has no SCTP).
  """
  @spec listen(keyword()) :: {:ok, pid() | [pid()]} | {:error, term()}
  def listen(opts) when is_list(opts) do
    transport = Keyword.fetch!(opts, :transport)
    workers = Keyword.get(opts, :workers, 1)

    cond do
      transport == Transport.SCTP ->
        Listener.start_link(opts)

      transport == Transport.UDP and is_integer(workers) and workers > 1 ->
        listen_many(Keyword.put(Keyword.delete(opts, :workers), :count, workers))

      transport == Transport.UDP ->
        DatagramServer.start_link(Keyword.delete(opts, :workers))

      true ->
        Acceptor.start_link(opts)
    end
  end

  @doc """
  Starts `count` UDP `DatagramServer`s on one port with `reuseport: true`.

  The first worker binds `:port` (or `0`); remaining workers reuse that port.
  Returns `{:error, :reuseport_unsupported}` when the OS rejects reuseport.
  """
  @spec listen_many(keyword()) :: {:ok, [pid()]} | {:error, term()}
  def listen_many(opts) when is_list(opts) do
    transport = Keyword.get(opts, :transport, Transport.UDP)
    count = Keyword.get(opts, :count, 2)

    if transport != Transport.UDP or count < 1 do
      {:error, :invalid_opts}
    else
      do_listen_many(opts, count)
    end
  end

  defp do_listen_many(opts, count) do
    listen_opts =
      opts
      |> Keyword.get(:listen_opts, [])
      |> Keyword.put(:reuseport, true)

    base = Keyword.put(opts, :listen_opts, listen_opts)

    case DatagramServer.start_link(base) do
      {:ok, first} ->
        port = DatagramServer.port(first)
        rest_opts = Keyword.put(base, :port, port)

        case start_rest(rest_opts, count - 1, []) do
          {:ok, rest} ->
            {:ok, [first | rest]}

          {:error, reason} ->
            GenServer.stop(first)
            {:error, reason}
        end

      {:error, _} = err ->
        err
    end
  end

  defp start_rest(_opts, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp start_rest(opts, n, acc) do
    case DatagramServer.start_link(opts) do
      {:ok, pid} ->
        start_rest(opts, n - 1, [pid | acc])

      {:error, :eaddrinuse} ->
        {:error, :reuseport_unsupported}

      {:error, reason} ->
        Enum.each(acc, &GenServer.stop/1)
        {:error, reason}
    end
  end

  defp normalize_on_ramp(opts) do
    with {:ok, opts} <- normalize_transport(opts),
         {:ok, opts} <- normalize_framing(opts) do
      {:ok, opts}
    end
  end

  defp normalize_transport(opts) do
    case Keyword.fetch!(opts, :transport) do
      :udp ->
        {:ok, Keyword.put(opts, :transport, Transport.UDP)}

      :tcp ->
        {:ok, Keyword.put(opts, :transport, Transport.TCP)}

      :tls ->
        {:ok, Keyword.put(opts, :transport, Transport.TLS)}

      :dtls ->
        {:ok, Keyword.put(opts, :transport, Transport.DTLS)}

      mod when is_atom(mod) ->
        if transport_module?(mod) do
          {:ok, opts}
        else
          {:error, :invalid_transport}
        end
    end
  end

  defp transport_module?(mod) do
    mod in [Transport.UDP, Transport.TCP, Transport.TLS, Transport.DTLS, Transport.SCTP] or
      (Code.ensure_loaded?(mod) and function_exported?(mod, :listen, 3))
  end

  defp normalize_framing(opts) do
    cond do
      Keyword.has_key?(opts, :pipeline) or Keyword.has_key?(opts, :accumulator) ->
        {:ok, Keyword.delete(opts, :framing)}

      true ->
        transport = Keyword.fetch!(opts, :transport)
        chosen = Keyword.get(opts, :framing) || default_framing(transport)
        opts = Keyword.delete(opts, :framing)

        case framing_to_accumulator(chosen) do
          {:ok, acc} -> {:ok, Keyword.put(opts, :accumulator, acc)}
          {:error, _} = err -> err
        end
    end
  end

  defp default_framing(Transport.TCP), do: :length_prefixed
  defp default_framing(Transport.TLS), do: :length_prefixed
  defp default_framing(_transport), do: :raw

  defp framing_to_accumulator(:raw), do: {:ok, Accumulator.Raw}

  defp framing_to_accumulator(:length_prefixed),
    do: {:ok, {Accumulator.LengthPrefixed, header_size: 2}}

  defp framing_to_accumulator({:length_prefixed, acc_opts}) when is_list(acc_opts) do
    {:ok, {Accumulator.LengthPrefixed, Keyword.put_new(acc_opts, :header_size, 2)}}
  end

  defp framing_to_accumulator(_), do: {:error, :invalid_framing}
end
