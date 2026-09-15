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

defmodule XSockets.Transport.TLS do
  @moduledoc """
  TLS stream transport (`:ssl`) for secure listeners and connections.

  ## What problem this solves

  Hosts that terminate TLS need the same listen/accept/send surface as TCP.
  This module wraps OTP `:ssl` behind the `Transport` behaviour: secure listen
  defaults, handshake on accept, and normalized `{:ssl, ...}` messages for the
  drain loop.

  Defaults to TLS 1.2/1.3 and AEAD cipher suites. Legacy versions (`:tlsv1`,
  `:"tlsv1.1"`, `:sslv3`) are stripped from `:versions`. Certificates come from
  `listen/3` opts (`certfile`, `keyfile`, `cacertfile`), then
  `Config.get(:certs)` (host `:config_app` or `:xsockets`).

  ## RFCs

  - [RFC 8446](https://www.rfc-editor.org/rfc/rfc8446) - TLS 1.3 (handshake and record layer)
  - [RFC 8656](https://www.rfc-editor.org/rfc/rfc8656) - TURN (TLS as a common host transport)
  """
  @behaviour XSockets.Transport

  alias XSockets.{Config, Telemetry}

  @listen_opts [
    reuseaddr: true,
    active: false
  ]

  @tcp_listen_opts [
    keepalive: true,
    backlog: 100,
    nodelay: true
  ]

  @secure_defaults [
    versions: [:"tlsv1.2", :"tlsv1.3"],
    ciphers: :aead_only,
    secure_renegotiate: true,
    reuse_sessions: false,
    honor_cipher_order: true,
    fail_if_no_peer_cert: false,
    verify: :verify_none,
    depth: 2
  ]

  @dtls_versions [:"dtlsv1.2"]

  @rejected_versions [:tlsv1, :"tlsv1.1", :sslv3]

  @doc """
  Normalized listen/security options for a named preset.

  Presets:

  * `:server` / `:default` - TLS 1.2/1.3, AEAD, `verify: :verify_none`
  * `:mutual_tls` - same cipher baseline with `verify: :verify_peer` and
    `fail_if_no_peer_cert: true`

      iex> opts = XSockets.Transport.TLS.security_opts(:server)
      iex> Keyword.get(opts, :verify)
      :verify_none
      iex> mtls = XSockets.Transport.TLS.security_opts(:mutual_tls)
      iex> Keyword.get(mtls, :verify)
      :verify_peer
  """
  @spec security_opts(:server | :default | :mutual_tls) :: keyword()
  def security_opts(preset \\ :server)

  def security_opts(:default), do: security_opts(:server)

  def security_opts(:server) do
    @secure_defaults
    |> normalize_ssl_options()
    |> resolve_cipher_opt()
  end

  def security_opts(:mutual_tls) do
    :server
    |> security_opts()
    |> Keyword.merge(verify: :verify_peer, fail_if_no_peer_cert: true)
  end

  def security_opts(other) when is_atom(other) do
    raise ArgumentError, "unknown TLS security preset: #{inspect(other)}"
  end

  @doc false
  @impl true
  def listen(ip, port, opts) do
    with {:ok, cert_opts} <- resolve_cert_opts(opts),
         listen_opts <- build_listen_opts(ip, Keyword.merge(cert_opts, opts)) do
      case :ssl.listen(port, listen_opts) do
        {:ok, _sock} = ok ->
          Telemetry.emit(:tls_listener_created, %{}, %{ip: ip, port: port})
          ok

        {:error, _} = error ->
          error
      end
    end
  end

  @doc false
  @impl true
  def accept(listen_sock, timeout), do: accept(listen_sock, timeout, :tls)

  @doc false
  def accept(listen_sock, timeout, protocol) when protocol in [:tls, :dtls] do
    ssl_timeout = Config.ssl_handshake_timeout()
    accept_timeout = if timeout == :infinity, do: ssl_timeout, else: timeout

    with {:ok, transport} <- :ssl.transport_accept(listen_sock, accept_timeout),
         {:ok, cli} <- :ssl.handshake(transport, ssl_timeout) do
      :ok = :ssl.setopts(cli, [{:active, false}])
      Telemetry.emit(:ssl_handshake_success, %{protocol: protocol}, %{})
      {:ok, cli}
    else
      {:error, :timeout} = error ->
        Telemetry.emit(:ssl_handshake_timeout, %{protocol: protocol}, %{})
        error

      {:error, reason} = error ->
        Telemetry.emit(:ssl_handshake_error, %{protocol: protocol, error: reason}, %{})
        error
    end
  end

  @doc false
  @impl true
  def send(socket, data, _to), do: :ssl.send(socket, data)

  @doc false
  @impl true
  def setopts(socket, opts), do: :ssl.setopts(socket, opts)

  @doc false
  @impl true
  def sockname(socket), do: :ssl.sockname(socket)

  @doc false
  @impl true
  def peername(socket), do: :ssl.peername(socket)

  @doc false
  @impl true
  def close(socket) do
    :ssl.close(socket)
    Telemetry.emit(:socket_closed, %{protocol: :tls}, %{})
    :ok
  end

  @doc false
  @impl true
  def controlling_process(socket, pid), do: :ssl.controlling_process(socket, pid)

  @doc """
  Outbound TLS client connect (optional `Transport` callback).

  Merges `security_opts(:server)` with `opts`, then calls `:ssl.connect/3`.
  """
  @impl true
  def connect(ip, port, opts \\ []) do
    protocol = Keyword.get(opts, :protocol, :tls)

    connect_opts =
      security_opts(:server)
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(normalize_ssl_options(opts))
      |> resolve_cipher_opt()
      |> apply_protocol_versions(protocol)
      |> Keyword.drop([:honor_cipher_order, :fail_if_no_peer_cert])
      |> Keyword.put(:active, false)
      |> Keyword.put(:protocol, protocol)

    :ssl.connect(ip, port, connect_opts, Config.ssl_handshake_timeout())
  end

  @doc false
  @impl true
  def framing(), do: :stream

  @doc """
  Maps `:ssl` / `:ssl_closed` / `:ssl_error` / `:ssl_passive` messages.

      iex> XSockets.Transport.TLS.handle_message({:ssl, :port, "hi"}, :sock)
      {:data, "hi", nil}
      iex> XSockets.Transport.TLS.handle_message({:ssl_closed, :port}, :sock)
      {:closed, :normal}
      iex> XSockets.Transport.TLS.framing()
      :stream
  """
  @impl true
  def handle_message({:ssl, _port, data}, _socket), do: {:data, data, nil}
  def handle_message({:ssl_closed, _}, _socket), do: {:closed, :normal}
  def handle_message({:ssl_error, _, reason}, _socket), do: {:closed, reason}

  def handle_message({:ssl_passive, _}, socket) do
    :ssl.setopts(socket, active: :once)
    :ignore
  end

  def handle_message(_, _socket), do: :ignore

  defp build_listen_opts(ip, opts) do
    opts = normalize_ssl_options(opts)
    protocol = Keyword.get(opts, :protocol, :tls)

    secure =
      Config.get(:ssl_options, @secure_defaults)
      |> normalize_ssl_options()
      |> resolve_cipher_opt()
      |> apply_protocol_versions(protocol)

    @listen_opts
    |> maybe_merge_tcp_opts(protocol)
    |> Keyword.merge(buffer_opts())
    |> Keyword.merge(secure)
    |> Keyword.merge(opts)
    |> with_ip_family(ip)
  end

  defp with_ip_family(opts, ip) when tuple_size(ip) == 8 do
    # OTP wants the bare atom `:inet6`, not `{:inet6, true}` (that raises :badarg).
    opts =
      opts
      |> Keyword.put(:ip, ip)
      |> maybe_ipv6_v6only()

    [:inet6 | opts]
  end

  defp with_ip_family(opts, ip), do: Keyword.put(opts, :ip, ip)

  defp maybe_ipv6_v6only(opts) do
    case :os.type() do
      {:unix, :linux} -> Keyword.put(opts, :ipv6_v6only, true)
      _ -> opts
    end
  end

  defp maybe_merge_tcp_opts(opts, :dtls), do: opts
  defp maybe_merge_tcp_opts(opts, _protocol), do: Keyword.merge(opts, @tcp_listen_opts)

  defp apply_protocol_versions(secure, :dtls) do
    Keyword.put(secure, :versions, @dtls_versions)
  end

  defp apply_protocol_versions(secure, _protocol), do: secure

  defp normalize_ssl_options(opts) when is_list(opts) do
    opts
    |> Enum.map(&normalize_ssl_opt/1)
    |> Keyword.new()
  end

  defp normalize_ssl_opt({key, value}) when is_atom(key) do
    {key, normalize_ssl_value(key, value)}
  end

  defp normalize_ssl_opt({key, value}) when is_list(key) do
    {List.to_atom(key), normalize_ssl_value(List.to_atom(key), value)}
  end

  defp normalize_ssl_value(:versions, versions) when is_list(versions) do
    versions
    |> Enum.map(&normalize_tls_version/1)
    |> Enum.reject(&(&1 in @rejected_versions))
  end

  defp normalize_ssl_value(:ciphers, :aead_only), do: aead_ciphers()

  defp normalize_ssl_value(_key, value), do: value

  defp resolve_cipher_opt(opts) do
    case Keyword.get(opts, :ciphers) do
      :aead_only -> Keyword.put(opts, :ciphers, aead_ciphers())
      ciphers when is_list(ciphers) -> opts
      _ -> Keyword.put(opts, :ciphers, aead_ciphers())
    end
  end

  defp aead_ciphers do
    # TLS 1.3 exclusive suites do not overlap :default for tlsv1.2. Advertising
    # 1.3 in :versions without those suites makes OTP fail the handshake with
    # no_suitable_cipher instead of negotiating 1.2 (coturn uclient -t -S).
    (:ssl.cipher_suites(:exclusive, :"tlsv1.3") ++ :ssl.cipher_suites(:default, :"tlsv1.2"))
    |> Enum.uniq()
    |> Enum.reject(&insecure_cipher?/1)
  end

  defp insecure_cipher?(suite) do
    name =
      suite
      |> cipher_suite_name()
      |> Atom.to_string()
      |> String.downcase()

    String.contains?(name, "des") or String.contains?(name, "rc4") or
      String.contains?(name, "null") or String.contains?(name, "cbc")
  end

  defp cipher_suite_name(%{cipher: cipher}), do: cipher
  defp cipher_suite_name({name, _, _, _}), do: name
  defp cipher_suite_name({name, _, _}), do: name
  defp cipher_suite_name(name) when is_atom(name), do: name

  defp normalize_tls_version(version) when is_atom(version), do: version

  defp normalize_tls_version(version) when is_list(version) do
    version |> List.to_atom()
  end

  defp normalize_tls_version(version) when is_binary(version) do
    String.to_atom(version)
  end

  defp buffer_opts do
    size = Config.buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end

  defp resolve_cert_opts(opts) do
    if cert_keys_present?(opts) do
      {:ok, []}
    else
      cert_options_from_config()
    end
  end

  defp cert_keys_present?(opts) do
    Enum.any?([:certfile, :keyfile, :cacertfile], &Keyword.has_key?(opts, &1))
  end

  defp cert_options_from_config do
    case Config.get(:certs) do
      list when is_list(list) -> {:ok, normalize_ssl_options(list)}
      _ -> {:error, :no_certificates_configured}
    end
  end
end
