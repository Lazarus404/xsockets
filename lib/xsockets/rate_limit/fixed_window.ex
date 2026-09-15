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

defmodule XSockets.RateLimit.FixedWindow do
  @moduledoc """
  Default Engine rate limiter: fixed window per client IP for datagram framing.

  ## What problem this solves

  Preserves the 1.0 Engine policy: only transports with `framing() == :datagram`
  are gated. Stream transports always pass. For TCP/TLS gating set
  `config :xsockets, rate_limiter: XSockets.RateLimit.PerConnection`. Uses the
  shared `:xsockets_rate_limits` ETS table via `RateLimit.Window`.

  ## RFCs

  No transport RFC; operational control-plane protection.
  """

  @behaviour XSockets.RateLimit

  alias XSockets.{Config, Conn}

  @doc false
  @impl true
  def check(%Conn{client_ip: ip}, transport, _meta)
      when not is_nil(ip) and is_atom(transport) do
    if datagram?(transport) do
      Config.check_rate_limit(ip)
    else
      :ok
    end
  end

  def check(_, _, _), do: :ok

  defp datagram?(transport) do
    function_exported?(transport, :framing, 0) and transport.framing() == :datagram
  end
end
