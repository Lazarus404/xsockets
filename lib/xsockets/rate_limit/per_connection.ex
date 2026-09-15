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

defmodule XSockets.RateLimit.PerConnection do
  @moduledoc """
  Fixed-window rate limiter keyed by `{client_ip, client_port}` for all framings.

  ## What problem this solves

  Opt-in replacement for the default `FixedWindow` (datagram-only). Hosts that
  want Engine gating on TCP/TLS (and UDP) set
  `config :xsockets, rate_limiter: XSockets.RateLimit.PerConnection`. Uses the
  same window/max knobs and ETS table via `XSockets.RateLimit.Window`.

  ## RFCs

  No transport RFC; operational control-plane protection.
  """

  @behaviour XSockets.RateLimit

  alias XSockets.Conn

  @doc false
  @impl true
  def check(%Conn{client_ip: ip, client_port: port}, _transport, _meta)
      when not is_nil(ip) and not is_nil(port) do
    XSockets.RateLimit.Window.bump({ip, port})
  end

  def check(%Conn{client_ip: ip}, _transport, _meta) when not is_nil(ip) do
    XSockets.RateLimit.Window.bump(ip)
  end

  def check(_, _, _), do: :ok
end
