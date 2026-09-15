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

defmodule XSockets.RateLimit do
  @moduledoc """
  Behaviour for Engine inbound rate limiting.

  ## What problem this solves

  Hosts need different budgets for datagram control traffic versus stream
  connections. The Engine calls the configured limiter module before framing
  when `Config.engine_rate_limit?/0` is true. Swap modules via
  `config :xsockets, rate_limiter: MyLimiter`.

  ## RFCs

  No transport RFC; operational control-plane protection.
  """

  alias XSockets.Conn

  @doc """
  Returns `:ok` to accept the chunk or `{:error, :rate_limited}` to drop it.
  """
  @callback check(Conn.t(), transport :: module(), meta :: map()) ::
              :ok | {:error, :rate_limited}
end
