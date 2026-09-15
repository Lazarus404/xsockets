# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
import Config

# XSockets configuration
config :xsockets,
  # Connection Management
  ssl_handshake_timeout: 10_000,  # 10 seconds

  # Rate Limiting (for DDoS protection). Applies to control-plane requests only;
  # see XSockets.Config.check_rate_limit/1.
  rate_limit_enabled: true,
  rate_limit_window: 60_000,  # 1 minute
  max_requests_per_window: 1000,

  # Buffer Sizes (optimized for media relay)
  buffer_size: 256 * 1024,  # 256KB default, per-connection
  listener_buffer_size: 4 * 1024 * 1024,  # shared listener sockets absorb bursts

  # Security Settings
  ssl_options: [
    versions: [:"tlsv1.2", :"tlsv1.3"],
    secure_renegotiate: true,
    reuse_sessions: false,
    honor_cipher_order: true,
    fail_if_no_peer_cert: false,
    verify: :verify_none
  ],

  # Monitoring & Telemetry
  telemetry_enabled: true

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# 3rd-party users, it should be done in your "mix.exs" file.

# You can configure your application as:
#
#     config :xsockets, key: :value
#
# and access this configuration in your application as:
#
#     Application.get_env(:xsockets, :key)
#
# You can also configure a 3rd-party app:
#
#     config :logger, level: :info
#

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env()}.exs"
