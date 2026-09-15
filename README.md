# XSockets

Ported from (and deprecates) [xturn-sockets](https://github.com/Lazarus404/xturn-sockets)

An Elixir library for listening, dialing, and reading whole messages on UDP, TCP,
TLS, DTLS, and (optionally) SCTP -- without baking in STUN, TURN, RTP, or SIP.
Those protocols live in *your* app. XSockets just handles the pipes and the
"when is a full packet ready?" part.

Home: [https://github.com/Lazarus404/xsockets](https://github.com/Lazarus404/xsockets)

## What problem this solves

If you have built a real-time server, you have probably written the same GenServer
loop more than once: bind a socket, read, figure out where one message ends and
the next begins, call your logic, write a reply, repeat.

XSockets turns that into three plugs:

1. **Transport** -- how bytes move (`:udp`, `:tcp`, `:tls`, `:dtls`)
2. **Framing** -- how you spot a whole message (`:raw`, `:length_prefixed`)
3. **Handler** -- what *you* do with each message

A shared **Engine** drains every complete packet from each read before it asks
the socket for more. That keeps control traffic (handshakes, requests) tidy.
High-rate media can skip the engine and use `Transport.UDP.open_relay/2` instead.

## Start here

### 1. Write a handler

```elixir
defmodule MyApp.Handler do
  @behaviour XSockets.Handler

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, conn, state) do
    IO.inspect({packet, conn.client_ip, conn.client_port})
    {:ok, state}
  end
end
```

### 2. Serve (or dial)

```elixir
# UDP -- each datagram is already a whole message (:raw)
{:ok, pid} =
  XSockets.serve(MyApp.Handler,
    transport: :udp,
    ip: {0, 0, 0, 0},
    port: 3478
  )

# TCP -- messages are length-prefixed (2-byte size header by default)
{:ok, pid} =
  XSockets.serve(MyApp.Handler,
    transport: :tcp,
    ip: {0, 0, 0, 0},
    port: 3478
  )

# Call out as a client (needs :ip and :port)
{:ok, conn} =
  XSockets.dial(MyApp.Handler,
    transport: :tcp,
    ip: {127, 0, 0, 1},
    port: 3478
  )
```

`serve/2` and `dial/2` are the friendly front door. They pick sensible framing
defaults and call `listen/1` or `Client.connect/1` for you. Pass `:accumulator`
or `:pipeline` when you want to override framing. Reach for `listen/1` when you
need SCTP, explicit modules, or multi-step pipelines.

### Try the examples

```bash
mix run examples/echo_udp.exs
```

1. `examples/echo_udp.exs` -- UDP echo
2. `examples/echo_tcp.exs` -- TCP with length prefixes
3. `examples/echo_tls.exs` -- TLS with a self-signed cert
4. `examples/echo_client.exs` -- `serve` + `dial` together
5. `examples/pipeline_inline.exs` -- two-step pipeline (advanced)
6. `examples/udp_relay.exs` -- Engine vs `open_relay/2` (advanced)

## Main modules

Everyday tools:

- `XSockets` -- `serve/2`, `dial/2`, `listen/1`, `listen_many/1`
- `XSockets.Client` -- outbound TCP/TLS
- `XSockets.Handler` -- your per-packet logic
- `XSockets.Engine` -- the shared "read, frame, dispatch" loop
- `Transport.*` -- UDP / TCP / TLS / DTLS / optional SCTP I/O
- `Accumulator.*` -- framing (`Raw`, `LengthPrefixed`, `Reorder`)

When you need more structure:

- `Pipeline` -- name several handler stages and route between them
- `Connection` -- one process per TCP/TLS/DTLS stream
- `DatagramServer` -- one process per UDP listen socket
- `Acceptor` -- accept loop for connection-oriented transports
- `SockSupervisor` / `TierSupervisor.*` -- supervision for connections and async work
- `RateLimit` (+ `FixedWindow`, `PerConnection`) -- optional Engine rate limiting
- `Sctp.Association` / `Sctp.Association.Server` / `Sctp.Dcep` -- WebRTC data channels
  (SCTP *inside* DTLS; different from IP-level `Transport.SCTP`)

STUN, TURN, RTP, SIP framing stays in *your* `Accumulator` / `Handler` modules.
This library only ships the generic plumbing.

## Features (in plain terms)

- One shape of API across UDP, TCP, TLS, DTLS, and optional SCTP
- Pluggable framing: "is there a whole packet yet?"
- Full drain: every complete message in a read is handled before the next read
- Backpressure on streams via a write queue and `{:busy, state}`
- Optional Telemetry under `[:xsockets, ...]`

**Control vs media:** put request-shaped traffic through the Engine and your
Handler. Put high-rate media on `UDP.open_relay/2` (or a raw socket) so it does
not share that loop.

**Two kinds of SCTP (easy to mix up):**

- `Transport.SCTP` + `Sctp.Listener` -- SCTP over IP (OTP `:gen_sctp`)
- `Sctp.Association` -- SCTP over a DTLS byte pipe (WebRTC data channels; needs
  optional `{:ex_sctp, "~> 0.1"}` and a Rust toolchain to compile)

**Useful config** (on `:xsockets` or your host `:config_app`):

- `engine_rate_limit` (default `true`) -- rate-limit when `conn.client_ip` is set
- `rate_limiter` -- default `FixedWindow` (datagrams only); use `PerConnection`
  if you also want to gate TCP/TLS chunks
- `pool_on_overflow` -- `:drop` (default) or `:inline_fallback` for busy `:pool` tiers
- `send_timeout_ms` (default `5000`) -- timeout before Engine replies
- `write_queue_max` (default `32`) -- outbound queue size on streams
- `datagram_write_on_error` -- `:retry_peer` (default) or `:drop` on UDP send failure

## Production configuration

Defaults lean toward "fail closed under load" (pool overflow drops work;
`FixedWindow` only watches datagrams). Opt in when you need stricter delivery:

```elixir
# Do not drop work on a busy :pool tier -- run it inline instead
config :xsockets, pool_on_overflow: :inline_fallback
# or per tier: on_overflow: :inline_fallback

# Also rate-limit TCP/TLS Engine chunks
config :xsockets, rate_limiter: XSockets.RateLimit.PerConnection
```

Async `:task` / `:pool` work is best-effort: a crashed worker can lose what was
in its mailbox, and `:drop` throws away work when the pool is full. Prefer
`:inline` for small control graphs, or `:inline_fallback` when a pool tier must
not lose messages.

## Installation

```elixir
def deps do
  [
    {:xsockets, "~> 1.0.0"},
    {:telemetry, "~> 1.0"}
    # optional: {:ex_sctp, "~> 0.1"}
  ]
end
```

## Advanced

Skip this until `serve` / `dial` are not enough. It covers multi-step pipelines,
reordering, async dispatch, and related knobs.

### Reordering

Sometimes packets arrive out of order and you need them sorted before your
handler sees them. `Accumulator.Reorder` wraps another accumulator, tags each
whole packet with a key (for example an RTP sequence number), and holds
stragglers until the window is contiguous -- or until a time/size limit says
"give up and flush."

```elixir
accumulator: {
  XSockets.Accumulator.Reorder,
  name: :rtp,
  inner: XSockets.Accumulator.LengthPrefixed,
  inner_opts: [header_size: 2],
  key_fun: &MyApp.RTP.sequence/2,
  window: 32,
  max_delay_ms: 100,
  on_overflow: :flush_oldest
}
```

Tunable keys (`window`, `max_delay_ms`, `on_overflow`, `enabled`) merge with
library defaults and app config:

```elixir
config :xsockets,
  config_app: :my_app,
  reorder: [
    rtp: [window: 32, max_delay_ms: 150]
  ]

config :my_app,
  reorder: [
    rtp: [window: 48]
  ]
```

Precedence: library defaults < `config :xsockets, :reorder` <
`config :config_app, :reorder` < keys in the accumulator spec.
`key_fun`, `inner`, and `name` always come from code -- never from config.

If an accumulator may hold packets across reads (waiting on a missing sequence
number), set `tick_interval_ms` on `Connection` or `DatagramServer` so the
Engine can flush on a timer even when no new data arrives.

### Pipelines

A pipeline is a small graph of stages. The root stage sees the wire; a handler
can `{:descend, :other_tier, payload, state}` to hand work to another stage:

```elixir
defmodule MyApp.Pipeline do
  use XSockets.Pipeline

  tier :root,
    accumulator: {XSockets.Accumulator.LengthPrefixed, header_size: 2},
    handler: MyApp.Handlers.Stun

  tier :rtp,
    accumulator: XSockets.Accumulator.Raw,
    handler: MyApp.Handlers.Rtp
end

{:ok, pid} =
  XSockets.listen(
    transport: XSockets.Transport.TCP,
    ip: {0, 0, 0, 0},
    port: 3478,
    pipeline: MyApp.Pipeline,
    assigns: %{}
  )
```

See `examples/pipeline_inline.exs`.

Worth knowing:

- A crash inside a descended tier is caught, emits `[:xsockets, :tier_crashed]`,
  and the outer tier keeps going.
- `{:close, state}` from *any* tier closes the whole connection (or stops that
  datagram's processing).
- Only `:root` gets `handle_connect/1`. Descended tiers start with `nil` state
  on first visit. On disconnect, every activated tier gets `handle_disconnect/2`.

### Dispatch strategies

Each tier can say how its work runs:

- **`:inline`** (default) -- same process; safest for "must not drop"
- **`:task`** -- async supervised worker; root keeps moving; best-effort if the
  worker crashes
- **`:pool`** -- like `:task` with a size cap. Overflow `:drop` (default) or
  `:inline_fallback`. For control-critical tiers, prefer `:inline_fallback`.

Start the supervisors (or let `XSockets.Application` do it):

```elixir
{:ok, _} = XSockets.SockSupervisor.start_link()
{:ok, _} = XSockets.TierSupervisor.Task.start_link()
{:ok, _} = XSockets.TierSupervisor.Pool.start_link()
```

### Bounded buffers

Built-in accumulators accept `:max_size`. Overflow shows up once as
`{:error, :buffer_overflow, acc}`; the Engine emits `[:xsockets, :frame_error]`
and keeps draining.

### Telemetry

`XSockets.Telemetry.emit/3` plus your own `:telemetry` handlers.
`Telemetry.attach_handlers/0` installs log/counter handlers for events the
library actually emits (listeners, packets, TLS handshake, rate limit, tiers,
send errors).

Common events when `:telemetry_enabled` is true:

- `[:xsockets, :tier_dispatch]` / `:tier_crashed` / `:tier_pool_saturated`
- `[:xsockets, :udp_sessions_evicted]` / `:frame_error`
- `[:xsockets, :message_sent]` / `:send_error`
- `[:xsockets, :listener_started]` / `:rate_limit_exceeded` / `:ssl_handshake_*`

### Configuration

Point `:config_app` at your host app to override defaults:

```elixir
config :xsockets,
  config_app: :my_app,
  buffer_size: 262_144,
  listener_buffer_size: 4_194_304,
  ssl_handshake_timeout: 10_000,
  rate_limit_enabled: true,
  telemetry_enabled: true

config :my_app,
  buffer_size: 131_072
```

Lookup order: `config :config_app` -> `config :xsockets` -> built-in default.

For TLS/DTLS certs, pass `certfile` / `keyfile` in listen opts, or set
`config :xsockets, certs: [...]`. Prefer
`Transport.TLS.security_opts(:server)` or `:mutual_tls` when building option lists.

## Testing

```bash
mix compile --warnings-as-errors
mix test
mix dialyzer
```

CI runs the same three checks.

Optional Linux SCTP proof (when your host has no SCTP):

```bash
docker compose -f docker-compose.sctp.yml run --rm xsockets-sctp
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

Apache 2.0 -- see [LICENSE.md](LICENSE.md).
