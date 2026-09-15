# XSockets architecture

This note is a map of how XSockets fits together. You do not need to be a
networking specialist to follow it -- think in terms of "listen, read whole
messages, call my code, maybe write a reply."

XSockets is **format-agnostic**. It owns sockets, framing, and a shared drain
loop. It does **not** know STUN, TURN, RTP, or SIP. Those stay in the host app,
so the library never grows into a protocol stack.

For a hands-on start, see the [README](README.md) (`serve` / `dial` and the
`examples/` scripts). This file is for "where does this module sit?"

## The simple picture

```
Your app (Handler + framing)
        |
   serve / dial   (friendly front door)
        |
   listen / Client
        |
   Pipeline + Engine   (drain every whole packet, then ask for more)
        |
   Connection  or  DatagramServer
        |
   Transport.*   (:gen_tcp / :gen_udp / :ssl / :gen_sctp)
```

And separately, for WebRTC data channels (SCTP *inside* DTLS, not over raw IP):

```
Your DTLS plaintext pipe
        |
   Sctp.Association[.Server]   (sans-IO; you send {:transmit, ...} back on DTLS)
        |
   Sctp.Dcep                   (open/ack for data channels)
```

**Rule of thumb:** request-shaped traffic (handshakes, signaling) goes through
the Engine and a Handler. High-rate media should use `Transport.UDP.open_relay/2`
or a raw socket -- not the drain loop.

## On-ramp vs full control

- Prefer `XSockets.serve/2` and `XSockets.dial/2` for a single stage (one
  transport, one framer, one handler).
- Use `listen/1` + `Pipeline` when you need named stages, SCTP-over-IP, or
  explicit module picks.

`serve` / `dial` only normalize atom options and call the lower APIs. Nothing
magic -- just less boilerplate.

## What each layer does

| Layer | In one sentence |
|---|---|
| `XSockets` | Public entry: `serve`, `dial`, `listen`, `listen_many` |
| `Client` | Outbound TCP/TLS dial, then the same drain loop |
| `Transport.*` | Thin wrappers around OTP sockets (listen/accept/send/setopts) |
| `Accumulator.*` | "Do we have a whole packet yet?" |
| `Handler` | Your code per packet (`:ok` / `:reply` / `:busy` / `:close` / `:descend`) |
| `Engine` | Shared push-and-drain loop shared by UDP and streams |
| `Connection` | One process per TCP/TLS/DTLS stream (+ write queue) |
| `DatagramServer` | One process per UDP listen socket (+ per-peer sessions) |
| `Acceptor` | Accept loop for TCP/TLS/DTLS |
| `Pipeline` | Name several accumulator/handler stages and route between them |
| `RateLimit.*` | Optional gate before framing |
| `Sctp.Association` | WebRTC SCTP-over-DTLS (not a Transport) |

Full module list (same ideas, more names):

| Module | Role |
|---|---|
| `XSockets` | `serve/2`, `dial/2`, `listen/1`, `listen_many/1` (UDP reuseport) |
| `XSockets.Client` | Outbound TCP/TLS + `Connection` under SockSupervisor |
| `XSockets.Application` | Optional SockSupervisor + TierSupervisor children |
| `XSockets.Transport` | Behaviour for listen/accept/send/setopts/mailbox normalize |
| `Transport.UDP` | Datagrams, ICMP normalize, `open_relay/2`, optional `reuseport` |
| `Transport.TCP` | Stream listen/accept/connect |
| `Transport.TLS` | TLS 1.2/1.3; `security_opts/1`, `connect/3` |
| `Transport.DTLS` | DTLS over UDP; accept + `connect/3` |
| `Transport.SCTP` | Optional OTP `:gen_sctp` over IP |
| `Sctp.Listener` | Owns SCTP-over-IP associations (not `Acceptor.accept/2`) |
| `Sctp.Association` | WebRTC SCTP-over-DTLS via optional `ex_sctp` |
| `Sctp.Association.Server` | Optional GenServer bridge over the sans-IO association |
| `Sctp.Dcep` | Data-channel Open/Ack (RFC 8832) |
| `XSockets.Accumulator` | Framing behaviour |
| `Accumulator.Raw` | One chunk/datagram = one packet |
| `Accumulator.LengthPrefixed` | Size-prefixed stream framing |
| `Accumulator.Reorder` | Hold out-of-order packets until a key window is contiguous |
| `XSockets.Handler` | Application behaviour |
| `XSockets.Engine` | Shared drain loop + write-queue busy signaling |
| `XSockets.Connection` | One process per stream; bounded outbound write queue |
| `XSockets.DatagramServer` | One process per UDP listen socket |
| `XSockets.Acceptor` | Accept loop (`accept_workers`) |
| `XSockets.Pipeline` | `use` + `tier` DSL |
| `XSockets.SockSupervisor` | DynamicSupervisor for `Connection` children |
| `XSockets.TierSession` | Async worker for `:task` / `:pool` tiers |
| `TierSupervisor.Task` / `.Pool` | Supervisors for those workers |
| `XSockets.Config` | Host-app env overlay (`:config_app`) |
| `XSockets.RateLimit` | Pluggable Engine rate-limit behaviour |
| `RateLimit.FixedWindow` / `.PerConnection` | Built-in limiters |
| `RateLimit.Window` / `.Table` | ETS window bump + sweep (internal) |
| `XSockets.Telemetry` | Optional `[:xsockets, ...]` events |
| `XSockets.Conn` | Per-packet view (`client_ip`, `socket`, `assigns`) |

## How processes are arranged

**UDP (plain).** One `DatagramServer` owns the listen socket. Each remote peer
is a small session (framing + handler state) *inside* that process -- not a
separate GenServer per peer. Incoming datagrams are framed and fully drained
before the socket is re-armed for more. `listen_many/1` starts several servers
on one port with `reuseport: true` when you want more readers.

**TCP / TLS / DTLS.** An `Acceptor` (or `listen/1`) accepts, hands the socket
to a `Connection`, and that process runs the same Engine. `Client.connect/1`
dials TCP/TLS; `Transport.DTLS.connect/3` dials DTLS. Bytes can span several
reads; if one read contains two messages, both are handled before the next read.

**SCTP over IP (optional).** When `Transport.SCTP.available?/0` is true,
`listen/1` starts `Sctp.Listener`, which owns associations. Outbound dials that
need an association id use `connect_assoc/3`. Hosts without SCTP get
`{:error, :sctp_not_supported}`. Linux proof image:
`docker-compose -f docker-compose.sctp.yml run --rm xsockets-sctp`.

**WebRTC data channels** use `Sctp.Association` (SCTP *inside* DTLS via optional
`ex_sctp`), **not** `Transport.SCTP`. Your app feeds DTLS plaintext with
`handle_packet/2` and must send every `{:transmit, packets}` event back on the
DTLS socket -- or run `Sctp.Association.Server`. Channel setup uses `Sctp.Dcep`.

`XSockets.Application` starts `SockSupervisor` and the tier supervisors when
`start_supervisors` is true (default). Set
`config :xsockets, start_supervisors: false` if you want to place them in your
own supervision tree first.

## Engine and pipelines

After each read, `Engine.push_and_drain/9` (and on a timer tick, `Engine.drain/8`)
asks the current tier's accumulator for whole packets until it says
"need more data."

When `engine_rate_limit` is true (default) and `conn.client_ip` is set, the
configured rate limiter runs *before* framing. Default is `FixedWindow`
(datagrams only). Use `PerConnection` if you also want to gate TCP/TLS chunks.

A Handler may return:

- `{:ok, state}` -- consumed, nothing to send
- `{:reply, iodata, state}` -- send on the same socket
- `{:busy, state}` -- stop draining for now; streams may flush a write queue
- `{:descend, tier, payload, state}` -- hand payload to another named tier
- `{:close, state}` -- stop this connection / datagram path

Failed replies become `{:busy, iodata}` so `Connection` can queue and retry.
Under sustained send errors, streams piggyback flush on inbound messages and
use a short adaptive backoff. UDP uses `datagram_write_on_error`
(`:retry_peer` by default, or `:drop`) and always re-arms reads.

How a descended tier runs:

- `:inline` (default) -- same process; use when work must not be dropped
- `:task` -- async supervised worker; best-effort (mailbox lost on crash)
- `:pool` -- like `:task` with a size cap; overflow `:drop` (default) or
  `:inline_fallback`. Prefer `:inline_fallback` for control-critical tiers.

Unhandled exceptions in a descended tier are caught, emit
`[:xsockets, :tier_crashed]`, and the outer tier continues. `{:close, _}` from
any tier closes the whole connection.

## How hosts usually wire it

1. Start with `serve/2` or `dial/2`.
2. Move to `listen/1` / `Client.connect/1` when you need workers, explicit
   transports, or a `Pipeline`.
3. Declare custom accumulators and handlers for *your* wire format.
4. For high-rate relay or media, call `Transport.UDP.open_relay/2` and own that
   socket in a host process -- do not put it under `DatagramServer`.

That is the whole idea: XSockets keeps the boring socket loop correct and
shared; your app keeps the protocol smarts.
