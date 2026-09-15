# Changelog

## 1.0.0

First standalone release of `xsockets` / `XSockets`.

- Ported from [xturn-sockets](https://github.com/Lazarus404/xturn-sockets) 2.2.1 and renamed for host-agnostic use (`Xirsys.Sockets` -> `XSockets`, `:xturn_sockets` -> `:xsockets`, telemetry `[:xsockets, ...]`).
- Format-agnostic listen/accept, pluggable framing, shared drain engine, multi-protocol transports (UDP/TCP/TLS/DTLS/SCTP).
- `XSockets.serve/2` and `XSockets.dial/2` on-ramp (atom `:transport` / `:framing`); `listen/1` and `listen_many/1` (UDP reuseport) facades; optional `XSockets.Application` supervisors (`start_supervisors`). UDP uses `DatagramServer`; TCP/TLS/DTLS use `Acceptor`; SCTP uses `Sctp.Listener` when the OS/OTP stack supports it (`Transport.SCTP.available?/0`).
- `listen(workers: n)` for UDP reuseport; `Acceptor` `accept_workers: n` with unexpected worker respawn.
- `XSockets.Client.connect/1` for outbound TCP/TLS with the same drain loop; `Transport.DTLS.connect/3` for outbound DTLS.
- Engine rate limit via pluggable `XSockets.RateLimit` (`FixedWindow` default for datagram; `PerConnection` for all framings); shared concurrency-safe `RateLimit.Window` bump helper; ETS sweep via `RateLimit.Table` (always started from `Application`).
- Pool overflow `:drop` | `:inline_fallback`, send timeout via `send_timeout_ms`.
- Stream write queue and handler `{:busy, state}` backpressure (`write_queue_max`); piggyback flush plus adaptive backoff timer (10/25/50/100 ms); send failures return `{:busy, iodata}` from `Engine`.
- UDP `datagram_write_on_error`: `:retry_peer` (default) or `:drop`.
- TLS `security_opts/1` presets (`:server` default; `:mutual_tls`) and `Transport.TLS.connect/3`. Certificates from listen opts or `Config.get(:certs)` only (no legacy `:xturn` / `:certs` app peeks).
- SCTP clustered under `XSockets.Sctp.*` (`Listener`, `Association`, `Association.Server`, `Dcep`); `Transport.SCTP` association lifecycle (`connect_assoc/3`, `decode_message/1`); optional on hosts without SCTP. Linux proof: `docker-compose -f docker-compose.sctp.yml run --rm xsockets-sctp`.
- `Telemetry.attach_handlers/0` matches events the library emits; rate-limit ETS table `:xsockets_rate_limits`.
- `Transport.DTLS.close/1` emits `[:xsockets, :socket_closed]` with `protocol: :dtls`.
- Pipeline multi-tier dispatch (`:inline` / `:task` / `:pool`), relay helpers (`Transport.UDP.open_relay/2`), dialyxir, package CI (compile warnings-as-errors, test, dialyzer), `ARCHITECTURE.md` in the Hex package.
- Examples ladder: `echo_udp`, `echo_tcp`, `echo_tls`, `echo_client`, `pipeline_inline`, `udp_relay`; README Start-here progressive disclosure.
- `sock_supervisor.ex` file name matches `SockSupervisor` module.
