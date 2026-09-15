defmodule XSockets.DoctestTest do
  use ExUnit.Case, async: false

  doctest XSockets.Config
  doctest XSockets.Spec
  doctest XSockets.Conn
  doctest XSockets.Pipeline
  doctest XSockets.Pipeline.Tier
  doctest XSockets.Accumulator.Raw
  doctest XSockets.Accumulator.LengthPrefixed
  doctest XSockets.Accumulator.Reorder
  doctest XSockets.Transport.TCP
  doctest XSockets.Transport.UDP
  doctest XSockets.Transport.TLS
  doctest XSockets.Transport.DTLS
  doctest XSockets.Transport.SCTP
  doctest XSockets.Telemetry
end
