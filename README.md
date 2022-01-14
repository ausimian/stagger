# Stagger

[![Build Status](https://github.com/ausimian/stagger/actions/workflows/elixir.yml/badge.svg)](https://github.com/ausimian/stagger/actions?query=workflow%3A%22CI%22)
[![Coverage Status](https://coveralls.io/repos/github/ausimian/stagger/badge.svg?branch=main)](https://coveralls.io/github/ausimian/stagger?branch=main)
[![Hex](https://img.shields.io/hexpm/v/stagger.svg)](https://hex.pm/packages/stagger)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/stagger)

Point-to-point, durable message-queues as GenStage producers.

Stagger enables the creation of GenStage processes that enqueue terms to simple,
file-backed message-queues, allowing the producer and consumer to run independently
of each other, possibly at different times.

    +----------+    +----------+    +----------+       +------------+
    | Upstream |    | MsgQueue |    | MsgQueue |       | Downstream |
    |          | -> |          | <- |          | <---> |            |
    |  Client  |    | Producer |    | Consumer |       | Processing |
    +----------+    +----------+    +----------+       +------------+
                      |    | read
                write |    |
                     +------+
                     | FILE |
                     |      |
                     |      |
                     +------+

Your upstream client writes its events into the message-queue (provided by
Stagger), which persists them to local storage.  Your (GenStage) consumer, subscribes
to the producer and receives events, via this local storage.
## Installation

The package can be installed by adding `stagger` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:stagger, "~> 0.1.7"}
  ]
end
```

The docs can be found at <https://hexdocs.pm/stagger>.

## Copyright and License

Copyright (c) 2022, Nick Gunn

Stagger runtime source code is licensed under the [MIT License](LICENSE.md).
Stagger test source code is licensed under the [GPL3 License](test/LICENSE).

