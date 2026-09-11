# PlugStatem

[![CI](https://github.com/jessestimpson/plug_statem/actions/workflows/ci.yml/badge.svg)](https://github.com/jessestimpson/plug_statem/actions/workflows/ci.yml)

A `gen_statem`-style event loop that fits the Plug contract.

Elixir HTTP servers using Plug require every write to a `Plug.Conn` to come from the
process that received the request. `PlugStatem.enter_loop/6` takes over that process
during the execution of a long-lived response, such as when serving SSE.

Over the lifetime of the response, your callback module will be executed just like
a standard `gen_statem`.

On `:stop`, `PlugStatem.enter_loop/5` returns a result instead of exiting the process,
so that the rest of the Plug `call/2` contract can be fulfilled.

```elixir
def call(%Plug.Conn{path_info: ["events"]} = conn, _opts) do
  conn = Plug.Conn.put_resp_content_type(conn, "text/event-stream")

  case PlugStatem.enter_loop(MyStream, [], conn, :streaming, %{}) do
    {_reason, _state, conn, _data} -> conn
  end
end
```

See the `PlugStatem` module documentation for more details.

## Installation

```elixir
def deps do
  [
    {:plug_statem, "~> 0.1.0"}
  ]
end
```
