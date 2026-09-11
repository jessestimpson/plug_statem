defmodule PlugStatemBanditTest do
  # Runs the loop inside a real Bandit HTTP/1.1 request and talks to it over
  # a raw TCP socket, so the chunked framing and Bandit's own bookkeeping are
  # exercised rather than stubbed.
  use ExUnit.Case, async: true

  alias Plug.Conn

  defmodule Stream do
    @behaviour PlugStatem

    def callback_mode, do: [:handle_event_function, :state_enter]

    def handle_event(:enter, _old, :streaming, conn, %{test: test} = data) do
      conn = Conn.send_chunked(conn, 200)
      send(test, {:streaming, self()})
      {:keep_state, conn, data}
    end

    def handle_event(:info, {:event, iodata}, :streaming, conn, data) do
      case Conn.chunk(conn, iodata) do
        {:ok, conn} -> {:keep_state, conn, data}
        {:error, reason} -> {:stop, {:shutdown, reason}, conn, data}
      end
    end

    def handle_event({:call, from}, :ping, :streaming, _conn, _data),
      do: {:keep_state_and_data, {:reply, from, :pong}}

    def handle_event(:info, :stop, :streaming, conn, data), do: {:stop, :normal, conn, data}

    def terminate(reason, _state, _conn, %{test: test}), do: send(test, {:terminated, reason})
  end

  defmodule Endpoint do
    @behaviour Plug

    def init(test), do: test

    def call(%Conn{path_info: ["stream"]} = conn, test) do
      conn = Conn.put_resp_content_type(conn, "text/event-stream")

      {_reason, :streaming, conn, _data} =
        PlugStatem.enter_loop(Stream, [], conn, :streaming, %{test: test})

      conn
    end

    def call(conn, _test), do: Conn.send_resp(conn, 200, "plain")
  end

  setup do
    pid =
      start_supervised!(
        {Bandit, plug: {Endpoint, self()}, ip: :loopback, port: 0, startup_log: false}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    {:ok, sock} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    %{sock: sock}
  end

  test "streams chunks, answers calls, and leaves the connection reusable", %{sock: sock} do
    get(sock, "/stream")
    assert_receive {:streaming, stream}

    headers = recv_until(sock, "\r\n\r\n")
    assert headers =~ "HTTP/1.1 200"
    assert String.downcase(headers) =~ "content-type: text/event-stream"
    assert String.downcase(headers) =~ "transfer-encoding: chunked"

    send(stream, {:event, "data: hello\n\n"})
    assert recv_until(sock, "data: hello\n\n") =~ "data: hello"

    assert :gen_statem.call(stream, :ping) == :pong

    send(stream, :stop)
    assert_receive {:terminated, :normal}
    # Bandit closes the chunked body once the plug returns the conn.
    assert recv_until(sock, "0\r\n\r\n")

    # The same connection serves another request, so the loop left Bandit's
    # process in working order.
    get(sock, "/plain")
    response = recv_until(sock, "plain")
    assert response =~ "HTTP/1.1 200"
  end

  test "a client disconnect surfaces as a failed write and stops the loop", %{sock: sock} do
    get(sock, "/stream")
    assert_receive {:streaming, stream}
    recv_until(sock, "\r\n\r\n")

    :ok = :gen_tcp.close(sock)

    # The first write after a close may still succeed at the TCP level, so
    # keep writing until the failure reaches the loop.
    reason =
      Enum.find_value(1..100, fn _ ->
        send(stream, {:event, "data: x\n\n"})

        receive do
          {:terminated, reason} -> reason
        after
          20 -> nil
        end
      end)

    assert {:shutdown, _} = reason
  end

  defp get(sock, path) do
    :ok = :gen_tcp.send(sock, "GET #{path} HTTP/1.1\r\nHost: localhost\r\n\r\n")
  end

  # Reads until the accumulated bytes contain `marker`, then returns them.
  defp recv_until(sock, marker, acc \\ "") do
    if String.contains?(acc, marker) do
      acc
    else
      {:ok, data} = :gen_tcp.recv(sock, 0, 2_000)
      recv_until(sock, marker, acc <> data)
    end
  end
end
