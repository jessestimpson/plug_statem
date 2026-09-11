defmodule PlugStatem do
  @moduledoc """
  A `gen_statem`-style event loop that fits the Plug contract.

  Elixir HTTP servers using Plug require every write to a `Plug.Conn` to come from the
  process that received the request. `PlugStatem.enter_loop/6` takes over that process
  during the execution of a long-lived response, such as when serving SSE.

  Over the lifetime of the response, your callback module will be executed just like
  a standard `gen_statem`.

  On `:stop`, `PlugStatem.enter_loop/5` returns a result instead of exiting the process,
  so that the rest of the Plug `call/2` contract can be fulfilled.

  There are a handful of message shapes that are reserved by Bandit. Messages
  that match these patterns will not be delivered to your module as an `:info`
  event.

  - `{:bandit, _}`
  - `{:plug_conn, :sent}`

  ## Example

  A Plug's `call/2` enters the loop and returns the `conn` when finished:

  ```elixir
  def call(%Plug.Conn{path_info: ["events"]} = conn, _opts) do
    conn = Plug.Conn.put_resp_content_type(conn, "text/event-stream")

    case PlugStatem.enter_loop(MyStream, [], conn, :starting, %{}) do
      {_reason, _state, conn, _data} -> conn
    end
  end
  ```

  Your module streams chunks to the `conn`:

  ```elixir
  defmodule MyStream do
    @behaviour PlugStatem

    @keep_alive {{:timeout, :keep_alive}, 15_000, nil}

    @impl true
    def callback_mode, do: [:handle_event_function, :state_enter]

    @impl true
    def handle_event(:enter, _old, :streaming, conn, data) do
      {:keep_state, Plug.Conn.send_chunked(conn, 200), data, @keep_alive}
    end

    def handle_event(:info, {:event, iodata}, :streaming, conn, data) do
      write(conn, iodata, data)
    end

    def handle_event({:timeout, :keep_alive}, nil, :streaming, conn, data) do
      write(conn, ": keep-alive\\n\\n", data)
    end

    def handle_event(:info, :stop, :streaming, conn, data) do
      {:stop, :normal, conn, data}
    end

    defp write(conn, iodata, data) do
      case Plug.Conn.chunk(conn, iodata) do
        {:ok, conn} -> {:keep_state, conn, data, @keep_alive}
        {:error, reason} -> {:stop, {:shutdown, reason}, conn, data}
      end
    end
  end
  ```

  Other processes can drive the stream by sending it messages, for example
  `send(pid, {:event, "data: hello\\n\\n"})`. With the `:name` option they can
  find it through a `Registry` instead of holding the pid.

  ## Callbacks

  The callback module mirrors `gen_statem`, with the `Plug.Conn` threaded through
  every callback alongside `state` and `data`.

      callback_mode() :: mode | [mode | :state_enter]
        where mode :: :handle_event_function | :state_functions

      # callback_mode :handle_event_function
      handle_event(event_type, event_content, state, conn, data) :: result

      # callback_mode :state_functions, one function per state atom
      state_name(event_type, event_content, conn, data) :: result

      # optional
      terminate(reason, state, conn, data) :: term

  ## Events

      :info                message that is not one of the below
      {:call, from}        from `:gen_statem.call/2,3`; reply with a `:reply` action
      :cast                from `:gen_statem.cast/2`
      :state_timeout       content of a `:state_timeout` action
      :timeout             content of a `:timeout` action
      {:timeout, name}     content of a `{:timeout, name}` action
      :enter               state enter call; content is the previous state
      any other type       inserted by a `:next_event` action

  ## Results

      {:next_state, state, conn, data}
      {:next_state, state, conn, data, actions}
      {:keep_state, conn, data}
      {:keep_state, conn, data, actions}
      :keep_state_and_data
      {:keep_state_and_data, actions}
      {:repeat_state, conn, data}
      {:repeat_state, conn, data, actions}
      :repeat_state_and_data
      {:repeat_state_and_data, actions}
      {:stop, reason}
      {:stop, reason, conn, data}
      {:stop_and_reply, reason, replies}
      {:stop_and_reply, reason, replies, conn, data}

  A state enter call may not change the state and may not use `:postpone`
  or `:next_event`.

  ## Actions

      {:reply, from, reply}
      :postpone | {:postpone, boolean}
      {:next_event, event_type, event_content}
      {:state_timeout, ms | :infinity, content}
      {:state_timeout, :cancel}
      {:timeout, ms | :infinity, content}
      {:timeout, :cancel}
      {{:timeout, name}, ms | :infinity, content}
      {{:timeout, name}, :cancel}

  Not yet implemented, and rejected with an `ArgumentError` naming the action:

      :hibernate
      {timeout_type, :update, content}
      {timeout_type, ms, content, opts}
      {:change_callback_module, module}
      {:push_callback_module, module}
      :pop_callback_module

  `format_status/1` and `code_change/4` are never called.

  After a state change, events are handled in this order: events inserted by
  `:next_event`, then postponed events in the order they were postponed, then
  anything already queued, then the mailbox.

  ## Options

  `enter_loop/6` takes a keyword list of options as its second argument, in
  the position `GenServer.start_link/3` uses:

    * `:name` - registers the current process for the lifetime of the loop
      under a name, in any of the forms `GenServer` accepts: an atom, a
      `{:global, term}`, or a `{:via, module, term}` such as
      `{:via, Registry, {MyRegistry, key}}`. The name is unregistered when the
      loop returns, including when a callback raises. If the name is already
      taken, `enter_loop/6` returns `{:error, {:already_started, pid}}`
      without running any callback.
  """

  @type event_type ::
          :info
          | {:call, from()}
          | :cast
          | :state_timeout
          | :timeout
          | {:timeout, term()}
          | :enter
          | term()
  @type from :: {pid(), term()}
  @type state :: term()
  @type conn :: term()
  @type data :: term()
  @type reply_action :: {:reply, from(), term()}
  @type action ::
          reply_action()
          | :postpone
          | {:postpone, boolean()}
          | {:next_event, event_type(), term()}
          | {:state_timeout, timeout(), term()}
          | {:state_timeout, :cancel}
          | {:timeout, timeout(), term()}
          | {:timeout, :cancel}
          | {{:timeout, term()}, timeout(), term()}
          | {{:timeout, term()}, :cancel}
  @type actions :: [action()] | action()

  @type result ::
          {:next_state, state(), conn(), data()}
          | {:next_state, state(), conn(), data(), actions()}
          | {:keep_state, conn(), data()}
          | {:keep_state, conn(), data(), actions()}
          | :keep_state_and_data
          | {:keep_state_and_data, actions()}
          | {:repeat_state, conn(), data()}
          | {:repeat_state, conn(), data(), actions()}
          | :repeat_state_and_data
          | {:repeat_state_and_data, actions()}
          | {:stop, term()}
          | {:stop, term(), conn(), data()}
          | {:stop_and_reply, term(), [reply_action()] | reply_action()}
          | {:stop_and_reply, term(), [reply_action()] | reply_action(), conn(), data()}

  @callback callback_mode() ::
              :handle_event_function
              | :state_functions
              | [:handle_event_function | :state_functions | :state_enter]
  @callback handle_event(event_type(), term(), state(), conn(), data()) :: result()
  @callback terminate(reason :: term(), state(), conn(), data()) :: term()
  @optional_callbacks handle_event: 5, terminate: 4

  defstruct module: nil,
            mode: nil,
            state_enter?: false,
            state: nil,
            conn: nil,
            data: nil,
            # {type, content} of the event being handled, for :postpone
            event: nil,
            # events to handle before the mailbox, in order
            queue: [],
            # postponed events, most recent first
            postponed: [],
            # {ref, content} | nil
            state_timer: nil,
            event_timer: nil,
            # name => {ref, content}
            timers: %{}

  @type option :: {:name, GenServer.name()}

  @doc """
  Runs the loop in the current process until a callback stops it.

  Returns `{reason, state, conn, data}`, or `{:error, {:already_started, pid}}`
  if the `:name` option names a registered process. Exceptions raised by
  callbacks propagate to the caller.
  """
  @spec enter_loop(module(), [option()], conn(), state(), data(), actions()) ::
          {reason :: term(), state(), conn(), data()}
          | {:error, {:already_started, pid()}}
  def enter_loop(module, opts, conn, state, data, actions \\ []) do
    case Keyword.fetch(opts, :name) do
      :error ->
        run(module, conn, state, data, actions)

      {:ok, name} ->
        case register_name(name) do
          :ok ->
            try do
              run(module, conn, state, data, actions)
            after
              unregister_name(name)
            end

          {:error, pid} ->
            {:error, {:already_started, pid}}
        end
    end
  end

  defp run(module, conn, state, data, actions) do
    {mode, state_enter?} = parse_callback_mode(module.callback_mode())

    loop = %__MODULE__{
      module: module,
      mode: mode,
      state_enter?: state_enter?,
      state: state,
      conn: conn,
      data: data
    }

    {loop, next_events} = apply_actions(loop, actions, :enter_loop)

    case maybe_enter_state(loop, state) do
      {:ok, loop} -> loop(%{loop | queue: next_events})
      {:stop, result} -> result
    end
  end

  defp register_name(name) when is_atom(name) do
    try do
      Process.register(self(), name)
      :ok
    rescue
      ArgumentError -> {:error, GenServer.whereis(name)}
    end
  end

  defp register_name({:global, term} = name),
    do: registered(:global.register_name(term, self()), name)

  defp register_name({:via, module, term} = name),
    do: registered(module.register_name(term, self()), name)

  defp registered(:yes, _name), do: :ok
  defp registered(:no, name), do: {:error, GenServer.whereis(name)}

  defp unregister_name(name) when is_atom(name), do: Process.unregister(name)
  defp unregister_name({:global, term}), do: :global.unregister_name(term)
  defp unregister_name({:via, module, term}), do: module.unregister_name(term)

  defp parse_callback_mode(mode) when mode in [:handle_event_function, :state_functions],
    do: {mode, false}

  defp parse_callback_mode(modes) when is_list(modes) do
    mode =
      Enum.find(modes, &(&1 in [:handle_event_function, :state_functions])) ||
        raise ArgumentError,
              "callback_mode must include one of :handle_event_function or :state_functions"

    {mode, :state_enter in modes}
  end

  ## Receive loop

  # Bandit's reserved messages. They must be left in the mailbox.
  defguardp is_reserved(message)
            when (is_tuple(message) and tuple_size(message) == 2 and elem(message, 0) == :bandit) or
                   message == {:plug_conn, :sent}

  # Events inserted by next_event or retried after postponing come first.
  defp loop(%__MODULE__{queue: [{type, content} | rest]} = loop) do
    dispatch(%{loop | queue: rest}, type, content)
  end

  defp loop(%__MODULE__{queue: []} = loop) do
    %__MODULE__{timers: timers} = loop
    {state_ref, state_content} = loop.state_timer || {nil, nil}
    {event_ref, event_content} = loop.event_timer || {nil, nil}

    # Timer messages carry the timer's key. The ref must match the live timer
    # of that key, or the message is stale and falls through to :info.
    receive do
      {:timeout, ^state_ref, :state_timer} ->
        dispatch(loop, :state_timeout, state_content)

      {:timeout, ^event_ref, :event_timer} ->
        dispatch(loop, :timeout, event_content)

      {:timeout, ref, {:timer, name}}
      when is_map_key(timers, name) and elem(:erlang.map_get(name, timers), 0) == ref ->
        {^ref, content} = Map.fetch!(timers, name)
        dispatch(loop, {:timeout, name}, content)

      {:"$gen_call", from, request} ->
        dispatch(loop, {:call, from}, request)

      {:"$gen_cast", message} ->
        dispatch(loop, :cast, message)

      message when not is_reserved(message) ->
        dispatch(loop, :info, message)
    end
  end

  defp dispatch(loop, type, content) do
    # Any event cancels the event timeout.
    loop = %{cancel_timer(loop, :event_timer) | event: {type, content}}

    case handle_result(loop, call(loop, type, content)) do
      {:ok, loop} -> loop(loop)
      {:stop, result} -> result
    end
  end

  defp call(%__MODULE__{mode: :handle_event_function} = loop, type, content) do
    loop.module.handle_event(type, content, loop.state, loop.conn, loop.data)
  end

  defp call(%__MODULE__{mode: :state_functions, state: state} = loop, type, content)
       when is_atom(state) do
    apply(loop.module, state, [type, content, loop.conn, loop.data])
  end

  defp call(%__MODULE__{mode: :state_functions, state: state}, _type, _content) do
    raise ArgumentError, "state must be an atom in :state_functions mode, got: #{inspect(state)}"
  end

  ## Results of event callbacks

  defp handle_result(loop, {:next_state, state, conn, data}),
    do: transition(loop, state, conn, data, [])

  defp handle_result(loop, {:next_state, state, conn, data, actions}),
    do: transition(loop, state, conn, data, actions)

  defp handle_result(loop, {:keep_state, conn, data}),
    do: transition(loop, loop.state, conn, data, [])

  defp handle_result(loop, {:keep_state, conn, data, actions}),
    do: transition(loop, loop.state, conn, data, actions)

  defp handle_result(loop, :keep_state_and_data),
    do: transition(loop, loop.state, loop.conn, loop.data, [])

  defp handle_result(loop, {:keep_state_and_data, actions}),
    do: transition(loop, loop.state, loop.conn, loop.data, actions)

  defp handle_result(loop, {:repeat_state, conn, data}),
    do: repeat(loop, conn, data, [])

  defp handle_result(loop, {:repeat_state, conn, data, actions}),
    do: repeat(loop, conn, data, actions)

  defp handle_result(loop, :repeat_state_and_data),
    do: repeat(loop, loop.conn, loop.data, [])

  defp handle_result(loop, {:repeat_state_and_data, actions}),
    do: repeat(loop, loop.conn, loop.data, actions)

  defp handle_result(loop, {:stop, reason}),
    do: stop(loop, reason)

  defp handle_result(loop, {:stop, reason, conn, data}),
    do: stop(%{loop | conn: conn, data: data}, reason)

  defp handle_result(loop, {:stop_and_reply, reason, replies}),
    do: stop_and_reply(loop, reason, replies)

  defp handle_result(loop, {:stop_and_reply, reason, replies, conn, data}),
    do: stop_and_reply(%{loop | conn: conn, data: data}, reason, replies)

  defp handle_result(_loop, other),
    do: raise(ArgumentError, "bad return value from state function: #{inspect(other)}")

  defp transition(loop, state, conn, data, actions) do
    old_state = loop.state
    changed? = state != old_state

    # A change of state cancels the state timeout; keeping the state does not.
    loop = if changed?, do: cancel_timer(loop, :state_timer), else: loop
    loop = %{loop | state: state, conn: conn, data: data}
    {loop, next_events} = apply_actions(loop, actions, :event)

    entered = if changed?, do: maybe_enter_state(loop, old_state), else: {:ok, loop}

    with {:ok, loop} <- entered do
      loop = if changed?, do: retry_postponed(loop), else: loop
      {:ok, %{loop | queue: next_events ++ loop.queue}}
    end
  end

  defp repeat(loop, conn, data, actions) do
    loop = %{loop | conn: conn, data: data}
    {loop, next_events} = apply_actions(loop, actions, :event)

    with {:ok, loop} <- maybe_enter_state(loop, loop.state) do
      {:ok, %{loop | queue: next_events ++ loop.queue}}
    end
  end

  defp retry_postponed(%__MODULE__{postponed: postponed, queue: queue} = loop) do
    %{loop | postponed: [], queue: Enum.reverse(postponed) ++ queue}
  end

  ## State enter calls

  defp maybe_enter_state(%__MODULE__{state_enter?: false} = loop, _old_state), do: {:ok, loop}

  defp maybe_enter_state(loop, old_state) do
    handle_enter_result(loop, call(loop, :enter, old_state))
  end

  defp handle_enter_result(loop, {:next_state, state, conn, data}) when state == loop.state,
    do: enter_done(loop, conn, data, [])

  defp handle_enter_result(loop, {:next_state, state, conn, data, actions})
       when state == loop.state,
       do: enter_done(loop, conn, data, actions)

  defp handle_enter_result(loop, {:next_state, state, _conn, _data}),
    do: raise_enter_state_change(loop, state)

  defp handle_enter_result(loop, {:next_state, state, _conn, _data, _actions}),
    do: raise_enter_state_change(loop, state)

  defp handle_enter_result(loop, {:keep_state, conn, data}),
    do: enter_done(loop, conn, data, [])

  defp handle_enter_result(loop, {:keep_state, conn, data, actions}),
    do: enter_done(loop, conn, data, actions)

  defp handle_enter_result(loop, :keep_state_and_data),
    do: enter_done(loop, loop.conn, loop.data, [])

  defp handle_enter_result(loop, {:keep_state_and_data, actions}),
    do: enter_done(loop, loop.conn, loop.data, actions)

  defp handle_enter_result(loop, {:repeat_state, conn, data}),
    do: enter_repeat(loop, conn, data, [])

  defp handle_enter_result(loop, {:repeat_state, conn, data, actions}),
    do: enter_repeat(loop, conn, data, actions)

  defp handle_enter_result(loop, :repeat_state_and_data),
    do: enter_repeat(loop, loop.conn, loop.data, [])

  defp handle_enter_result(loop, {:repeat_state_and_data, actions}),
    do: enter_repeat(loop, loop.conn, loop.data, actions)

  defp handle_enter_result(loop, {:stop, reason}),
    do: stop(loop, reason)

  defp handle_enter_result(loop, {:stop, reason, conn, data}),
    do: stop(%{loop | conn: conn, data: data}, reason)

  defp handle_enter_result(loop, {:stop_and_reply, reason, replies}),
    do: stop_and_reply(loop, reason, replies)

  defp handle_enter_result(loop, {:stop_and_reply, reason, replies, conn, data}),
    do: stop_and_reply(%{loop | conn: conn, data: data}, reason, replies)

  defp handle_enter_result(_loop, other),
    do: raise(ArgumentError, "bad return value from state enter call: #{inspect(other)}")

  defp enter_done(loop, conn, data, actions) do
    {loop, []} = apply_actions(%{loop | conn: conn, data: data}, actions, :enter)
    {:ok, loop}
  end

  defp enter_repeat(loop, conn, data, actions) do
    {loop, []} = apply_actions(%{loop | conn: conn, data: data}, actions, :enter)
    maybe_enter_state(loop, loop.state)
  end

  defp raise_enter_state_change(loop, state) do
    raise ArgumentError,
          "state enter call may not change state: #{inspect(loop.state)} -> #{inspect(state)}"
  end

  ## Stopping

  defp stop_and_reply(loop, reason, replies) do
    Enum.each(List.wrap(replies), fn
      {:reply, from, reply} -> :gen_statem.reply(from, reply)
      other -> raise ArgumentError, "bad reply action: #{inspect(other)}"
    end)

    stop(loop, reason)
  end

  defp stop(loop, reason) do
    loop =
      loop
      |> cancel_timer(:state_timer)
      |> cancel_timer(:event_timer)
      |> cancel_all_named_timers()

    %__MODULE__{module: module, state: state, conn: conn, data: data} = loop

    if function_exported?(module, :terminate, 4) do
      module.terminate(reason, state, conn, data)
    end

    {:stop, {reason, state, conn, data}}
  end

  ## Actions
  #
  # `context` is :event for an event callback, :enter for a state enter call,
  # and :enter_loop for the actions given to enter_loop/5. Returns the loop and
  # the list of next_events collected, in order.

  defp apply_actions(loop, actions, context) do
    {loop, next_events} =
      Enum.reduce(List.wrap(actions), {loop, []}, fn action, {loop, next_events} ->
        apply_action(action, loop, next_events, context)
      end)

    {loop, Enum.reverse(next_events)}
  end

  defp apply_action({:reply, from, reply}, loop, next_events, _context) do
    :gen_statem.reply(from, reply)
    {loop, next_events}
  end

  defp apply_action(:postpone, loop, next_events, context),
    do: apply_action({:postpone, true}, loop, next_events, context)

  defp apply_action({:postpone, false}, loop, next_events, _context),
    do: {loop, next_events}

  defp apply_action(
         {:postpone, true},
         %__MODULE__{event: {_, _} = event} = loop,
         next_events,
         :event
       ),
       do: {%{loop | postponed: [event | loop.postponed]}, next_events}

  defp apply_action({:postpone, true}, _loop, _next_events, context),
    do: raise(ArgumentError, "postpone is not allowed in #{context_name(context)}")

  defp apply_action({:next_event, type, content}, loop, next_events, context)
       when context in [:event, :enter_loop],
       do: {loop, [{type, content} | next_events]}

  defp apply_action({:next_event, _type, _content}, _loop, _next_events, context),
    do: raise(ArgumentError, "next_event is not allowed in #{context_name(context)}")

  defp apply_action({type, :update, _content} = action, _loop, _next_events, _context)
       when type in [:state_timeout, :timeout] or (is_tuple(type) and elem(type, 0) == :timeout),
       do: not_implemented(action)

  defp apply_action({type, _time, _content, _opts} = action, _loop, _next_events, _context)
       when type in [:state_timeout, :timeout] or (is_tuple(type) and elem(type, 0) == :timeout),
       do: not_implemented(action)

  defp apply_action({:change_callback_module, _module} = action, _loop, _next_events, _context),
    do: not_implemented(action)

  defp apply_action({:push_callback_module, _module} = action, _loop, _next_events, _context),
    do: not_implemented(action)

  defp apply_action(:pop_callback_module = action, _loop, _next_events, _context),
    do: not_implemented(action)

  defp apply_action({:state_timeout, time, content}, loop, next_events, _context),
    do: {start_timer(loop, :state_timer, time, content), next_events}

  defp apply_action({:state_timeout, :cancel}, loop, next_events, _context),
    do: {cancel_timer(loop, :state_timer), next_events}

  defp apply_action({:timeout, time, content}, loop, next_events, _context),
    do: {start_timer(loop, :event_timer, time, content), next_events}

  defp apply_action({:timeout, :cancel}, loop, next_events, _context),
    do: {cancel_timer(loop, :event_timer), next_events}

  defp apply_action({{:timeout, name}, time, content}, loop, next_events, _context),
    do: {start_timer(loop, {:timer, name}, time, content), next_events}

  defp apply_action({{:timeout, name}, :cancel}, loop, next_events, _context),
    do: {cancel_timer(loop, {:timer, name}), next_events}

  defp apply_action(hibernate, _loop, _next_events, _context)
       when hibernate == :hibernate or
              (is_tuple(hibernate) and elem(hibernate, 0) == :hibernate),
       do: raise(ArgumentError, "hibernate is not supported by PlugStatem")

  defp apply_action(other, _loop, _next_events, _context),
    do: raise(ArgumentError, "bad action: #{inspect(other)}")

  defp not_implemented(action),
    do: raise(ArgumentError, "not implemented by PlugStatem: #{inspect(action)}")

  defp context_name(:enter), do: "a state enter call"
  defp context_name(:enter_loop), do: "enter_loop actions"

  ## Timers
  #
  # Erlang timers addressed to self/0. The message is {:timeout, ref, key}
  # where key is :state_timer, :event_timer, or {:timer, name}; the content
  # is kept in the loop so classify/2 can look it up by ref.

  defp start_timer(loop, key, :infinity, _content), do: cancel_timer(loop, key)

  defp start_timer(loop, key, time, content) when is_integer(time) and time >= 0 do
    loop = cancel_timer(loop, key)
    put_timer(loop, key, {:erlang.start_timer(time, self(), key), content})
  end

  defp start_timer(_loop, key, time, _content) do
    raise ArgumentError, "bad timeout for #{inspect(key)}: #{inspect(time)}"
  end

  defp cancel_timer(loop, key) do
    case get_timer(loop, key) do
      nil ->
        loop

      {ref, _content} ->
        :erlang.cancel_timer(ref)
        # If it already fired, drop the message so it is not seen as info.
        receive do
          {:timeout, ^ref, _} -> :ok
        after
          0 -> :ok
        end

        delete_timer(loop, key)
    end
  end

  defp cancel_all_named_timers(%__MODULE__{timers: timers} = loop) do
    Enum.reduce(Map.keys(timers), loop, &cancel_timer(&2, {:timer, &1}))
  end

  defp get_timer(loop, {:timer, name}), do: Map.get(loop.timers, name)
  defp get_timer(loop, key), do: Map.fetch!(loop, key)

  defp put_timer(loop, {:timer, name}, timer),
    do: %{loop | timers: Map.put(loop.timers, name, timer)}

  defp put_timer(loop, key, timer), do: Map.put(loop, key, timer)

  defp delete_timer(loop, {:timer, name}), do: %{loop | timers: Map.delete(loop.timers, name)}
  defp delete_timer(loop, key), do: Map.put(loop, key, nil)
end
