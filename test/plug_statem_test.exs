defmodule PlugStatemTest do
  use ExUnit.Case, async: true

  # A small machine. `conn` is a list used as a log of "writes", to stand in
  # for a Plug.Conn. `data` counts events.
  defmodule Machine do
    @behaviour PlugStatem

    def callback_mode, do: :handle_event_function

    def handle_event(:info, {:write, x}, _state, conn, n),
      do: {:keep_state, [x | conn], n + 1}

    def handle_event(:info, {:sleep, ms}, _state, _conn, _n) do
      Process.sleep(ms)
      :keep_state_and_data
    end

    def handle_event(:info, {:goto, state, actions}, _old, conn, n),
      do: {:next_state, state, conn, n + 1, actions}

    def handle_event(:info, :ignore, _state, _conn, _n),
      do: :keep_state_and_data

    def handle_event(:info, {:stop, reason}, _state, conn, n),
      do: {:stop, reason, conn, n + 1}

    def handle_event(:state_timeout, content, state, conn, n),
      do: {:next_state, {:timed_out, state, content}, conn, n + 1}

    def handle_event(:timeout, content, _state, conn, n),
      do: {:stop, {:idle, content}, conn, n + 1}

    def handle_event(:internal, x, _state, conn, n),
      do: {:keep_state, [{:internal, x} | conn], n + 1}

    def terminate(reason, _state, _conn, _n), do: send(self(), {:terminated, reason})
  end

  defmodule StateFns do
    @behaviour PlugStatem

    def callback_mode, do: :state_functions

    def a(:info, :next, conn, data), do: {:next_state, :b, conn, data}
    def b(:info, :next, conn, data), do: {:stop, :done, [:b | conn], data}
  end

  test "handles info, keeps and changes state, returns on stop" do
    send(self(), {:write, 1})
    send(self(), :ignore)
    send(self(), {:goto, :second, []})
    send(self(), {:write, 2})
    send(self(), {:stop, :normal})

    assert {:normal, :second, [2, 1], 4} = PlugStatem.enter_loop(Machine, [], [], :first, 0)
    assert_received {:terminated, :normal}
  end

  test "state_timeout fires and carries its content" do
    send(self(), {:goto, :waiting, [{:state_timeout, 10, :tick}]})
    Process.send_after(self(), {:stop, :normal}, 100)

    assert {:normal, {:timed_out, :waiting, :tick}, [], 3} =
             PlugStatem.enter_loop(Machine, [], [], :first, 0)
  end

  test "state_timeout is cancelled by a state change and kept by keep_state" do
    send(self(), {:goto, :a, [{:state_timeout, 10, :tick}]})
    send(self(), {:write, :x})
    send(self(), {:goto, :b, []})
    Process.send_after(self(), {:stop, :normal}, 100)

    assert {:normal, :b, [:x], 4} = PlugStatem.enter_loop(Machine, [], [], :first, 0)
  end

  test "event timeout fires when nothing arrives" do
    assert {{:idle, :bored}, :first, [], 1} =
             PlugStatem.enter_loop(Machine, [], [], :first, 0, {:timeout, 10, :bored})
  end

  test "event timeout is cancelled by any event" do
    send(self(), {:write, 1})
    Process.send_after(self(), {:stop, :normal}, 100)

    assert {:normal, :first, [1], 2} =
             PlugStatem.enter_loop(Machine, [], [], :first, 0, {:timeout, 10, :bored})
  end

  test "next_event is handled before mailbox messages" do
    send(self(), {:write, :from_mailbox})
    send(self(), {:stop, :normal})

    actions = [{:next_event, :internal, 1}, {:next_event, :internal, 2}]

    assert {:normal, :first, [:from_mailbox, {:internal, 2}, {:internal, 1}], 4} =
             PlugStatem.enter_loop(Machine, [], [], :first, 0, actions)
  end

  test "a timer that fired before being cancelled is flushed, not delivered as info" do
    # Start a 0ms state timeout, then sleep inside the loop so the timer
    # message is in the mailbox before the next event. That event changes
    # state, which cancels the timer and must also drop the stale message.
    send(self(), {:goto, :a, [{:state_timeout, 0, :tick}]})
    send(self(), {:sleep, 5})
    send(self(), {:goto, :b, []})
    send(self(), {:stop, :normal})

    assert {:normal, :b, [], 3} = PlugStatem.enter_loop(Machine, [], [], :first, 0)
    refute_received {:timeout, _ref, :tick}
  end

  test "state_functions mode dispatches on the state atom" do
    send(self(), :next)
    send(self(), :next)
    assert {:done, :b, [:b], :d} = PlugStatem.enter_loop(StateFns, [], [], :a, :d)
  end

  test "callback exceptions propagate" do
    send(self(), {:goto, :whatever, [{:bogus_action}]})

    assert_raise ArgumentError, ~r/bad action/, fn ->
      PlugStatem.enter_loop(Machine, [], [], :first, 0)
    end
  end

  test "unimplemented gen_statem actions raise naming the action" do
    for action <- [
          {:state_timeout, :update, :x},
          {:timeout, :update, :x},
          {{:timeout, :n}, :update, :x},
          {:state_timeout, 10, :x, [abs: true]},
          {:timeout, 10, :x, [abs: true]},
          {{:timeout, :n}, 10, :x, [abs: true]},
          {:change_callback_module, Machine},
          {:push_callback_module, Machine},
          :pop_callback_module
        ] do
      send(self(), {:goto, :whatever, [action]})

      assert_raise ArgumentError, ~r/not implemented by PlugStatem/, fn ->
        PlugStatem.enter_loop(Machine, [], [], :first, 0)
      end
    end
  end

  test "a non-integer timeout is a bad action" do
    send(self(), {:goto, :whatever, [{:timeout, :soon, :x}]})

    assert_raise ArgumentError, ~r/bad timeout/, fn ->
      PlugStatem.enter_loop(Machine, [], [], :first, 0)
    end
  end

  test "hibernate is rejected" do
    send(self(), {:goto, :whatever, [:hibernate]})

    assert_raise ArgumentError, ~r/hibernate/, fn ->
      PlugStatem.enter_loop(Machine, [], [], :first, 0)
    end
  end

  test "Bandit's reserved messages are left in the mailbox, in order" do
    send(self(), {:bandit, {:send_window_update, 10}})
    send(self(), {:write, 1})
    send(self(), {:plug_conn, :sent})
    send(self(), {:stop, :normal})

    # Machine has no clause for the reserved shapes, so dispatching either
    # would raise.
    assert {:normal, :first, [1], 2} = PlugStatem.enter_loop(Machine, [], [], :first, 0)

    # terminate/4 sends {:terminated, reason} to self, which lands after these.
    assert {:messages, [{:bandit, {:send_window_update, 10}}, {:plug_conn, :sent} | _]} =
             Process.info(self(), :messages)
  end

  ## call / cast

  defmodule Server do
    @behaviour PlugStatem
    def callback_mode, do: :handle_event_function

    def handle_event({:call, from}, {:add, x}, _state, conn, n),
      do: {:keep_state, conn, n + x, [{:reply, from, n + x}]}

    def handle_event({:call, from}, :quit, _state, _conn, _n),
      do: {:stop_and_reply, :normal, {:reply, from, :bye}}

    def handle_event(:cast, {:add, x}, _state, conn, n),
      do: {:keep_state, conn, n + x}
  end

  test "call events reply through the reply action and stop_and_reply" do
    me = self()

    # Results are collected and sent once the loop has stopped, otherwise
    # they would arrive as :info events while the loop is running.
    spawn_link(fn ->
      r1 = :gen_statem.call(me, {:add, 2})
      :gen_statem.cast(me, {:add, 10})
      r2 = :gen_statem.call(me, {:add, 3})
      r3 = :gen_statem.call(me, :quit)
      send(me, {:results, r1, r2, r3})
    end)

    assert {:normal, :s, [], 15} = PlugStatem.enter_loop(Server, [], [], :s, 0)
    assert_receive {:results, 2, 15, :bye}
  end

  ## postpone

  defmodule Postponer do
    @behaviour PlugStatem
    def callback_mode, do: :handle_event_function

    # In :closed, jobs are postponed. In :open, they are logged.
    def handle_event(:info, {:job, _}, :closed, _conn, _log),
      do: {:keep_state_and_data, :postpone}

    def handle_event(:info, {:job, j}, :open, conn, log), do: {:keep_state, conn, [j | log]}

    def handle_event(:info, :open, :closed, conn, log),
      do: {:next_state, :open, conn, log, {:next_event, :internal, :first}}

    def handle_event(:internal, x, _state, conn, log), do: {:keep_state, conn, [x | log]}

    def handle_event(:info, :stop, _state, conn, log),
      do: {:stop, :normal, conn, Enum.reverse(log)}
  end

  test "postponed events are retried in order after a state change, after next_events" do
    send(self(), {:job, 1})
    send(self(), {:job, 2})
    send(self(), :open)
    send(self(), {:job, 3})
    send(self(), :stop)

    assert {:normal, :open, [], [:first, 1, 2, 3]} =
             PlugStatem.enter_loop(Postponer, [], [], :closed, [])
  end

  test "postpone is rejected in a state enter call" do
    defmodule BadEnter do
      @behaviour PlugStatem
      def callback_mode, do: [:handle_event_function, :state_enter]
      def handle_event(:enter, _old, _state, _conn, _data), do: {:keep_state_and_data, :postpone}
    end

    assert_raise ArgumentError, ~r/postpone is not allowed/, fn ->
      PlugStatem.enter_loop(BadEnter, [], [], :s, nil)
    end
  end

  ## named timers

  defmodule Named do
    @behaviour PlugStatem
    def callback_mode, do: :handle_event_function

    def handle_event(:info, {:start, name, ms}, _state, _conn, _log),
      do: {:keep_state_and_data, {{:timeout, name}, ms, {:fired, name}}}

    def handle_event(:info, {:cancel, name}, _state, _conn, _log),
      do: {:keep_state_and_data, {{:timeout, name}, :cancel}}

    def handle_event(:info, {:sleep, ms}, _state, _conn, _log) do
      Process.sleep(ms)
      :keep_state_and_data
    end

    def handle_event(:info, {:goto, s}, _state, conn, log), do: {:next_state, s, conn, log}

    def handle_event({:timeout, name}, content, _state, conn, log),
      do: {:keep_state, conn, [{name, content} | log]}

    def handle_event(:info, :stop, _state, conn, log),
      do: {:stop, :normal, conn, Enum.reverse(log)}
  end

  test "named timers fire with their name, survive state changes, and can be cancelled" do
    send(self(), {:start, :a, 10})
    send(self(), {:start, :b, 10})
    send(self(), {:cancel, :b})
    send(self(), {:goto, :elsewhere})
    Process.send_after(self(), :stop, 100)

    assert {:normal, :elsewhere, [], [a: {:fired, :a}]} =
             PlugStatem.enter_loop(Named, [], [], :s, [])
  end

  ## state enter calls

  defmodule Enterer do
    @behaviour PlugStatem
    def callback_mode, do: [:state_functions, :state_enter]

    # Initial enter call has old == new.
    def idle(:enter, old, conn, log), do: {:keep_state, conn, [{:enter, :idle, old} | log]}
    def idle(:info, :go, conn, log), do: {:next_state, :busy, conn, log}

    # Enter call may set a state timeout and use next_state to the same state.
    def busy(:enter, old, conn, log),
      do: {:next_state, :busy, conn, [{:enter, :busy, old} | log], {:state_timeout, 10, :done}}

    def busy(:info, :again, conn, log), do: {:repeat_state, conn, [:again | log]}
    def busy(:state_timeout, :done, conn, log), do: {:stop, :normal, conn, Enum.reverse(log)}
  end

  test "state enter calls run at start, on change, and on repeat_state" do
    send(self(), :go)
    send(self(), :again)

    assert {:normal, :busy, [], log} = PlugStatem.enter_loop(Enterer, [], [], :idle, [])

    assert log == [
             {:enter, :idle, :idle},
             {:enter, :busy, :idle},
             :again,
             {:enter, :busy, :busy}
           ]
  end

  test "a state enter call may not change state" do
    defmodule Escaper do
      @behaviour PlugStatem
      def callback_mode, do: [:handle_event_function, :state_enter]
      def handle_event(:enter, _old, :a, conn, data), do: {:next_state, :b, conn, data}
    end

    assert_raise ArgumentError, ~r/may not change state/, fn ->
      PlugStatem.enter_loop(Escaper, [], [], :a, nil)
    end
  end

  test "bad return values raise" do
    defmodule BadReturn do
      @behaviour PlugStatem
      def callback_mode, do: :handle_event_function
      def handle_event(:info, _msg, _state, _conn, _data), do: :nonsense
    end

    send(self(), :x)

    assert_raise ArgumentError, ~r/bad return value/, fn ->
      PlugStatem.enter_loop(BadReturn, [], [], :s, nil)
    end
  end

  ## name registration

  defmodule Lookup do
    @behaviour PlugStatem
    def callback_mode, do: :handle_event_function

    def handle_event(:info, {:lookup, name}, _state, conn, _data),
      do: {:keep_state, conn, GenServer.whereis(name)}

    def handle_event(:info, :stop, _state, conn, data), do: {:stop, :normal, conn, data}
    def handle_event(:info, :boom, _state, _conn, _data), do: raise("boom")
  end

  test "name: via registers for the lifetime of the loop and unregisters after" do
    start_supervised!({Registry, keys: :unique, name: PlugStatemTest.Registry})
    name = {:via, Registry, {PlugStatemTest.Registry, :stream}}
    me = self()

    send(self(), {:lookup, name})
    send(self(), :stop)

    assert {:normal, :s, [], ^me} = PlugStatem.enter_loop(Lookup, [name: name], [], :s, nil)
    assert GenServer.whereis(name) == nil
  end

  test "name: atom registers and unregisters, even when a callback raises" do
    send(self(), :boom)

    assert_raise RuntimeError, "boom", fn ->
      PlugStatem.enter_loop(Lookup, [name: :plug_statem_test_name], [], :s, nil)
    end

    assert Process.whereis(:plug_statem_test_name) == nil
  end

  test "name already registered returns an error without running the loop" do
    start_supervised!({Registry, keys: :unique, name: PlugStatemTest.Registry2})
    name = {:via, Registry, {PlugStatemTest.Registry2, :stream}}
    me = self()

    holder =
      spawn_link(fn ->
        {:ok, _} = Registry.register(PlugStatemTest.Registry2, :stream, nil)
        send(me, :registered)

        receive do
          :release -> :ok
        end
      end)

    assert_receive :registered
    send(self(), :stop)

    assert {:error, {:already_started, ^holder}} =
             PlugStatem.enter_loop(Lookup, [name: name], [], :s, nil)

    # The loop never ran, so :stop is still in the mailbox.
    assert_received :stop
    send(holder, :release)
  end

  ## timers, remaining forms

  test ":infinity and :cancel stop a timer set in the same action list" do
    send(self(), {:goto, :a, [{:state_timeout, 0, :tick}, {:state_timeout, :infinity, :tick}]})
    send(self(), {:goto, :a, [{:state_timeout, 0, :tick}, {:state_timeout, :cancel}]})
    send(self(), {:goto, :a, [{:timeout, 0, :bored}, {:timeout, :cancel}]})
    send(self(), {:goto, :a, [{:timeout, 0, :bored}, {:timeout, :infinity, :bored}]})
    send(self(), {:sleep, 5})
    send(self(), {:stop, :normal})

    # Had any timer fired, the state would be {:timed_out, ...} or the loop
    # would have stopped with {:idle, :bored}.
    assert {:normal, :a, [], 5} = PlugStatem.enter_loop(Machine, [], [], :first, 0)
    refute_received {:timeout, _ref, _key}
  end

  test "setting a timer of the same kind replaces it" do
    send(self(), {:goto, :a, [{:state_timeout, 0, :first}]})
    send(self(), {:goto, :a, [{:state_timeout, 0, :second}]})
    send(self(), {:sleep, 5})
    Process.send_after(self(), {:stop, :normal}, 50)

    assert {:normal, {:timed_out, :a, :second}, [], 4} =
             PlugStatem.enter_loop(Machine, [], [], :first, 0)
  end

  test "stopping cancels named timers and flushes their messages" do
    send(self(), {:start, :z, 0})
    send(self(), {:sleep, 5})
    send(self(), :stop)

    assert {:normal, :s, [], []} = PlugStatem.enter_loop(Named, [], [], :s, [])
    refute_received {:timeout, _ref, {:timer, :z}}
  end

  ## remaining result shapes

  defmodule Stopper do
    @behaviour PlugStatem
    def callback_mode, do: :handle_event_function

    def handle_event(:info, :stop2, _state, _conn, _data), do: {:stop, :two}

    def handle_event({:call, from}, :stop5, _state, _conn, _data),
      do: {:stop_and_reply, :five, [{:reply, from, :bye}], :new_conn, :new_data}
  end

  test "{:stop, reason} keeps the current conn and data" do
    send(self(), :stop2)
    assert {:two, :s, :conn, :data} = PlugStatem.enter_loop(Stopper, [], :conn, :s, :data)
  end

  test "stop_and_reply with conn and data replies, then returns the new conn and data" do
    me = self()
    spawn_link(fn -> send(me, {:result, :gen_statem.call(me, :stop5)}) end)

    assert {:five, :s, :new_conn, :new_data} =
             PlugStatem.enter_loop(Stopper, [], :conn, :s, :data)

    assert_receive {:result, :bye}
  end

  ## enter calls, remaining paths

  defmodule Repeater do
    @behaviour PlugStatem
    def callback_mode, do: [:handle_event_function, :state_enter]

    # The enter call repeats itself until data reaches 3, logging each entry.
    def handle_event(:enter, old, :s, conn, n) when n < 3,
      do: {:repeat_state, [{:enter, old, n} | conn], n + 1}

    def handle_event(:enter, old, :s, conn, n), do: {:keep_state, [{:enter, old, n} | conn], n}
    def handle_event(:info, :repeat_and_data, :s, _conn, _n), do: :repeat_state_and_data
    def handle_event(:info, :stop, :s, conn, n), do: {:stop, :normal, conn, n}
  end

  test "repeat_state from an enter call runs the enter call again" do
    send(self(), :stop)

    assert {:normal, :s, log, 3} = PlugStatem.enter_loop(Repeater, [], [], :s, 0)

    assert Enum.reverse(log) == [
             {:enter, :s, 0},
             {:enter, :s, 1},
             {:enter, :s, 2},
             {:enter, :s, 3}
           ]
  end

  test "repeat_state_and_data from an event runs the enter call with unchanged data" do
    send(self(), :repeat_and_data)
    send(self(), :stop)

    assert {:normal, :s, [{:enter, :s, 3} | _] = log, 3} =
             PlugStatem.enter_loop(Repeater, [], [], :s, 0)

    assert length(log) == 5
  end

  test "an initial enter call that stops returns without receiving" do
    defmodule EnterStop do
      @behaviour PlugStatem
      def callback_mode, do: [:handle_event_function, :state_enter]
      def handle_event(:enter, _old, :s, conn, data), do: {:stop, :early, conn, data}
    end

    send(self(), :untouched)

    assert {:early, :s, :c, :d} = PlugStatem.enter_loop(EnterStop, [], :c, :s, :d)
    assert_received :untouched
  end

  test "next_event is rejected in a state enter call" do
    defmodule BadEnterNext do
      @behaviour PlugStatem
      def callback_mode, do: [:handle_event_function, :state_enter]

      def handle_event(:enter, _old, _state, _conn, _data),
        do: {:keep_state_and_data, {:next_event, :internal, :x}}
    end

    assert_raise ArgumentError, ~r/next_event is not allowed/, fn ->
      PlugStatem.enter_loop(BadEnterNext, [], [], :s, nil)
    end
  end

  ## argument validation

  test "callback_mode without a dispatch mode raises" do
    defmodule NoMode do
      @behaviour PlugStatem
      def callback_mode, do: [:state_enter]
    end

    assert_raise ArgumentError, ~r/callback_mode must include/, fn ->
      PlugStatem.enter_loop(NoMode, [], [], :s, nil)
    end
  end

  test "state_functions mode requires an atom state" do
    send(self(), :x)

    assert_raise ArgumentError, ~r/state must be an atom/, fn ->
      PlugStatem.enter_loop(StateFns, [], [], {:not, :atom}, nil)
    end
  end

  test "an exception in terminate/4 propagates" do
    defmodule BadTerminate do
      @behaviour PlugStatem
      def callback_mode, do: :handle_event_function
      def handle_event(:info, :stop, _state, conn, data), do: {:stop, :normal, conn, data}
      def terminate(_reason, _state, _conn, _data), do: raise("terminate boom")
    end

    send(self(), :stop)

    assert_raise RuntimeError, "terminate boom", fn ->
      PlugStatem.enter_loop(BadTerminate, [], [], :s, nil)
    end
  end

  ## postpone, remaining paths

  defmodule CallPostponer do
    @behaviour PlugStatem
    def callback_mode, do: :handle_event_function

    def handle_event({:call, _from}, :ask, :closed, _conn, _data),
      do: {:keep_state_and_data, :postpone}

    def handle_event(:info, :noop, :closed, _conn, _data),
      do: {:keep_state_and_data, {:postpone, false}}

    def handle_event(:info, :open, :closed, conn, data), do: {:next_state, :open, conn, data}

    def handle_event({:call, from}, :ask, :open, conn, data),
      do: {:keep_state, conn, data, {:reply, from, :answer}}

    def handle_event(:info, :stop, _state, conn, data), do: {:stop, :normal, conn, data}
  end

  test "a postponed call is answered after the state change, and {:postpone, false} is a no-op" do
    me = self()
    spawn_link(fn -> send(me, {:result, :gen_statem.call(me, :ask)}) end)

    # The call has to be in the mailbox ahead of :open.
    wait_for_call = fn wait ->
      case Process.info(self(), :messages) do
        {:messages, [{:"$gen_call", _, :ask} | _]} -> :ok
        _ -> Process.sleep(1) && wait.(wait)
      end
    end

    wait_for_call.(wait_for_call)
    send(self(), :noop)
    send(self(), :open)
    send(self(), :stop)

    assert {:normal, :open, [], nil} = PlugStatem.enter_loop(CallPostponer, [], [], :closed, nil)
    assert_receive {:result, :answer}
  end

  ## name registration, remaining form

  test "name: {:global, term} registers and unregisters" do
    name = {:global, {__MODULE__, make_ref()}}
    me = self()

    send(self(), {:lookup, name})
    send(self(), :stop)

    assert {:normal, :s, [], ^me} = PlugStatem.enter_loop(Lookup, [name: name], [], :s, nil)
    assert GenServer.whereis(name) == nil
  end
end
