defmodule StaggerProps do

  require Logger

  #
  # This is stateful property test that uses a Stagger.MsgQueue process and a consumer
  # in tandem to confirm the expected behavior of the former. The set of commands
  # that are generated are used to drive both processes.
  #

  defmodule Subscriber do
    use GenStage

    #
    # The subscriber api deliberately has a number of blocking calls to make
    # test construction easier. For example, the subscribe call will block until
    # confirmation of the subscription has been received. The others follow a
    # similar pattern.
    #

    def start_link(args),              do: GenStage.start_link(__MODULE__, args)
    def subscribe(consumer, producer), do: GenStage.call(consumer, {:subscribe, producer})
    def unsubscribe(consumer),         do: GenStage.call(consumer, :unsubscribe)
    def resubscribe(consumer),         do: GenStage.call(consumer, :resubscribe)
    def ask(subscriber, demand),       do: GenStage.call(subscriber, {:ask, demand})
    def get_rcvd(subscriber, upto),    do: GenStage.call(subscriber, {:get_rcvd, upto})

    @impl true
    def init(args) do
      ack = args[:ack] || 0
      {:consumer, %{subscription: nil, ack: ack, caller: nil, rcvd: %{}, upto: nil}}
    end

    @impl true
    def handle_subscribe(:producer, _opts, from, %{caller: caller} = state) when not is_nil(caller) do
      GenStage.reply(caller, :ok)
      {:manual, %{state | subscription: from, caller: nil}}
    end

    @impl true
    def handle_cancel({_, :resubscribe}, from, %{subscription: from} = state) do
      {:noreply, [], %{state | subscription: nil}}
    end
    def handle_cancel({_, :resubscribe}, _from, %{} = state) do
      {:noreply, [], state}
    end
    def handle_cancel(_reason, from, %{subscription: from, caller: caller} = state) do
      unless is_nil(caller) do
        GenStage.reply(caller, :ok)
      end
      {:noreply, [], %{state | subscription: nil, caller: nil}}
    end

    @impl true
    def handle_call({:subscribe, producer}, from, %{subscription: nil, ack: ack, caller: nil} = state) do
      :ok = GenStage.async_subscribe(self(), to: producer, cancel: :temporary, ack: ack, purge: ack)
      {:noreply, [], %{state | caller: from}}
    end
    def handle_call(:unsubscribe, from, %{subscription: sub, caller: nil} = state) when not is_nil(sub) do
      :ok = GenStage.cancel(sub, :normal)
      {:noreply, [], %{state | caller: from}}
    end
    def handle_call(:resubscribe, from, %{subscription: {pid, tag}, ack: ack, caller: nil} = state) do
      :ok = GenStage.async_resubscribe(self(), tag, :resubscribe, to: pid, cancel: :temporary, ack: ack, purge: ack)
      {:noreply, [], %{state | caller: from}}
    end
    def handle_call({:ask, demand}, _from, %{subscription: sub} = state) do
      reply = GenStage.ask(sub, demand)
      {:reply, reply, [], state}
    end
    def handle_call({:get_rcvd, upto}, _from, %{rcvd: rcvd} = state) when is_map_key(rcvd, upto) do
      {:reply, copy_rcvd(rcvd, upto), [], state}
    end
    def handle_call({:get_rcvd, upto}, from, state) do
      {:noreply, [], %{state | upto: {from, upto}}}
    end

    @impl true
    def handle_events([], _from, state) do
      {:noreply, [], try_copy(state)}
    end
    def handle_events([{seqno, msg} | msgs], from, %{rcvd: rcvd, ack: ack} = state) do
      handle_events(msgs, from, %{state | rcvd: Map.put(rcvd, seqno, msg), ack: max(seqno, ack)})
    end

    defp try_copy(%{rcvd: rcvd, upto: {from, upto}} = state) when is_map_key(rcvd, upto) do
      :ok = GenServer.reply(from, copy_rcvd(rcvd, upto))
      %{state | upto: nil}
    end
    defp try_copy(state), do: state


    defp copy_rcvd(rcvd, upto, acc \\ %{})
    defp copy_rcvd(rcvd, upto, acc) when is_map_key(rcvd, upto) do
      copy_rcvd(rcvd, upto - 1, Map.put(acc, upto, Map.fetch!(rcvd, upto)))
    end
    defp copy_rcvd(_rcvd, _upto, acc), do: acc

  end

  use ExUnit.Case
  use PropCheck
  use PropCheck.StateM

  alias Stagger.MsgQueue, as: Producer
  alias StaggerProps.Subscriber, as: Consumer

  defstruct msgs: %{}, demand: 0, rd_seqno: 0, wr_seqno: 0, subscribed: false
  alias __MODULE__, as: Model

  @filepath "/tmp/staggerprops"

  property "run the msg queue", [:verbose, numtests: 1000] do
    forall cmds <- commands(__MODULE__, %Model{}) do
      {:ok, _} = File.rm_rf(@filepath)
      {:ok, producer} = Producer.start_link(%{path: @filepath})
      {:ok, consumer} = start_supervised(Consumer)
      {history, model, result} = run_commands(Model, cmds, producer: producer, consumer: consumer)
      :ok = stop_supervised(Consumer)
      :ok = GenServer.stop(producer)

      (result == :ok)
      |> aggregate(command_names(cmds))
      |> when_fail(IO.puts("""
         History: #{inspect(history)}
         Model: #{inspect(model)}
         Result: #{inspect(result)}
         """))
    end
  end

  def command(%{wr_seqno: wr_seqno, rd_seqno: rd_seqno, demand: demand}) do
    oneof([
      {:call, Consumer, :subscribe,   [{:var, :consumer}, {:var, :producer}]},
      {:call, Consumer, :unsubscribe, [{:var, :consumer}]},
      {:call, Consumer, :resubscribe, [{:var, :consumer}]},
      {:call, Producer, :write, [{:var, :producer}, term(), 5000]},
      {:call, Consumer, :ask, [{:var, :consumer}, (such_that n <- pos_integer(), when: n < 10)]},
      {:call, Consumer, :get_rcvd, [{:var, :consumer}, min(wr_seqno, rd_seqno + demand)]}
    ])
  end

  def precondition(%Model{subscribed: subscribed}, {:call, Consumer, :subscribe, _args}), do: not subscribed
  def precondition(%Model{subscribed: subscribed}, {:call, Consumer, :unsubscribe, _args}), do: subscribed
  def precondition(%Model{subscribed: subscribed}, {:call, Consumer, :resubscribe, _args}), do: subscribed
  def precondition(%Model{}, {:call, Producer, :write, _args}), do: true
  def precondition(%Model{subscribed: true, demand: 0}, {:call, Consumer, :ask, _args}), do: true
  def precondition(%Model{subscribed: true}, {:call, Consumer, :get_rcvd, [_, upto]}) do
    upto > 0
  end
  def precondition(_model, _call), do: false

  def postcondition(%Model{subscribed: subscribed}, {:call, Consumer, :subscribe, _args}, result) do
    (not subscribed) && result == :ok
  end
  def postcondition(%Model{subscribed: subscribed}, {:call, Consumer, :unsubscribe, _args}, result) do
    subscribed && (result == :ok)
  end
  def postcondition(%Model{subscribed: subscribed}, {:call, Consumer, :resubscribe, _args}, result) do
    subscribed && (result == :ok)
  end
  def postcondition(%Model{}, {:call, Consumer, :ask, _args}, result) do
    result == :ok
  end
  def postcondition(%Model{}, {:call, Producer, :write, _args}, result) do
    result == :ok
  end
  def postcondition(%Model{msgs: msgs, rd_seqno: prev}, {:call, Consumer, :get_rcvd, [_, upto]}, result) do
    (prev + 1)..upto // 1 |> Enum.all?(fn seqno -> Map.has_key?(result, seqno) && Map.fetch!(result, seqno) == Map.fetch!(msgs, seqno) end)
  end

  def next_state(%Model{} = model, _result, {:call, Consumer, :subscribe, _args}) do
    %Model{model | subscribed: true}
  end
  def next_state(%Model{} = model, _result, {:call, Consumer, :unsubscribe, _args}) do
    %Model{model | subscribed: false}
  end
  def next_state(%Model{} = model, _result, {:call, Consumer, :resubscribe, _args}) do
    %Model{model | demand: 0}
  end
  def next_state(%Model{} = model, _result, {:call, Consumer, :ask, [_consumer, demand]}) do
    %Model{model | demand: demand}
  end
  def next_state(%Model{msgs: msgs, wr_seqno: seqno} = model, _result, {:call, Producer, :write, [_, term, _]}) do
    next = seqno + 1
    %Model{model | wr_seqno: next, msgs: Map.put(msgs, next, term)}
  end
  def next_state(%Model{rd_seqno: rd_seqno, demand: demand} = model, _result, {:call, Consumer, :get_rcvd, [_, upto]}) do
    %Model{model | rd_seqno: max(rd_seqno, upto), demand: demand - (max(0, upto - rd_seqno))}
  end

  def initial_state, do: %Model{}
end
