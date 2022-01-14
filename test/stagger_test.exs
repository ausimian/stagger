defmodule StaggerTest do
  use ExUnit.Case
  doctest Stagger

  require Logger

  defmodule Consumer do
    use GenStage, restart: :transient

    def start_link(_args) do
      GenStage.start_link(__MODULE__, nil)
    end

    def wait_for(stage, seqno, timeout \\ 5000) do
      GenStage.call(stage, {:wait_for, seqno}, timeout)
    end

    @impl true
    def init(_args) do
      {:consumer, %{}}
    end

    @impl true
    def handle_call({:wait_for, seqno}, from, state) do
      if Map.has_key?(state, seqno) do
        {:reply, state[seqno], [], state}
      else
        {:noreply, [], Map.put(state, seqno, from)}
      end
    end

    @impl true
    def handle_events(events, _from, state) do
      {:noreply, [], process(events, state)}
    end

    @impl true
    def handle_cancel(_reason, _from, _state) do
      {:noreply, [], %{}}
    end

    defp process([], state), do: state
    defp process([{seqno, msg} | events], state) do
      if Map.has_key?(state, seqno) do
        GenStage.reply(state[seqno], msg)
      end
      process(events, Map.put(state, seqno, msg))
    end
  end

  @testfile "/tmp/stagger-test"

  setup do
    File.rm_rf!(@testfile)
    {:ok, producer} = Stagger.open(@testfile, hibernate_after: 1000)
    {:ok, consumer} = start_supervised(StaggerTest.Consumer)

    on_exit(fn ->
      GenStage.stop(producer)
    end)

    {:ok, producer: producer, consumer: consumer}
  end

  test "invalid file name" do
    {:error, _} = Stagger.open("")
  end

  test "one producer per file", context do
    producer = context[:producer]
    {:ok, ^producer} = Stagger.open(@testfile)
  end

  test "producer hibernates", context do
    producer = context[:producer]

    # Should cause hibernation
    Process.sleep(2000)

    {:current_function, {:erlang, :hibernate, 3}} = Process.info(producer, :current_function)
  end

  test "single consumer only", context do
    producer = context[:producer]
    consumer = context[:consumer]

    {:ok, additional} = start_supervised(StaggerTest.Consumer, id: :additional)

    {:ok, _tag} = GenStage.sync_subscribe(consumer, to: producer, ack: 0, cancel: :transient)
    {:ok, _tag} = GenStage.sync_subscribe(additional, to: producer, ack: 0, cancel: :temporary)

    :ok = Stagger.write(producer, "foo")
    "foo" = StaggerTest.Consumer.wait_for(consumer, 1)

    try do
      "foo" = StaggerTest.Consumer.wait_for(additional, 1, 1000)
    catch
      :exit, reason ->
        assert {:timeout, _call} = reason
    end
  end

  test "post events after subscription", context do
    producer = context[:producer]
    consumer = context[:consumer]

    {:ok, _tag} = GenStage.sync_subscribe(consumer, to: producer, ack: 0, cancel: :transient)

    :ok = Stagger.write(producer, "foo")
    :ok = Stagger.write(producer, "bar")
    :ok = Stagger.write(producer, "baz")

    "foo" = StaggerTest.Consumer.wait_for(consumer, 1)
    "bar" = StaggerTest.Consumer.wait_for(consumer, 2)
    "baz" = StaggerTest.Consumer.wait_for(consumer, 3)

  end

  test "post events before subscription", context do
    producer = context[:producer]
    consumer = context[:consumer]

    :ok = Stagger.write(producer, "foo")
    :ok = Stagger.write(producer, "bar")
    :ok = Stagger.write(producer, "baz")

    {:ok, _tag} = GenStage.sync_subscribe(consumer, to: producer, ack: 0, cancel: :transient)

    "foo" = StaggerTest.Consumer.wait_for(consumer, 1)
    "bar" = StaggerTest.Consumer.wait_for(consumer, 2)
    "baz" = StaggerTest.Consumer.wait_for(consumer, 3)
  end

  test "post events across subscription", context do
    producer = context[:producer]
    consumer = context[:consumer]

    :ok = Stagger.write(producer, "foo")
    :ok = Stagger.write(producer, "bar")

    {:ok, _tag} = GenStage.sync_subscribe(consumer, to: producer, ack: 0, cancel: :transient)
    "foo" = StaggerTest.Consumer.wait_for(consumer, 1)

    :ok = Stagger.write(producer, "baz")

    "bar" = StaggerTest.Consumer.wait_for(consumer, 2)
    "baz" = StaggerTest.Consumer.wait_for(consumer, 3)
  end

  test "post events across subscriptions", context do
    producer = context[:producer]
    consumer = context[:consumer]

    {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, ack: 0, cancel: :transient)

    :ok = Stagger.write(producer, "foo")
    :ok = Stagger.write(producer, "bar")

    "foo" = StaggerTest.Consumer.wait_for(consumer, 1)
    "bar" = StaggerTest.Consumer.wait_for(consumer, 2)

    GenStage.cancel({producer, ref}, :normal)

    {:ok, _ref} = GenStage.sync_subscribe(consumer, to: producer, ack: 1, cancel: :transient)
    "bar" = StaggerTest.Consumer.wait_for(consumer, 2)

    :ok = Stagger.write(producer, "baz")
    "baz" = StaggerTest.Consumer.wait_for(consumer, 3)
  end

  @tag :missing
  test "missing events", context do
    producer = context[:producer]
    consumer = context[:consumer]

    :ok = Stagger.write(producer, "foo")
    :ok = Stagger.write(producer, "bar")
    :ok = Stagger.write(producer, "baz")

    {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, ack: 2, purge: 2, cancel: :transient)
    "baz" = StaggerTest.Consumer.wait_for(consumer, 3)

    {:ok, _ref} = GenStage.sync_resubscribe(consumer, ref, :normal, to: producer, ack: 1, cancel: :transient)
    "baz" = StaggerTest.Consumer.wait_for(consumer, 3)
  end

  test "producer ignores spurious down messages", context do
    producer = context[:producer]
    consumer = context[:consumer]

    send(producer, {:DOWN, make_ref(), :process, self(), :test})

    GenStage.sync_subscribe(consumer, to: producer, ack: 0, cancel: :transient)

    :ok = Stagger.write(producer, "foo")
    "foo" = StaggerTest.Consumer.wait_for(consumer, 1)
  end

  test "mini-stress", context do
    producer = context[:producer]
    consumer = context[:consumer]

    {:ok, _ref} = GenStage.sync_subscribe(consumer, to: producer, ack: 0, cancel: :transient)
    blobs = for _ <- 1..10_000, do: :crypto.strong_rand_bytes(23_456)
    blobs
    |> Enum.each(fn blob -> Stagger.write(producer, blob) end)

    blobs
    |> Enum.with_index
    |> Enum.all?(fn {blob, index} -> blob == StaggerTest.Consumer.wait_for(consumer, index + 1) end)
  end
end
