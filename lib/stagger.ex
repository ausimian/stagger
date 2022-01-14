defmodule Stagger do
  @moduledoc ~S"""
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

  ## Producers

  Upstream clients must first open their message-queue, via `open/1`, and then use the
  resulting process to enqueue writes, via `write/2`.

      {:ok, pid} = Stagger.open("/path/to/msg/queue")
      ...
      :ok = Stagger.write(pid, "foo")
      :ok = Stagger.write(pid, "bar")
      :ok = Stagger.write(pid, "baz")

  The process created via `open/1` is the GenStage MsgQueue - by writing entries to it,
  it will satisfy demand from a downstream consumer.

  ## Consumers

  Downstream clients are GenStage consumers. They must also open the message-queue, via
  `open/1` and then subscribe use existing GenStage subscription facilities:

      def init(args) do
        {:ok, pid} = Stagger.open("/path/to/msg/queue")
        {:ok, sub} = GenStage.sync_subscribe(self(), to: pid, ack: last_processed())
        ...
      end

      def handle_events(events, _from, stage) do
        ...
      end

  ## Sequence numbers

  Sequence numbers are used to control the events seen by a subscriber. Every event
  delivered to a consumer is a 2-tuple of `{seqno, msg}` and it is the consumer's
  responsibility to successfully record this sequence number as having been
  processed.

  A consumer must indicate its last-processed sequence number by passing `ack: N` in
  the subscription options (pass `ack: 0` when no such number exists) whenever it
  (re)subscribes. Event delivery will resume from the Nth + 1 event.

  Every message _written_ to the message-queue is assigned an incrementing sequence number
  by the Stagger process. When an existing message queue is re-opened, the process will
  first recover the last written number, using that as the base for any subsequent writes.

  ## Purging

  In order to prevent unconstrained growth of the message-queue file, a consumer may
  periodically purge the queue of old entries by passing a `purge: N` option when it
  (re)subscribes e.g:

      last = last_processed()
      {:ok, sub} = GenStage.sync_subscribe(self(), to: pid, ack: last, purge: last)

  All entries _up to and including_ N are removed from the head of message-queue file.
  The value of N will be capped to no more than the value of the last ack'd message.

  To summarize:

   - `ack: N` determines that the next delivered message will have a seqno of N + 1
   - `purge: M` is a hint to the producer to remove messages 1..M from the head of
     the message queue.

  ## Why not RabbitMQ?

  If you think you need something like RabbitMQ, you probably do :-). Stagger is
  intended to be a lightweight durable message queue with minimal dependencies.
  """

  @doc """
  Open a message-queue file, returning the pid responsible for managing it.

  The resulting pid can be used by upstream clients to enqueue messages via `write/2`, or
  may be used as the target of a GenStage subscribe operation.

  The following option may be passed to the function:

  * `hibernate_after: N` - After a period of _N_ milliseconds, the returned process will
     hibernate. If unspecified, defaults to 15 seconds. Pass `hibernate_after: :infinite`
     to inhibit this behaviour. This option only takes effect if the process managing the
     queue is created by the call to `open/2`.

  """
  @spec open(binary, Keyword.t) :: :ignore | {:error, any} | {:ok, any} | {:ok, pid, any}
  def open(path, opts \\ []) when is_binary(path) do
    args = Map.new(opts) |> Map.put(:path, path)
    case DynamicSupervisor.start_child(Stagger.MsgQueueSupervisor, {Stagger.MsgQueue, args}) do
      {:ok, pid} ->
        {:ok, pid}
      {:error, {:already_started, pid}} ->
        {:ok, pid}
      error ->
        error
    end
  end

  @doc """
  Write a term to the message-queue.
  """
  @spec write(pid :: atom | pid | {atom, any} | {:via, atom, any}, term :: any, timeout :: :infinite | non_neg_integer()) :: :ok | {:error, any}
  def write(pid, term, timeout \\ 5000) do
    Stagger.MsgQueue.write(pid, term, timeout)
  end

end
