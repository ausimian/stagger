defmodule Stagger.MsgQueue do
  @moduledoc false
  use GenServer, restart: :transient

  require Logger

  defstruct [
    path:       nil,       # Path to the file
    wr_file:    nil,       # File object used to write new messages
    wr_seqno:   0,         # The most recent written seqno
    subscriber: nil,       # The current subscriber
    monitor:    nil,       # The monitor on the current subscriber
    rd_file:    nil,       # File object used to read messages
    rd_seqno:   0,         # The most recent read seqno
    demand:     0,         # The current demand,
    timeout:    :infinite  # The hibernation timeout
  ]

  @chunk_size      65_536
  @default_timeout 15_000

  #
  # Stagger.MsgQueue is a GenServer that implements the GenStage Producer protocol
  # directly. The justification for this is:
  #
  # * It has single-consumer, point-to-point semantics with recovery and
  #   acknowledgement.
  # * It implicitly does its own buffering via the underlying file.
  # * It allows the use `handle_continue`, something not currently supported by
  #   GenStage.
  #
  # It currently uses two file 'handles' to the same file, one for writing and
  # one for reading. It could be possible to reduce this to a single file if the
  # positions were managed explicitly.
  #
  # The format of the file is very simple: Each message is encoded as follows
  #
  # -+-----------------+--------+--------------+-
  #  | Sequence number |  Size  | Message      |
  # -+-----------------+--------+--------------+-
  #    8 bytes          4 bytes  <Size> bytes
  #
  # Both the sequence number and the size are encoded as little-endian
  #

  @spec start_link(%{:path => binary, optional(any) => any}) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(%{path: path} = args) when is_binary(path) do
    GenServer.start_link(__MODULE__, args, name: {:via, Registry, {Stagger.MsgQueueRegistry, path}})
  end

  @spec write(stage :: atom | pid | {atom, any} | {:via, atom, any}, term :: any, timeout :: :infinity | non_neg_integer()) :: any
  def write(stage, term, timeout) do
    # Using `term_to_iovec` makes it more efficient if the term itself contains binaries
    GenServer.call(stage, {:write, :erlang.term_to_iovec(term)}, timeout)
  end

  @impl true
  def init(%{path: path} = args) when is_binary(path) do
    timeout = Map.get(args, :hibernate_after, @default_timeout)
    {:ok, file, seqno} = open_write_file(path)
    {:ok, %__MODULE__{path: path, wr_file: file, wr_seqno: seqno, timeout: timeout}, timeout}
  end

  @impl true
  def handle_call({:write, data}, _from, %__MODULE__{wr_file: file, wr_seqno: seqno} = state) do
    next = seqno + 1
    size = :erlang.iolist_size(data)
    blob = [<<next::little-size(64), size::little-size(32)>> | data]
    reply = :file.write(file, blob)

    {:reply, reply, %__MODULE__{state | wr_seqno: next}, {:continue, :fulfill}}
  end

  @impl true
  def handle_info({:"$gen_producer", from, {:ask, new}}, %__MODULE__{timeout: timeout} = state) do
    # Handle new demand providing it comes from our current subscription
    case state do
      %__MODULE__{subscriber: ^from, demand: current} ->
        {:noreply, %__MODULE__{state | demand: current + new}, {:continue, :fulfill}}
      _ ->
        {:noreply, state, timeout}
    end
  end
  def handle_info({:"$gen_producer", {pid, ref} = from, {:subscribe, cancel, opts}}, %__MODULE__{subscriber: subscriber, timeout: timeout} = state) do
    # Handle subscription / resubscription
    cond do
      from == subscriber ->
        Process.send(pid, {:"$gen_consumer", {self(), ref}, {:cancel, :duplicated_subscription}}, [:noconnect])
        {:noreply, state, timeout}
      is_nil(cancel) && not is_nil(subscriber) ->
        Process.send(pid, {:"$gen_consumer", {self(), ref}, {:cancel, :oversubscribed}}, [:noconnect])
        {:noreply, state, timeout}
      true ->
        reason = if cancel, do: elem(cancel, 1), else: nil
        {:noreply, subscribe(from, opts, unsubscribe(reason, state)), {:continue, :fulfill}}
    end
  end
  def handle_info({:"$gen_producer", _from, {:cancel, reason}}, %__MODULE__{timeout: timeout} = state) do
    # Handle unsubscription
    {:noreply, unsubscribe(reason, state), timeout}
  end
  def handle_info({:DOWN, ref, :process, pid, _reason}, %__MODULE__{subscriber: {pid, _}, monitor: ref, timeout: timeout} = state) do
    # Handle subscriber loss
    {:noreply, unsubscribe(nil, state), timeout}
  end
  def handle_info({:DOWN, _, _, _, _}, %__MODULE__{timeout: timeout} = state) do
    # Ignore loss of old subscribers
    {:noreply, state, timeout}
  end
  def handle_info(:timeout, state) do
    {:noreply, state, :hibernate}
  end

  @impl true
  def handle_continue(:fulfill, state) do
    fulfill(state)
  end

  defp subscribe({pid, _ref} = from, opts, %__MODULE__{path: path, wr_file: wr_file, wr_seqno: wr_seqno, subscriber: nil, monitor: nil, rd_file: nil} = state) do
    # ack is the seqno that subscriber last processed
    ack  = seqno_from_options(opts, :ack, 0)
    # last is the maximum sequence number that can be purged from the file
    last = min(ack, seqno_from_options(opts, :purge, 0))

    # close the file, purge it (if possible), and then recover
    :ok = :file.close(wr_file)
    :ok = purge_up_to(path, min(last + 1, wr_seqno))
    # we should have the same wr_seqno after recovery
    {:ok, wr_file, ^wr_seqno} = open_write_file(path)

    {:ok, rd_file}   = :file.open(path, [:read, :write, :raw, :binary])
    {:ok, _position} = move_to_seqno(rd_file, ack + 1)

    %__MODULE__{state | subscriber: from, monitor: Process.monitor(pid), wr_file: wr_file, rd_file: rd_file, rd_seqno: ack}
  end

  defp unsubscribe(_reason, %__MODULE__{subscriber: nil} = state), do: state
  defp unsubscribe(reason, %__MODULE__{subscriber: subscriber, monitor: ref, rd_file: rd_file} = state) do
    {sub, oldref} = subscriber
    Process.send(sub, {:"$gen_consumer", {self(), oldref}, {:cancel, reason}}, [:noconnect])
    Process.demonitor(ref)
    :ok = :file.close(rd_file)
    %__MODULE__{state | subscriber: nil, monitor: nil, rd_file: nil, rd_seqno: 0}
  end

  defp fulfill(%__MODULE__{subscriber: subscriber, demand: demand, timeout: timeout} = state) when is_nil(subscriber) or demand == 0 do
    # Don't send events if there's no subscriber or no demand
    {:noreply, state, timeout}
  end
  defp fulfill(%__MODULE__{wr_seqno: seqno, rd_seqno: seqno, timeout: timeout} = state) do
    # Don't send events if the subscriber has caught up
    {:noreply, state, timeout}
  end
  defp fulfill(%__MODULE__{rd_file: file, rd_seqno: seqno, demand: demand} = state) do
    {:ok, posn} = :file.position(file, :cur)
    fulfill_from_file(state, demand, seqno, posn, <<>>, [])
  end

  #
  # `fulfill_from_file` and `fulfill_from_chunk` are a couple of mutually recursive
  # functions that traverse the file from its current position, accumulating events
  # to deliver to the consumer.
  #
  defp fulfill_from_file(state, demand, seqno, posn, _prev, events) when demand == 0 do
    fulfilled(state, demand, seqno, posn, events)
  end
  defp fulfill_from_file(%__MODULE__{rd_file: file} = state, demand, seqno, posn, prev, events) do
    case :file.read(file, @chunk_size) do
      {:ok, chunk} ->
        fulfill_from_chunk(state, demand, seqno, posn, prev <> chunk, events)
      :eof ->
        fulfilled(state, demand, seqno, posn, events)
    end
  end

  defp fulfill_from_chunk(state, demand, seqno, posn, _prev, events) when demand == 0 do
    fulfilled(state, demand, seqno, posn, events)
  end
  defp fulfill_from_chunk(state, demand, seqno, posn, chunk, events) do
    case chunk do
      <<next::little-size(64), size::little-size(32), data::binary-size(size), rest::binary>> ->
        cond do
          next == seqno + 1 ->
            term = :erlang.binary_to_term(data)
            fulfill_from_chunk(state, demand - 1, next, posn + 8 + 4 + size, rest, [{next, term} | events])
          next > seqno + 1 ->
            fulfill_from_chunk(state, demand , seqno + 1, posn, chunk, events)
          true ->
            fulfilled(state, demand, seqno, posn, events)
        end
      _ ->
        fulfill_from_file(state, demand, seqno, posn, chunk, events)
    end
  end

  defp fulfilled(%__MODULE__{subscriber: {sub, ref}, rd_file: file, timeout: timeout} = state, demand, seqno, posn, events) do
    {:ok, ^posn} = :file.position(file, posn)
    send(sub, {:"$gen_consumer", {self(), ref}, :lists.reverse(events)})
    {:noreply, %__MODULE__{state | rd_seqno: seqno, demand: demand}, timeout}
  end

  defp purge_up_to(path, seqno) do
      {:ok, infile}   = :file.open(path, [:read, :raw, :binary])
      {:ok, inlength} = :file.position(infile, :eof)
      {:ok, 0}        = :file.position(infile, :bof)
      {:ok, position} = move_to_seqno(infile, seqno)
      case position do
        0 ->
          :ok = :file.close(infile)
        _ ->
          tmppath = "#{path}.tmp"
          try do
            {:ok, outfile} = :file.open(tmppath, [:write, :raw, :binary])
            {:ok, _} = :file.position(outfile, {:bof, inlength - position})
            :ok      = :file.truncate(outfile)
            {:ok, 0} = :file.position(outfile, :bof)

            :ok = copy_file(infile, outfile)
            :ok = :file.close(outfile)
            :ok = :file.close(infile)
            :ok = :file.rename(tmppath, path)
          after
            File.rm_rf(tmppath)
          end
      end
  end

  defp copy_file(infile, outfile) do
    case :file.read(infile, @chunk_size) do
      {:ok, chunk} ->
        :ok = :file.write(outfile, chunk)
        copy_file(infile, outfile)
      :eof ->
        :ok
    end
  end

  defp open_write_file(path) do
    {:ok, file} = :file.open(path, [:read, :write, :raw, :binary])
    {:ok, _good, _bad, seqno} = recover(file)
    {:ok, file, seqno}
  end

  defp move_to_seqno(file, seqno) do
    {:ok, cur}  = :file.position(file, :cur)
    {:ok, off}  = move_to_seqno_file(file, seqno, <<>>, cur)
    :file.position(file, off)
  end

  defp move_to_seqno_file(file, seqno, prev, off) do
    case :file.read(file, @chunk_size) do
      {:ok, chunk} ->
        move_to_seqno_chunk(file, seqno, prev <> chunk, off)
      :eof ->
        {:ok, off}
    end
  end

  defp move_to_seqno_chunk(file, seqno, chunk, off) do
    case chunk do
      <<next::little-size(64), _::binary>> when next >= seqno ->
        {:ok, off}
      <<_next::little-size(64), size::little-size(32), _::binary-size(size), rest::binary>> ->
        move_to_seqno_chunk(file, seqno, rest, off + 8 + 4 + size)
      remainder ->
        move_to_seqno_file(file, seqno, remainder, off)
    end
  end

  defp recover(file) do
    {:ok, cur}  = :file.position(file, :cur)
    {:ok, good, bad, _seqno} = result = recover_from_file(file, 0, <<>>, cur)
    {:ok, ^good} = :file.position(file, good)
    if bad > 0 do
      :ok = :file.truncate(file)
    end
    result
  end

  defp recover_from_file(file, seqno, prev, posn) do
    case :file.read(file, @chunk_size) do
      {:ok, chunk} ->
        recover_from_chunk(file, seqno, prev <> chunk, posn)
      :eof ->
      {:ok, posn, byte_size(prev), seqno}
    end
  end

  defp recover_from_chunk(file, seqno, chunk, off) do
    case chunk do
      <<next::little-size(64), size::little-size(32), _::binary-size(size), rest::binary>>
      when seqno == 0 or seqno + 1 == next ->
        recover_from_chunk(file, next, rest, off + 8 + 4 + size)
      remainder ->
        recover_from_file(file, seqno, remainder, off)
    end
  end

  defp seqno_from_options(opts, kw, dv) do
    case Keyword.get(opts, kw, dv) do
      v when is_integer(v) and v >= 0 ->
        v
      _ ->
        0
    end
  end

end
