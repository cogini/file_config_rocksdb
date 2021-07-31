defmodule FileConfigRocksdb.Server do
  @moduledoc "Owns connections to RocksDB"

  use GenServer
  require Logger

  @server __MODULE__
  @call_timeout 30_000

  # API

  def open(db_path, options) do
    {duration, reply} = :timer.tc(:gen_server, :call, [@server, {:open, db_path, options}, @call_timeout])
    backoff(duration, options)
    reply
  end

  def write(db_path, batch, options) do
    {duration, reply} = :timer.tc(:gen_server, :call, [@server, {:write, db_path, batch, options}, @call_timeout])
    backoff(duration, options)
    reply
  end

  def get(db_path, key, options) do
    {_duration, reply} = :timer.tc(:gen_server, :call, [@server, {:get, db_path, key, options}, @call_timeout])
    # backoff(duration, options)
    reply
  end

  def start_link do
    :gen_server.start_link({:local, @server}, __MODULE__, [], [])
  end

  def start_link(args, opts \\ []) do
    :gen_server.start_link({:local, @server}, __MODULE__, args, opts)
  end

  # gen_server callbacks

  @impl true
  def init(_args) do
    # Process.flag(:trap_exit, true)

    :ets.new(__MODULE__, [:named_table, :public, :set])

    {:ok, %{}}
  end

  @impl true
  def handle_call({:open, db_path, options}, _from, state) do
    {reply, new_state} =
      case Map.fetch(state, db_path) do
        {:ok, _value} = reply ->
          {reply, state}

        :error ->
          Logger.warning("open #{db_path}")

          case :rocksdb.open(to_charlist(db_path), options) do
            {:ok, db} = reply ->
              {reply, Map.put(state, db_path, db)}

            {:error, reason} = reply ->
              Logger.debug("Error opening rocksdb #{db_path}: #{inspect(reason)}")
              {reply, state}
          end
      end
    {:reply, reply, new_state}
  end

  def handle_call({:write, db_path, batch, options}, _from, state) do
    {:ok, db} = get_db(db_path)
    reply = :rocksdb.write(db, batch, options)
    {:reply, reply, state}
  end

  def handle_call({:get, db_path, key, options}, _from, state) do
    {:ok, db} = get_db(db_path)
    result = :rocksdb.get(db, key, options)
    {:reply, result, state}
  end

  def handle_call(request, _from, state) do
    Logger.warn("Unexpected call: #{inspect request}")
    {:reply, :ok, state}
  end

  @impl true
  def terminate(reason, _state) do
    Logger.info("terminating #{inspect(reason)}")
  end

  defp get_db(db_path) do
    case :ets.lookup(__MODULE__, db_path) do
      [{_, db}] ->
        {:ok, db}

      [] ->
        open_options = [create_if_missing: true]
        case :rocksdb.open(to_charlist(db_path), open_options) do
          {:ok, db} = reply ->
            Logger.info("Opened #{db_path} #{inspect(db)}")

            true = :ets.insert(__MODULE__, [{db_path, db}])
            reply

          {:error, reason} = reply ->
            Logger.error("Error opening rocksdb #{db_path}: #{inspect(reason)}")
            reply
        end
    end

    # Logger.error("state: #{inspect(state)}")
    # db_cache = state.db_cache
    # case Map.fetch(db_cache, db_path) do
    #   {:ok, db} = reply ->
    #     # Logger.info("Using cached handle #{db_path} #{inspect(db)}")
    #     {reply, state}
    #   :error ->
    #     open_options = [create_if_missing: true]
    #     case :rocksdb.open(to_charlist(db_path), open_options) do
    #       {:ok, db} = reply ->
    #         Logger.info("Opened #{db_path} #{inspect(db)}")
    #         db_cache = Map.put(db_cache, db_path, db)
    #         {reply, %{state | db_cache: db_cache}}
    #       {:error, reason} = reply ->
    #         Logger.error("Error opening rocksdb #{db_path}: #{inspect(reason)}")
    #         {reply, state}
    #     end
    # end
  end

  @spec backoff(non_neg_integer(), Keyword.t()) :: true
  def backoff(duration, opts \\ []) do
    # Duration threshold to trigger backoff (ms)
    threshold = opts[:backoff_threshold] || 400
    # Backoff is duration times multiple
    multiple = opts[:backoff_multiple] || 5
    # Maximum backoff
    max = opts[:backoff_max] || 5_000

    # duration is in microseconds, natively, convert to milliseconds
    duration = div(duration, 1024)
    if duration > threshold do
      backoff = min(duration * multiple, max)
      Logger.warning("duration #{duration} backoff #{backoff}")
      Process.sleep(max)
    end

    true
  end
end
