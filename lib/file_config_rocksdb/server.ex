defmodule FileConfigRocksdb.Server do
  @moduledoc "Owns connections to RocksDB"

  use GenServer
  require Logger

  @server __MODULE__
  @call_timeout 30000

  # API

  def open(db_path, options) do
    :gen_server.call(@server, {:open, db_path, options}, @call_timeout)
  end

  def write(db_path, batch, options) do
    :gen_server.call(@server, {:write, db_path, batch, options}, @call_timeout)
  end

  def get(db_path, key, options) do
    :gen_server.call(@server, {:get, db_path, key, options}, @call_timeout)
  end

  def start_link do
    :gen_server.start_link({:local, @server}, __MODULE__, [], [])
  end

  def start_link(args, opts \\ []) do
    :gen_server.start_link({:local, @server}, __MODULE__, args, opts)
  end

  # gen_server callbacks

  def init(args) do
    {:ok, %{
      db_cache: %{},
      backoff_threshold: args[:backoff_threshold] || 200,
      backoff_multiple: args[:backoff_multiple] || 10,
    }}
  end

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
    {{:ok, db}, new_state} = get_db(db_path, state)
    # reply = {length(batch), duration}
    # reply = :rocksdb.write(db, batch, options)

    {duration, reply} = :timer.tc(:rocksdb, :write, [db, batch, options])
    duration = div(duration, 1024) # convert microseconds to milliseconds

    if duration > state.backoff_threshold do
      backoff = state.backoff_multiple * duration
      Logger.warning("rocksdb #{db_path} duration #{duration} backoff #{backoff}")
      # Metrics.inc([:records, :throttle], [topic: topic], count)
      Process.sleep(backoff)
    end
    {:reply, reply, new_state}
  end

  def handle_call({:get, db_path, key, options}, _from, state) do
    {{:ok, db}, new_state} = get_db(db_path, state)
    # {duration, result} = :timer.tc(:rocksdb, :get, [db, key, options])
    # Logger.debug("rocksdb get #{db_path} #{key} duration #{duration}")
    result = :rocksdb.get(db, key, options)

    {:reply, result, new_state}
  end

  def handle_call(request, _from, state) do
    Logger.warn("Unexpected call: #{inspect request}")
    {:reply, :ok, state}
  end

  defp get_db(db_path, state) do
    Logger.error("state: #{inspect(state)}")
    db_cache = state.db_cache
    case Map.fetch(db_cache, db_path) do
      {:ok, db} = reply ->
        Logger.info("Using cached handle #{db_path} #{db}")
        {reply, state}
      :error ->
        open_options = [create_if_missing: true]
        case :rocksdb.open(to_charlist(db_path), open_options) do
          {:ok, db} = reply ->
            Logger.info("Opened #{db_path} #{db}")
            db_cache = Map.put(db_cache, db_path, db)
            {reply, %{state | db_cache: db_cache}}
          {:error, reason} = reply ->
            Logger.error("Error opening rocksdb #{db_path}: #{inspect(reason)}")
            {reply, state}
        end
    end
  end
end
