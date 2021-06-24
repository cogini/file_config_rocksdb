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

  def init(_args) do
    Logger.debug("Starting")
    {:ok, %{}}
  end

  def handle_call({:open, db_path, options}, _from, state) do
    {reply, new_state} =
      case Map.fetch(state, db_path) do
        {:ok, _value} = reply ->
          {reply, state}
        :error ->
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
    # {duration, :ok} = :timer.tc(:rocksdb, :write, [db, batch, options])
    # Logger.debug("rocksdb write #{db_path} duration #{duration}")

    # reply = {length(batch), duration}
    reply = :rocksdb.write(db, batch, options)

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
    case Map.fetch(state, db_path) do
      {:ok, _db} = reply ->
        {reply, state}
      :error ->
        open_options = [create_if_missing: true]
        case :rocksdb.open(to_charlist(db_path), open_options) do
          {:ok, db} = reply ->
            {reply, Map.put(state, db_path, db)}
          {:error, reason} = reply ->
            Logger.debug("Error opening rocksdb #{db_path}: #{inspect(reason)}")
            {reply, state}
        end
    end
  end
end
