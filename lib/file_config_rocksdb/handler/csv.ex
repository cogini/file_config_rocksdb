defmodule FileConfigRocksdb.Handler.Csv do
  @moduledoc "Handler for CSV files with RocksDB backend"
  @app :file_config_rocksdb

  NimbleCSV.define(FileConfigRocksdb.Handler.Csv.Parser, separator: "\t", escape: "\0")
  alias FileConfigRocksdb.Handler.Csv.Parser

  require Logger

  alias FileConfig.Loader
  # alias FileConfig.Lib

  alias FileConfigRocksdb.Server

  # @impl true
  @spec lookup(Loader.table_state(), term()) :: term()
  def lookup(%{id: tid, name: name, parser: parser} = state, key) do
    db_path = state[:db_path] || ''
    parser_opts = state[:parser_opts] || []

    case :ets.lookup(tid, key) do
      [{_key, :undefined}] ->
        # Cached "not found" result
        :undefined

      [{_key, value}] ->
        # Cached result
        {:ok, value}

      [] ->
        # Not found
        case Server.get(db_path, key, []) do
          {:ok, bin} ->
            case parser.decode(bin, parser_opts) do
              {:ok, value} ->
                # Cache parsed value
                true = :ets.insert(tid, [{key, value}])
                {:ok, value}

              {:error, reason} ->
                Logger.debug("Error parsing table #{name} key #{key}: #{inspect(reason)}")
                {:ok, bin}
            end

          :not_found ->
            # Cache "not found" result
            true = :ets.insert(tid, [{key, :undefined}])
            :undefined

          error ->
            Logger.warning("Error reading rocksdb #{name} #{key}: #{inspect(error)}")
            :undefined
        end

        # case Server.open(db_path, create_if_missing: true) do
        #   {:ok, _db} ->
        #       return =
        #         # case :rocksdb.get(db, key, []) do
        #         case Server.get(db_path, key, []) do
        #           {:ok, bin} ->
        #             case parser.decode(bin, parser_opts) do
        #               {:ok, value} ->
        #                 # Cache parsed value
        #                 true = :ets.insert(tid, [{key, value}])
        #                 {:ok, value}
        #
        #               {:error, reason} ->
        #                 Logger.debug("Error parsing table #{name} key #{key}: #{inspect(reason)}")
        #                 {:ok, bin}
        #             end
        #
        #           :not_found ->
        #             # Cache "not found" result
        #             true = :ets.insert(tid, [{key, :undefined}])
        #             :undefined
        #
        #           error ->
        #             Logger.warning("Error reading from rocksdb #{name} #{key}: #{inspect(error)}")
        #             :undefined
        #         end
        #
        #       # :ok = :rocksdb.close(db)
        #       return
        #   {:error, reason} ->
        #     Logger.warning("Error opening rocksdb #{db_path}: #{inspect(reason)}")
        #     :undefined
        # end
    end
  end

  # @impl true
  @spec load_update(Loader.name(), Loader.update(), :ets.tid()) :: Loader.table_state()
  def load_update(name, update, tid) do
    db_path = db_path(name)
    config = update.config
    chunk_size = config[:chunk_size] || 100
    # Logger.debug("#{name} #{db_path} #{inspect(update)}")

    status_path = status_path(db_path)
    status_mod = file_mtime(status_path)
    # Logger.debug("Status file #{status_path} #{inspect(status_mod)}")

    if update.mod > status_mod do
      # Sort files from oldest to newest
      files = Enum.sort(update.files, fn({_, %{mod: a}}, {_, %{mod: b}}) -> a <= b end)

      for {path, %{mod: file_mod}} <- files, file_mod > status_mod do
        Logger.info("Loading #{name} #{path} #{inspect(file_mod)}")
        {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, db_path, config])
        Logger.info("Loaded #{name} #{path} #{rec} rec #{time / 1_000_000} sec")
        # Record last successful file load
        :ok = File.touch(status_path, file_mod)
      end
      Logger.info("Loaded #{name} #{db_path} complete")
    else
      Logger.info("Loaded #{name} #{db_path} up to date")
    end

    Map.merge(
      %{
        name: name,
        id: tid,
        mod: update.mod,
        handler: __MODULE__,
        db_path: to_charlist(db_path),
        chunk_size: chunk_size
      },
      Map.take(config, [:parser, :parser_opts, :commit_cycle])
    )
  end

  # @impl true
  @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) :: true
  def insert_records(state, records) when is_list(records) do
    records
    |> Enum.sort()
    |> Enum.chunk_every(state.chunk_size)
    |> Enum.each(&insert_chunk(&1, state))

    # Record last update
    :ok = File.touch(status_path(state.db_path))

    true
  end

  def insert_records(state, record) when is_tuple(record), do: insert_records(state, [record])

  # Internal functions

  # This file keeps track of the last update to the db
  @spec status_path(Path.t()) :: Path.t()
  defp status_path(db_path) do
    # Path.join(db_path, "CURRENT")
    db_path <> ".stat"
  end

  # Get modification time of file or Unix epoch on error
  @spec file_mtime(Path.t()) :: :calendar.datetime()
  defp file_mtime(path) do
    case File.stat(path) do
      {:ok, %{mtime: mtime}} ->
        mtime
      {:error, reason} ->
        Logger.debug("Could not stat file #{path}: #{inspect(reason)}")
        {{1970, 1, 1}, {0, 0, 0}}
    end
  end

  # @spec parse_file(Path.t(), :rocksdb.db_handle(), map()) :: {:ok, non_neg_integer()}
  @spec parse_file(Path.t(), Path.t(), map()) :: {:ok, non_neg_integer()}
  defp parse_file(path, db_path, config) do
    chunk_size = config[:chunk_size] || 100
    csv_fields = config[:csv_fields] || {1, 2}

    stream =
      path
      |> File.stream!(read_ahead: 100_000)
      |> Parser.parse_stream(skip_headers: false)
      |> Stream.chunk_every(chunk_size)
      |> Stream.with_index()
      |> Stream.map(&write_chunk(&1, db_path, csv_fields))

    # start_time = :os.timestamp()
    results = Enum.to_list(stream)
    # tprocess = :timer.now_diff(:os.timestamp(), start_time) / 1_000_000

    rec = Enum.reduce(results, 0, fn {count, _duration}, acc -> acc + count end)
    {:ok, rec}
  end

  # @doc "Get path to db for name"
  @spec db_path(atom()) :: Path.t()
  defp db_path(name) do
    state_dir = Application.get_env(@app, :state_dir, "/var/lib/file_config")
    Path.join(state_dir, to_string(name))
  end

  @spec write_chunk({list(tuple()), non_neg_integer()}, Path.t(), {pos_integer(), pos_integer()}) :: {non_neg_integer(), non_neg_integer()}
  def write_chunk({chunk, index}, db_path, {k, v}) do
    if rem(index + 1, 10) == 0 do
      Logger.info("Writing #{db_path} index #{index}")
    end
    batch = for row <- chunk, do: {:put, Enum.at(row, k - 1), Enum.at(row, v - 1)}
    {duration, :ok} = :timer.tc(Server, :write, [db_path, batch, []])
    {length(batch), duration}
  end

  @spec insert_chunk(list(tuple()), Loader.table_state()) :: {non_neg_integer(), non_neg_integer()}
  defp insert_chunk(chunk, state) do
    batch = for {key, value} <- chunk, do: {:put, key, value}
    {duration, :ok} = :timer.tc(Server, :write, [state.db_path, batch, []])
    true = :ets.insert(state.id, chunk)

    {length(batch), duration}
  end

end
