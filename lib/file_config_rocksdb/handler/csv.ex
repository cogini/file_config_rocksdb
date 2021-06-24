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
    db_path = state[:db_path] || []
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
            Logger.warning("Error reading from rocksdb #{name} #{key}: #{inspect(error)}")
            :undefined
        end

        # TODO: somehow reuse db handle

        # case :rocksdb.open(db_path, create_if_missing: false) do
        # case :rocksdb.open(db_path, create_if_missing: true) do

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
    Logger.debug("#{name} #{db_path} #{inspect(update)}")

    if update_db?(db_path, update.mod) do
      # {topen, {:ok, db}} =
      #   :timer.tc(:rocksdb, :open, [to_charlist(db_path), [create_if_missing: true]])

      {:ok, _db} = Server.open(db_path, create_if_missing: true)

      for {path, %{mod: file_mod}} <- Enum.reverse(update.files), update_db?(path, file_mod) do
        Logger.debug("Loading #{name} #{path} #{db_path}")
        {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, db_path, config])
        Logger.info("Loaded #{name} #{path} #{rec} rec #{time / 1_000_000} sec")
        # Record last file load
        :ok = File.touch(status_path(db_path), file_mod)
      end

      # :ok = :rocksdb.close(db)
      # {tclose, :ok} = :timer.tc(:rocksdb, :close, [db])

      # Logger.debug("Loaded #{name} open #{topen / 1_000_000}, close #{ tclose / 1_000_000}")
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
    # {:ok, db} = :rocksdb.open(state.db_path, create_if_missing: false)
    {:ok, db} = :rocksdb.open(state.db_path, create_if_missing: true)

    records
    |> Enum.sort()
    |> Enum.chunk_every(state.chunk_size)
    |> Enum.each(&insert_chunk(&1, state, db))

    # Record last updat
    :ok = File.touch(status_path(state.db_path))

    :ok = :rocksdb.close(db)
    true
  end

  def insert_records(state, record) when is_tuple(record), do: insert_records(state, [record])

  # Internal functions

  # This file keeps track of the last update to the db
  defp status_path(db_path) do
    # Path.join(db_path, "CURRENT")
    db_path <> ".stat"
  end

  # Determine if update is newer than db
  @spec update_db?(Path.t(), :calendar.datetime()) :: boolean()
  defp update_db?(db_path, update_mtime) do
    case File.stat(db_path) do
      {:ok, _dir_stat} ->
        case File.stat(status_path(db_path)) do
          {:ok, %{mtime: file_mtime}} ->
            file_mtime < update_mtime
          {:error, _reason} ->
            true
        end

      # case File.ls(path) do
      #   {:ok, files} ->
      #     file_times = for file <- files, file_path <- Path.join(path, file),
      #       {:ok, %{mtime: file_mtime}} <- File.stat(file_path), do: file_mtime
      #     Enum.all?(file_times, &(&1 < mod))
      #   {:error, reason} ->
      #     Logger.warning("Error reading path #{path}: #{reason}")
      #     true
      # end
      {:error, :enoent} ->
        true
    end
  end

 #  @spec parse_file(Path.t(), :rocksdb.db_handle(), map()) :: {:ok, non_neg_integer()}
  @spec parse_file(Path.t(), Path.t(), map()) :: {:ok, non_neg_integer()}
  defp parse_file(path, db_path, config) do
    chunk_size = config[:chunk_size] || 100
    csv_fields = config[:csv_fields] || {1, 2}

    stream =
      path
      |> File.stream!(read_ahead: 100_000)
      |> Parser.parse_stream(skip_headers: false)
      |> Stream.chunk_every(chunk_size)
      |> Stream.map(&write_chunk(&1, db_path, csv_fields))

    # start_time = :os.timestamp()
    results = Enum.to_list(stream)
    # tprocess = :timer.now_diff(:os.timestamp(), start_time) / 1_000_000

    # Logger.warning("results: #{inspect results}")

    rec = Enum.reduce(results, 0, fn {count, _duration}, acc -> acc + count end)
    {:ok, rec}
  end

  # @doc "Get path to db for name"
  @spec db_path(atom()) :: Path.t()
  defp db_path(name) do
    state_dir = Application.get_env(@app, :state_dir, "/var/lib/file_config")
    Path.join(state_dir, to_string(name))
  end

  # @spec write_chunk(list(tuple()), :rocksdb.db_handle(), {pos_integer(), pos_integer()}) :: {non_neg_integer(), non_neg_integer()}
  @spec write_chunk(list(tuple()), Path.t(), {pos_integer(), pos_integer()}) :: {non_neg_integer(), non_neg_integer()}
  def write_chunk(chunk, db_path, {k, v}) do
    batch = for row <- chunk, do: {:put, Enum.at(row, k - 1), Enum.at(row, v - 1)}
    # Logger.warning("batch: #{inspect(batch)}")
    # :ok = :rocksdb.write(db, batch, sync: true)
    # {duration, :ok} = :timer.tc(:rocksdb, :write, [db, batch, []])
    {duration, :ok} = :timer.tc(Server, :write, [db_path, batch, []])
    {length(batch), duration}
  end

  # @spec insert_chunk(list(tuple()), :ets.tab(), :rocksdb.db_handle()) ::
  #         {non_neg_integer(), non_neg_integer()}
  @spec insert_chunk(list(tuple()), :ets.tab(), Path.t()) ::
          {non_neg_integer(), non_neg_integer()}
  defp insert_chunk(chunk, tab, db_path) do
    batch = for {key, value} <- chunk, do: {:put, key, value}
    {duration, :ok} = :timer.tc(Server, :write, [db_path, batch, []])
    true = :ets.insert(tab, chunk)

    {length(batch), duration}
  end
end
