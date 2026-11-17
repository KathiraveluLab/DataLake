defmodule DataLake.HotStore do
  @moduledoc """
  High-velocity hot write buffer and speed layer backed by an ordered-set ETS
  table and persisted externally via write-through commits to `DataLake.RocksDB`.

  Absorbs schema-on-read map records with minimal ingestion overhead, serves
  low-latency point lookups by record ID, supports atomic draining by the
  compaction worker, and recovers uncompacted state from RocksDB on startup.
  """

  use GenServer

  alias DataLake.RocksDB

  @table_name :data_lake_hot_store
  @ns "hot"

  @type table_name :: atom() | String.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Ingests a single schema-on-read map record into the hot tier and writes
  through atomically to RocksDB.
  """
  @spec put(table_name(), map()) :: {:ok, pos_integer()}
  def put(table, record) when is_map(record) do
    put_batch(table, [record])
  end

  @doc """
  Ingests a batch of schema-on-read map records into the hot tier and commits
  them atomically to RocksDB via `WriteBatch`.
  """
  @spec put_batch(table_name(), [map()]) :: {:ok, non_neg_integer()}
  def put_batch(_table, []), do: {:ok, 0}

  def put_batch(table, records) when is_list(records) do
    now = System.system_time(:microsecond)

    {ets_entries, rocks_rec_items, rocks_id_items} =
      Enum.reduce(records, {[], [], []}, fn record, {ets_acc, rec_acc, id_acc} ->
        unless is_map(record) do
          raise ArgumentError, "expected map record, got: #{inspect(record)}"
        end

        seq = System.unique_integer([:monotonic, :positive])
        record_id = Map.get(record, :id) || Map.get(record, "id") || seq
        ingested_at = Map.get(record, :ingested_at) || Map.get(record, "ingested_at") || now
        ets_entry = {{table, seq, record_id}, record, ingested_at}

        rec_pair = {record_key(table, seq, record_id), ets_entry}
        id_pair = {id_key(table, record_id), record}

        {[ets_entry | ets_acc], [rec_pair | rec_acc], [id_pair | id_acc]}
      end)

    ordered_ets = Enum.reverse(ets_entries)
    ordered_rocks = Enum.reverse(rocks_rec_items) ++ Enum.reverse(rocks_id_items)

    :ok = RocksDB.put_batch(@ns, ordered_rocks)
    :ets.insert(@table_name, ordered_ets)
    {:ok, length(ordered_ets)}
  end

  @doc """
  Performs a fast point lookup for `record_id` in the hot tier for `table`,
  falling back to `DataLake.RocksDB` if absent from ETS.
  """
  @spec get_by_id(table_name(), term()) :: {:ok, map()} | :not_found
  def get_by_id(table, record_id) do
    matches =
      :ets.select(@table_name, [
        {{{table, :_, record_id}, :"$1", :_}, [], [:"$1"]}
      ])

    case List.last(matches) do
      nil -> RocksDB.get(@ns, id_key(table, record_id))
      record -> {:ok, record}
    end
  end

  @doc """
  Reads all uncompacted records for `table` directly from the hot tier.
  """
  @spec read_all(table_name()) :: [map()]
  def read_all(table) do
    :ets.select(@table_name, [
      {{{table, :_, :_}, :"$1", :_}, [], [:"$1"]}
    ])
  end

  @doc """
  Atomically drains up to `max_records` (or `:all`) from ETS and removes the
  drained keys from RocksDB.
  """
  @spec drain(table_name(), pos_integer() | :all) :: [map()]
  def drain(table, max_records \\ :all) do
    GenServer.call(__MODULE__, {:drain, table, max_records})
  end

  @doc """
  Returns the current buffered record count and estimated memory footprint in
  bytes for `table`.
  """
  @spec count(table_name()) :: %{count: non_neg_integer(), memory_bytes: non_neg_integer()}
  def count(table) do
    records = read_all(table)

    memory_bytes =
      Enum.reduce(records, 0, fn record, acc ->
        acc + :erlang.external_size(record)
      end)

    %{count: length(records), memory_bytes: memory_bytes}
  end

  @doc """
  Wipes only the in-memory ETS buffer and repopulates it from `DataLake.RocksDB`
  to recover persisted uncompacted records.
  """
  @spec reload_from_disk!() :: non_neg_integer()
  def reload_from_disk! do
    GenServer.call(__MODULE__, :reload_from_disk)
  end

  @doc """
  Clears all buffered records from ETS and RocksDB hot keys.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset)
  end

  @impl true
  def init(_opts) do
    :ets.new(@table_name, [
      :ordered_set,
      :named_table,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])

    do_reload_from_rocksdb()
    {:ok, %{}}
  end

  @impl true
  def handle_call({:drain, table, max_records}, _from, state) do
    keys = select_keys(table, max_records)

    {records, drained_id_set, rocks_keys_to_delete} =
      Enum.reduce(keys, {[], MapSet.new(), []}, fn {^table, seq, record_id} = key,
                                                   {recs_acc, id_set, rk_acc} ->
        case :ets.take(@table_name, key) do
          [{^key, record, _ingested_at}] ->
            rk = record_key(table, seq, record_id)
            {[record | recs_acc], MapSet.put(id_set, record_id), [rk | rk_acc]}

          [] ->
            {recs_acc, id_set, rk_acc}
        end
      end)

    {stale_id_keys, updated_id_items} =
      Enum.reduce(drained_id_set, {[], []}, fn record_id, {del_acc, put_acc} ->
        remaining =
          :ets.select(@table_name, [
            {{{table, :_, record_id}, :"$1", :_}, [], [:"$1"]}
          ])

        case List.last(remaining) do
          nil -> {[id_key(table, record_id) | del_acc], put_acc}
          latest_record -> {del_acc, [{id_key(table, record_id), latest_record} | put_acc]}
        end
      end)

    RocksDB.delete_batch(@ns, rocks_keys_to_delete ++ stale_id_keys)
    RocksDB.put_batch(@ns, updated_id_items)

    {:reply, Enum.reverse(records), state}
  end

  def handle_call(:reload_from_disk, _from, state) do
    :ets.delete_all_objects(@table_name)
    restored_count = do_reload_from_rocksdb()
    {:reply, restored_count, state}
  end

  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@table_name)
    RocksDB.clear_namespace(@ns)
    {:reply, :ok, state}
  end

  defp do_reload_from_rocksdb do
    entries =
      RocksDB.prefix_scan(@ns, "rec:")
      |> Enum.map(fn {_k, ets_entry} -> ets_entry end)

    if entries != [] do
      :ets.insert(@table_name, entries)
    end

    length(entries)
  end

  defp select_keys(table, :all) do
    :ets.select(@table_name, [
      {{{table, :_, :_}, :_, :_}, [], [:"$_"]}
    ])
    |> Enum.map(&elem(&1, 0))
  end

  defp select_keys(table, max_records) when is_integer(max_records) and max_records > 0 do
    case :ets.select(@table_name, [{{{table, :_, :_}, :_, :_}, [], [:"$_"]}], max_records) do
      {entries, _continuation} -> Enum.map(entries, &elem(&1, 0))
      :"$end_of_table" -> []
    end
  end

  defp record_key(table, seq, record_id) do
    padded_seq = seq |> Integer.to_string() |> String.pad_leading(20, "0")
    "rec:" <> encode_term(table) <> ":" <> padded_seq <> ":" <> encode_term(record_id)
  end

  defp id_key(table, record_id) do
    "id:" <> encode_term(table) <> ":" <> encode_term(record_id)
  end

  defp encode_term(term) do
    term
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
  end
end
