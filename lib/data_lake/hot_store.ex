defmodule DataLake.HotStore do
  @moduledoc """
  High-velocity hot write buffer and speed layer backed by an ordered-set ETS
  table with concurrent read and write access.

  Absorbs schema-on-read map records with minimal ingestion overhead, serves
  low-latency point lookups by record ID, and supports atomic draining by the
  compaction worker without blocking concurrent writers.
  """

  use GenServer

  @table_name :data_lake_hot_store

  @type table_name :: atom() | String.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Ingests a single schema-on-read map record into the hot tier.
  """
  @spec put(table_name(), map()) :: {:ok, pos_integer()}
  def put(table, record) when is_map(record) do
    put_batch(table, [record])
  end

  @doc """
  Ingests a batch of schema-on-read map records into the hot tier.
  """
  @spec put_batch(table_name(), [map()]) :: {:ok, non_neg_integer()}
  def put_batch(_table, []), do: {:ok, 0}

  def put_batch(table, records) when is_list(records) do
    now = System.system_time(:microsecond)

    entries =
      Enum.map(records, fn record ->
        unless is_map(record) do
          raise ArgumentError, "expected map record, got: #{inspect(record)}"
        end

        seq = System.unique_integer([:monotonic, :positive])
        record_id = Map.get(record, :id) || Map.get(record, "id") || seq
        ingested_at = Map.get(record, :ingested_at) || Map.get(record, "ingested_at") || now
        {{table, seq, record_id}, record, ingested_at}
      end)

    :ets.insert(@table_name, entries)
    {:ok, length(entries)}
  end

  @doc """
  Performs a fast point lookup for `record_id` in the hot tier for `table`.
  Returns the most recently ingested matching record if multiple versions exist.
  """
  @spec get_by_id(table_name(), term()) :: {:ok, map()} | :not_found
  def get_by_id(table, record_id) do
    matches =
      :ets.select(@table_name, [
        {{{table, :_, record_id}, :"$1", :_}, [], [:"$1"]}
      ])

    case List.last(matches) do
      nil -> :not_found
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
  Atomically drains up to `max_records` (or `:all`) from the hot tier for `table`.
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
  Clears all buffered records across all tables.
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

    {:ok, %{}}
  end

  @impl true
  def handle_call({:drain, table, max_records}, _from, state) do
    keys = select_keys(table, max_records)

    records =
      Enum.flat_map(keys, fn key ->
        case :ets.take(@table_name, key) do
          [{^key, record, _ingested_at}] -> [record]
          [] -> []
        end
      end)

    {:reply, records, state}
  end

  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@table_name)
    {:reply, :ok, state}
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
end
