defmodule DataLake do
  @moduledoc """
  Public API for the hybrid data lake platform.

  Combines a high-velocity hot write buffer (`DataLake.HotStore`), compressed
  per-column cold storage (`DataLake.ColdStore`), an ETS-backed transactional
  manifest catalog (`DataLake.Catalog`), and a hybrid query router
  (`DataLake.QueryRouter`) with four-dimensional cost telemetry.
  """

  alias DataLake.{Catalog, ColdStore, Compactor, HotStore, QueryRouter, RocksDB}

  @type table_name :: atom() | String.t()

  @doc """
  Ingests a single map record or a list of map records into the hot write tier.

  Pass `auto_compact: false` in `opts` to defer automatic threshold compaction.
  """
  @spec ingest(table_name(), map() | [map()], keyword()) :: {:ok, non_neg_integer()}
  def ingest(table, record_or_records, opts \\ [])

  def ingest(table, record, opts) when is_map(record) do
    ingest(table, [record], opts)
  end

  def ingest(table, records, opts) when is_list(records) do
    {:ok, count} = HotStore.put_batch(table, records)

    if Keyword.get(opts, :auto_compact, true) do
      Compactor.maybe_compact(table)
    end

    {:ok, count}
  end

  @doc """
  Performs a point lookup for `record_id` across the hot buffer first,
  falling back to zone-map pruned cold segments.
  """
  @spec get(table_name(), term()) :: {:ok, map()} | :not_found
  def get(table, record_id) do
    case HotStore.get_by_id(table, record_id) do
      {:ok, record} ->
        {:ok, record}

      :not_found ->
        find_in_cold(table, record_id)
    end
  end

  @doc """
  Synchronously compacts all buffered hot records for `table` into an
  immutable columnar cold segment and registers its zone maps.

  Supports `sort_by: column_key` in `opts` for range-clustered segments.
  """
  @spec compact(table_name(), keyword()) :: {:ok, map() | :noop}
  def compact(table, opts \\ []) do
    Compactor.compact(table, opts)
  end

  @doc """
  Executes a hybrid query across hot and cold tiers, returning rows and
  holistic cost metrics.
  """
  @spec query(table_name(), keyword()) :: {:ok, %{rows: [map()], metrics: map()}}
  def query(table, opts \\ []) do
    QueryRouter.execute(table, opts)
  end

  @doc """
  Returns discovered schema evolution and column statistics across all
  committed segments for `table`.
  """
  @spec schema(table_name()) :: map()
  def schema(table) do
    Catalog.schema_lineage(table)
  end

  @doc """
  Returns storage and metadata statistics for `table`.
  """
  @spec stats(table_name()) :: map()
  def stats(table) do
    hot_stats = HotStore.count(table)
    segments = Catalog.list_segments(table)
    snapshot = Catalog.latest_snapshot(table)

    {cold_rows, cold_bytes} =
      Enum.reduce(segments, {0, 0}, fn seg, {rows_acc, bytes_acc} ->
        seg_bytes =
          seg
          |> Map.get(:zone_maps, %{})
          |> Map.values()
          |> Enum.reduce(0, fn zm, acc -> acc + Map.get(zm, :byte_size, 0) end)

        {rows_acc + Map.get(seg, :row_count, 0), bytes_acc + seg_bytes}
      end)

    %{
      table: table,
      hot: hot_stats,
      cold: %{
        segments: length(segments),
        rows: cold_rows,
        compressed_bytes: cold_bytes
      },
      snapshot: snapshot,
      rocksdb: RocksDB.storage_info()
    }
  end

  @doc """
  Returns physical storage metadata for the external RocksDB persistence tier.
  """
  @spec rocksdb_info() :: map()
  def rocksdb_info do
    RocksDB.storage_info()
  end

  @doc """
  Flushes the active RocksDB MemTable to immutable `.sst` SSTable files on disk.
  """
  @spec rocksdb_flush() :: :ok
  def rocksdb_flush do
    RocksDB.flush()
  end

  @doc """
  Triggers full RocksDB LSM-tree compaction across SSTable levels.
  """
  @spec rocksdb_compact() :: :ok
  def rocksdb_compact do
    RocksDB.compact()
  end

  @doc """
  Benchmarks the same query in two modes:
  1. Naive unpruned full-column scan (`select: :all`, `use_zone_maps: false`).
  2. Hybrid zone-map pruned and column-projected scan (`use_zone_maps: true`).

  Returns metrics across startup time, query time, CPU reductions, and
  storage bytes scanned along with computed savings.
  """
  @spec benchmark_holistic_cost(table_name(), keyword()) :: {:ok, map()}
  def benchmark_holistic_cost(table, opts \\ []) do
    naive_opts =
      opts
      |> Keyword.put(:select, :all)
      |> Keyword.put(:use_zone_maps, false)

    hybrid_opts = Keyword.put(opts, :use_zone_maps, true)

    {:ok, %{metrics: naive_metrics}} = QueryRouter.execute(table, naive_opts)
    {:ok, %{rows: rows, metrics: hybrid_metrics}} = QueryRouter.execute(table, hybrid_opts)

    result = %{
      rows_returned: length(rows),
      naive_full_scan: naive_metrics,
      hybrid_pruned_scan: hybrid_metrics,
      savings: %{
        bytes_saved: naive_metrics.storage_bytes_scanned - hybrid_metrics.storage_bytes_scanned,
        reductions_saved: naive_metrics.cpu_reductions - hybrid_metrics.cpu_reductions,
        segments_pruned: hybrid_metrics.segments_pruned,
        time_saved_us: naive_metrics.total_time_us - hybrid_metrics.total_time_us
      }
    }

    {:ok, result}
  end

  @doc """
  Resets all hot buffers, catalog manifests, cold storage segments, and RocksDB state.
  """
  @spec reset!() :: :ok
  def reset! do
    HotStore.reset!()
    Catalog.reset!()
    ColdStore.reset!()
    RocksDB.reset!()
    :ok
  end

  defp find_in_cold(table, record_id) do
    case QueryRouter.execute(table, where: [{:id, :eq, record_id}], include_hot: false) do
      {:ok, %{rows: [_ | _] = rows}} ->
        {:ok, List.last(rows)}

      _ ->
        case QueryRouter.execute(table, where: [{"id", :eq, record_id}], include_hot: false) do
          {:ok, %{rows: [_ | _] = rows}} -> {:ok, List.last(rows)}
          _ -> :not_found
        end
    end
  end
end
