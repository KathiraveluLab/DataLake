defmodule DataLake.Catalog do
  @moduledoc """
  Transactional metadata and manifest catalog backed by in-memory ETS read
  caches and durable atomic commits in `DataLake.RocksDB`.

  Maintains versioned segment manifests, schema lineage, and per-column
  zone maps (`min`, `max`, `null_count`, `byte_size`) to enable sub-millisecond
  query planning, time-travel snapshot queries, and partition pruning.
  """

  use GenServer

  alias DataLake.RocksDB

  @manifests_table :data_lake_catalog_manifests
  @snapshots_table :data_lake_catalog_snapshots
  @ns "catalog"

  @type table_name :: atom() | String.t()
  @type predicate_op :: :eq | :gt | :gte | :lt | :lte | :in | :between
  @type predicate :: {term(), predicate_op(), term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Atomically registers an immutable cold segment manifest, assigns its
  monotonic `snapshot_version`, and commits both manifest and snapshot in
  RocksDB and ETS.
  """
  @spec register_segment(table_name(), map()) :: {:ok, map()}
  def register_segment(table, segment_metadata) when is_map(segment_metadata) do
    GenServer.call(__MODULE__, {:register_segment, table, segment_metadata})
  end

  @doc """
  Lists registered cold segment manifests for `table` directly from ETS.

  Pass `version: max_version` in `opts` to retrieve only segments committed
  at or before that snapshot version.
  """
  @spec list_segments(table_name(), keyword()) :: [map()]
  def list_segments(table, opts \\ []) do
    max_version = Keyword.get(opts, :version)

    segments =
      :ets.select(@manifests_table, [
        {{{table, :_}, :"$1"}, [], [:"$1"]}
      ])

    case max_version do
      nil ->
        segments

      v when is_integer(v) and v >= 0 ->
        Enum.filter(segments, fn seg -> Map.get(seg, :snapshot_version, 0) <= v end)
    end
  end

  @doc """
  Returns the current snapshot metadata for `table`, or `nil` if no segments
  have been committed.
  """
  @spec latest_snapshot(table_name()) :: map() | nil
  def latest_snapshot(table) do
    case :ets.lookup(@snapshots_table, table) do
      [{^table, snapshot}] -> snapshot
      [] -> nil
    end
  end

  @doc """
  Evaluates predicates against segment zone maps and returns `{kept_segments, pruned_count}`.

  Supports `version: max_version` in `opts` for time-travel pruning.
  """
  @spec prune_segments(table_name(), [predicate()], keyword()) :: {[map()], non_neg_integer()}
  def prune_segments(table, predicates, opts \\ []) when is_list(predicates) do
    segments = list_segments(table, opts)

    if predicates == [] do
      {segments, 0}
    else
      kept = Enum.filter(segments, &segment_may_match?(&1, predicates))
      {kept, length(segments) - length(kept)}
    end
  end

  @doc """
  Returns schema evolution and column statistics across all committed segments
  for `table`.
  """
  @spec schema_lineage(table_name()) :: map()
  def schema_lineage(table) do
    segments = list_segments(table)
    snapshot = latest_snapshot(table)

    columns =
      Enum.reduce(segments, %{}, fn seg, acc ->
        version = Map.get(seg, :snapshot_version, 1)
        zone_maps = Map.get(seg, :zone_maps, %{})

        Enum.reduce(zone_maps, acc, fn {col, zm}, col_acc ->
          inferred_type = infer_value_type(zm.min, zm.max)

          Map.update(
            col_acc,
            col,
            %{
              first_seen_version: version,
              last_seen_version: version,
              segment_count: 1,
              total_nulls: zm.null_count,
              total_compressed_bytes: zm.byte_size,
              types: [inferred_type]
            },
            fn existing ->
              types =
                if inferred_type in existing.types do
                  existing.types
                else
                  existing.types ++ [inferred_type]
                end

              %{
                first_seen_version: min(existing.first_seen_version, version),
                last_seen_version: max(existing.last_seen_version, version),
                segment_count: existing.segment_count + 1,
                total_nulls: existing.total_nulls + zm.null_count,
                total_compressed_bytes: existing.total_compressed_bytes + zm.byte_size,
                types: types
              }
            end
          )
        end)
      end)

    %{
      table: table,
      latest_version: (snapshot && snapshot.version) || 0,
      segment_count: length(segments),
      columns: columns
    }
  end

  @doc """
  Wipes only the in-memory ETS catalog tables and reloads all manifests and
  snapshots from `DataLake.RocksDB`.
  """
  @spec reload_from_disk!() :: non_neg_integer()
  def reload_from_disk! do
    GenServer.call(__MODULE__, :reload_from_disk)
  end

  @doc """
  Removes all manifests and snapshot metadata for `table` from ETS and RocksDB.
  """
  @spec clear(table_name()) :: :ok
  def clear(table) do
    GenServer.call(__MODULE__, {:clear, table})
  end

  @doc """
  Clears all catalog manifests and snapshots from ETS and RocksDB.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset)
  end

  @impl true
  def init(_opts) do
    :ets.new(@manifests_table, [
      :ordered_set,
      :named_table,
      :public,
      {:read_concurrency, true}
    ])

    :ets.new(@snapshots_table, [
      :set,
      :named_table,
      :public,
      {:read_concurrency, true}
    ])

    restore_from_rocksdb()
    {:ok, %{}}
  end

  @impl true
  def handle_call({:register_segment, table, segment_metadata}, _from, state) do
    segment_id = Map.fetch!(segment_metadata, :segment_id)
    row_count = Map.get(segment_metadata, :row_count, 0)
    now = System.system_time(:microsecond)

    prev_snapshot =
      case :ets.lookup(@snapshots_table, table) do
        [{^table, snap}] -> snap
        [] -> %{version: 0, segment_count: 0, total_rows: 0}
      end

    next_version = prev_snapshot.version + 1

    manifest =
      segment_metadata
      |> Map.put(:table, table)
      |> Map.put(:snapshot_version, next_version)
      |> Map.put_new(:created_at, now)

    next_snapshot = %{
      table: table,
      version: next_version,
      segment_count: prev_snapshot.segment_count + 1,
      total_rows: prev_snapshot.total_rows + row_count,
      updated_at: now
    }

    :ok =
      RocksDB.put_batch(@ns, [
        {manifest_key(table, segment_id), manifest},
        {snapshot_key(table), next_snapshot}
      ])

    :ets.insert(@manifests_table, {{table, segment_id}, manifest})
    :ets.insert(@snapshots_table, {table, next_snapshot})

    {:reply, {:ok, manifest}, state}
  end

  def handle_call(:reload_from_disk, _from, state) do
    :ets.delete_all_objects(@manifests_table)
    :ets.delete_all_objects(@snapshots_table)
    restored_count = restore_from_rocksdb()
    {:reply, restored_count, state}
  end

  def handle_call({:clear, table}, _from, state) do
    :ets.match_delete(@manifests_table, {{table, :_}, :_})
    :ets.delete(@snapshots_table, table)
    RocksDB.clear_namespace(@ns, "manifest:" <> encode_term(table) <> ":")
    RocksDB.delete_batch(@ns, [snapshot_key(table)])
    {:reply, :ok, state}
  end

  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@manifests_table)
    :ets.delete_all_objects(@snapshots_table)
    RocksDB.clear_namespace(@ns)
    {:reply, :ok, state}
  end

  defp restore_from_rocksdb do
    manifests =
      RocksDB.prefix_scan(@ns, "manifest:")
      |> Enum.map(fn {_k, manifest} ->
        {{manifest.table, manifest.segment_id}, manifest}
      end)

    snapshots =
      RocksDB.prefix_scan(@ns, "snapshot:")
      |> Enum.map(fn {_k, snapshot} ->
        {snapshot.table, snapshot}
      end)

    if manifests != [], do: :ets.insert(@manifests_table, manifests)
    if snapshots != [], do: :ets.insert(@snapshots_table, snapshots)

    length(manifests)
  end

  defp manifest_key(table, segment_id) do
    "manifest:" <> encode_term(table) <> ":" <> segment_id
  end

  defp snapshot_key(table) do
    "snapshot:" <> encode_term(table)
  end

  defp encode_term(term) do
    term
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
  end

  defp infer_value_type(nil, nil), do: :null
  defp infer_value_type(val, _max) when is_integer(val), do: :integer
  defp infer_value_type(val, _max) when is_float(val), do: :float
  defp infer_value_type(val, _max) when is_binary(val), do: :string
  defp infer_value_type(val, _max) when is_boolean(val), do: :boolean
  defp infer_value_type(val, _max) when is_atom(val), do: :atom
  defp infer_value_type(val, _max) when is_list(val), do: :list
  defp infer_value_type(val, _max) when is_map(val), do: :map
  defp infer_value_type(_val, _max), do: :term

  defp segment_may_match?(segment, predicates) do
    zone_maps = Map.get(segment, :zone_maps, %{})
    Enum.all?(predicates, &zone_map_matches?(zone_maps, &1))
  end

  defp zone_map_matches?(zone_maps, {column, op, value}) do
    case Map.get(zone_maps, column) do
      nil ->
        missing_column_matches?(op, value)

      %{min: nil, max: nil, null_count: nulls} ->
        all_null_column_matches?(op, value, nulls)

      %{min: seg_min, max: seg_max, null_count: nulls} ->
        range_matches?(op, value, seg_min, seg_max, nulls)
    end
  end

  defp missing_column_matches?(:eq, nil), do: true
  defp missing_column_matches?(:in, values), do: Enum.any?(values, &is_nil/1)
  defp missing_column_matches?(_op, _value), do: false

  defp all_null_column_matches?(:eq, nil, nulls), do: nulls > 0
  defp all_null_column_matches?(:in, values, nulls), do: nulls > 0 and Enum.any?(values, &is_nil/1)
  defp all_null_column_matches?(_op, _value, _nulls), do: false

  defp range_matches?(:eq, nil, _seg_min, _seg_max, nulls), do: nulls > 0
  defp range_matches?(:eq, val, seg_min, seg_max, _nulls), do: val >= seg_min and val <= seg_max
  defp range_matches?(:gt, val, _seg_min, seg_max, _nulls), do: not is_nil(val) and seg_max > val
  defp range_matches?(:gte, val, _seg_min, seg_max, _nulls), do: not is_nil(val) and seg_max >= val
  defp range_matches?(:lt, val, seg_min, _seg_max, _nulls), do: not is_nil(val) and seg_min < val
  defp range_matches?(:lte, val, seg_min, _seg_max, _nulls), do: not is_nil(val) and seg_min <= val

  defp range_matches?(:in, values, seg_min, seg_max, nulls) do
    Enum.any?(values, fn item ->
      if is_nil(item) do
        nulls > 0
      else
        item >= seg_min and item <= seg_max
      end
    end)
  end

  defp range_matches?(:between, {low, high}, seg_min, seg_max, _nulls) do
    not is_nil(low) and not is_nil(high) and seg_max >= low and seg_min <= high
  end

  defp range_matches?(_unknown_op, _val, _seg_min, _seg_max, _nulls), do: true
end
