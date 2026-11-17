defmodule DataLake.QueryRouter do
  @moduledoc """
  Hybrid query planner and execution engine across hot write buffers and
  columnar cold segments.

  Supports zone-map pruning, physical column projection, time-travel snapshot
  queries, and grouped aggregations while instrumenting four holistic cost
  dimensions: startup planning latency, storage bytes scanned, BEAM CPU
  reductions, and query execution time.
  """

  alias DataLake.{Catalog, ColdStore, HotStore}

  @type table_name :: atom() | String.t()

  @doc """
  Executes a query against `table` across compacted cold segments and uncompacted
  hot records.

  ## Options

    * `:select` - `:all` (default) or a list of column keys to project.
    * `:where` - list of `{column, op, value}` predicates (`:eq`, `:gt`, `:gte`, `:lt`, `:lte`, `:in`, `:between`).
    * `:version` - optional integer snapshot version for reproducible time-travel queries.
    * `:include_hot` - whether to union uncompacted records from `DataLake.HotStore`
      (defaults to `true`, or `false` when `:version` is specified).
    * `:use_zone_maps` - whether to prune cold segments using `DataLake.Catalog` zone maps (default `true`).
    * `:group_by` - optional column key or list of column keys to group results.
    * `:aggregate` - optional list of aggregate specs (`{:count, col}`, `{:sum, col}`,
      `{:avg, col}`, `{:min, col}`, `{:max, col}`, or `{op, col, alias_key}`).
  """
  @spec execute(table_name(), keyword()) :: {:ok, %{rows: [map()], metrics: map()}}
  def execute(table, opts \\ []) do
    select = Keyword.get(opts, :select, :all)
    where = Keyword.get(opts, :where, [])
    version = Keyword.get(opts, :version)
    include_hot = Keyword.get(opts, :include_hot, is_nil(version))
    use_zone_maps = Keyword.get(opts, :use_zone_maps, true)
    group_by = normalize_group_by(Keyword.get(opts, :group_by))
    aggregate = Keyword.get(opts, :aggregate, [])

    total_start_us = System.monotonic_time(:microsecond)
    {:reductions, start_reductions} = Process.info(self(), :reductions)

    plan_start_us = System.monotonic_time(:microsecond)

    catalog_opts = if is_nil(version), do: [], else: [version: version]

    {kept_segments, pruned_count, segments_total} =
      if use_zone_maps do
        {kept, pruned} = Catalog.prune_segments(table, where, catalog_opts)
        {kept, pruned, length(kept) + pruned}
      else
        all_segments = Catalog.list_segments(table, catalog_opts)
        {all_segments, 0, length(all_segments)}
      end

    plan_end_us = System.monotonic_time(:microsecond)
    startup_time_us = plan_end_us - plan_start_us

    is_aggregation = group_by != [] or aggregate != []
    required_columns = compute_required_columns(select, where, group_by, aggregate, is_aggregation)

    {cold_rows, cold_bytes_scanned} =
      Enum.reduce(kept_segments, {[], 0}, fn segment, {rows_acc, bytes_acc} ->
        {:ok, seg_rows, seg_bytes} = ColdStore.read_segment(segment, required_columns)

        filtered = Enum.filter(seg_rows, &row_matches?(&1, where))
        projected = if is_aggregation, do: filtered, else: Enum.map(filtered, &project_row(&1, select))

        {Enum.reverse(projected, rows_acc), bytes_acc + seg_bytes}
      end)

    cold_rows = Enum.reverse(cold_rows)

    {hot_rows, hot_bytes_scanned, hot_rows_scanned} =
      if include_hot do
        hot_records = HotStore.read_all(table)

        bytes =
          Enum.reduce(hot_records, 0, fn rec, acc ->
            acc + :erlang.external_size(rec)
          end)

        filtered = Enum.filter(hot_records, &row_matches?(&1, where))
        projected = if is_aggregation, do: filtered, else: Enum.map(filtered, &project_row(&1, select))

        {projected, bytes, length(hot_records)}
      else
        {[], 0, 0}
      end

    combined_rows = cold_rows ++ hot_rows

    rows =
      if is_aggregation do
        aggregate_rows(combined_rows, group_by, aggregate)
      else
        combined_rows
      end

    exec_end_us = System.monotonic_time(:microsecond)
    {:reductions, end_reductions} = Process.info(self(), :reductions)

    query_time_us = exec_end_us - plan_end_us
    total_time_us = exec_end_us - total_start_us
    cpu_reductions = max(end_reductions - start_reductions, 0)
    storage_bytes_scanned = cold_bytes_scanned + hot_bytes_scanned

    metrics = %{
      startup_time_us: startup_time_us,
      query_time_us: query_time_us,
      total_time_us: total_time_us,
      cpu_reductions: cpu_reductions,
      storage_bytes_scanned: storage_bytes_scanned,
      cold_bytes_scanned: cold_bytes_scanned,
      hot_bytes_scanned: hot_bytes_scanned,
      segments_total: segments_total,
      segments_scanned: length(kept_segments),
      segments_pruned: pruned_count,
      hot_rows_scanned: hot_rows_scanned,
      rows_returned: length(rows)
    }

    {:ok, %{rows: rows, metrics: metrics}}
  end

  defp normalize_group_by(nil), do: []
  defp normalize_group_by(cols) when is_list(cols), do: cols
  defp normalize_group_by(col), do: [col]

  defp compute_required_columns(:all, _where, _group_by, _aggregate, false), do: :all

  defp compute_required_columns(select, where, group_by, aggregate, _is_aggregation) do
    select_cols = if is_list(select), do: select, else: []
    where_cols = Enum.map(where, &elem(&1, 0))

    agg_cols =
      aggregate
      |> Enum.map(fn
        {_op, col} -> col
        {_op, col, _alias} -> col
      end)
      |> Enum.reject(&(&1 == :*))

    Enum.uniq(select_cols ++ where_cols ++ group_by ++ agg_cols)
  end

  defp aggregate_rows([], [], aggregate) do
    if aggregate == [] do
      []
    else
      [compute_aggregates([], aggregate)]
    end
  end

  defp aggregate_rows(rows, [], aggregate) do
    [compute_aggregates(rows, aggregate)]
  end

  defp aggregate_rows(rows, group_by, aggregate) do
    rows
    |> Enum.group_by(fn row ->
      Enum.map(group_by, fn col -> {col, Map.get(row, col)} end)
    end)
    |> Enum.map(fn {group_pairs, group_rows} ->
      group_map = Map.new(group_pairs)
      agg_map = compute_aggregates(group_rows, aggregate)
      Map.merge(group_map, agg_map)
    end)
  end

  defp compute_aggregates(rows, aggregate) do
    Enum.reduce(aggregate, %{}, fn spec, acc ->
      {op, col, out_key} = normalize_agg_spec(spec)
      Map.put(acc, out_key, compute_agg_value(op, col, rows))
    end)
  end

  defp normalize_agg_spec({op, col, out_key}), do: {op, col, out_key}

  defp normalize_agg_spec({op, :*}), do: {op, :*, :"#{op}_all"}

  defp normalize_agg_spec({op, col}) when is_atom(col) do
    {op, col, :"#{op}_#{col}"}
  end

  defp normalize_agg_spec({op, col}) do
    {op, col, "#{op}_#{col}"}
  end

  defp compute_agg_value(:count, :*, rows), do: length(rows)

  defp compute_agg_value(:count, col, rows) do
    Enum.count(rows, fn row -> not is_nil(Map.get(row, col)) end)
  end

  defp compute_agg_value(:sum, col, rows) do
    rows
    |> extract_non_nil(col)
    |> Enum.sum()
  end

  defp compute_agg_value(:avg, col, rows) do
    vals = extract_non_nil(rows, col)

    case vals do
      [] -> nil
      _ -> Enum.sum(vals) / length(vals)
    end
  end

  defp compute_agg_value(:min, col, rows) do
    case extract_non_nil(rows, col) do
      [] -> nil
      vals -> Enum.min(vals)
    end
  end

  defp compute_agg_value(:max, col, rows) do
    case extract_non_nil(rows, col) do
      [] -> nil
      vals -> Enum.max(vals)
    end
  end

  defp extract_non_nil(rows, col) do
    Enum.flat_map(rows, fn row ->
      case Map.get(row, col) do
        nil -> []
        val -> [val]
      end
    end)
  end

  defp row_matches?(_row, []), do: true

  defp row_matches?(row, predicates) do
    Enum.all?(predicates, fn {column, op, target} ->
      val = Map.get(row, column)
      value_matches?(val, op, target)
    end)
  end

  defp value_matches?(nil, :eq, target), do: is_nil(target)
  defp value_matches?(nil, :in, targets), do: Enum.any?(targets, &is_nil/1)
  defp value_matches?(nil, _op, _target), do: false

  defp value_matches?(val, :eq, target), do: val == target
  defp value_matches?(val, :gt, target), do: not is_nil(target) and val > target
  defp value_matches?(val, :gte, target), do: not is_nil(target) and val >= target
  defp value_matches?(val, :lt, target), do: not is_nil(target) and val < target
  defp value_matches?(val, :lte, target), do: not is_nil(target) and val <= target
  defp value_matches?(val, :in, targets), do: val in targets

  defp value_matches?(val, :between, {low, high}) do
    not is_nil(low) and not is_nil(high) and val >= low and val <= high
  end

  defp value_matches?(_val, _unknown_op, _target), do: true

  defp project_row(row, :all), do: row
  defp project_row(row, cols) when is_list(cols), do: Map.take(row, cols)
end
