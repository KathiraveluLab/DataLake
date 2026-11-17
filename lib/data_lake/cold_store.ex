defmodule DataLake.ColdStore do
  @moduledoc """
  Columnar cold storage tier backed by compressed per-column binary files.

  Stores each compacted segment as `<storage_dir>/<table>/<segment_id>/<column>.col`
  alongside a `_header.bin` metadata descriptor. Query reads load and
  decompress only the projected column files, avoiding I/O and CPU decoding
  for unrequested columns.
  """

  use GenServer

  @absent :__data_lake_absent__
  @header_filename "_header.bin"

  @type table_name :: atom() | String.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Transposes a batch of schema-on-read row maps into compressed column files,
  computes per-column zone maps, and persists the segment directory.
  """
  @spec write_segment(table_name(), String.t(), [map()]) :: {:ok, map()}
  def write_segment(table, segment_id, records)
      when is_binary(segment_id) and is_list(records) and records != [] do
    GenServer.call(__MODULE__, {:write_segment, table, segment_id, records})
  end

  @doc """
  Reads only `projected_columns` (or `:all`) from `segment_metadata`, returning
  `{:ok, rows, bytes_scanned}`.
  """
  @spec read_segment(map(), :all | [term()]) :: {:ok, [map()], non_neg_integer()}
  def read_segment(segment_metadata, projected_columns \\ :all) when is_map(segment_metadata) do
    available_columns = Map.fetch!(segment_metadata, :columns)
    column_files = Map.get(segment_metadata, :column_files, %{})
    segment_dir = Map.fetch!(segment_metadata, :file_path)
    row_count = Map.fetch!(segment_metadata, :row_count)

    cols_to_read =
      case projected_columns do
        :all -> available_columns
        cols when is_list(cols) -> Enum.filter(available_columns, &(&1 in cols))
      end

    if cols_to_read == [] do
      {:ok, List.duplicate(%{}, row_count), 0}
    else
      {col_vectors, bytes_scanned} =
        Enum.reduce(cols_to_read, {[], 0}, fn col, {vecs_acc, bytes_acc} ->
          filename = Map.get(column_files, col, default_column_filename(col))
          col_path = Path.join(segment_dir, filename)
          binary = File.read!(col_path)
          values = :erlang.binary_to_term(binary)
          {[{col, values} | vecs_acc], bytes_acc + byte_size(binary)}
        end)

      rows = reconstruct_rows(Enum.reverse(col_vectors), row_count)
      {:ok, rows, bytes_scanned}
    end
  end

  @doc """
  Removes all stored cold segments under the configured storage directory.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset)
  end

  @impl true
  def init(opts) do
    storage_dir =
      Keyword.get(opts, :storage_dir, Path.join(System.tmp_dir!(), "data_lake_store"))

    File.mkdir_p!(storage_dir)
    {:ok, %{storage_dir: storage_dir}}
  end

  @impl true
  def handle_call({:write_segment, table, segment_id, records}, _from, state) do
    table_dir_name = sanitize_path_segment(to_string(table))
    segment_dir = Path.join([state.storage_dir, table_dir_name, segment_id])
    File.mkdir_p!(segment_dir)

    columns = discover_columns(records)
    column_files = assign_column_filenames(columns)
    row_count = length(records)

    zone_maps =
      Enum.reduce(columns, %{}, fn col, acc ->
        col_values = Enum.map(records, &Map.get(&1, col, @absent))
        encoded = :erlang.term_to_binary(col_values, [:compressed])
        col_path = Path.join(segment_dir, Map.fetch!(column_files, col))
        File.write!(col_path, encoded)

        Map.put(acc, col, compute_zone_map(col_values, byte_size(encoded)))
      end)

    segment_metadata = %{
      segment_id: segment_id,
      table: table,
      row_count: row_count,
      columns: columns,
      column_files: column_files,
      zone_maps: zone_maps,
      file_path: segment_dir,
      created_at: System.system_time(:microsecond)
    }

    header_path = Path.join(segment_dir, @header_filename)
    File.write!(header_path, :erlang.term_to_binary(segment_metadata))

    {:reply, {:ok, segment_metadata}, state}
  end

  def handle_call(:reset, _from, state) do
    if File.exists?(state.storage_dir) do
      state.storage_dir
      |> File.ls!()
      |> Enum.each(fn entry ->
        path = Path.join(state.storage_dir, entry)

        if File.dir?(path) do
          File.rm_rf!(path)
        end
      end)
    end

    {:reply, :ok, state}
  end

  defp discover_columns(records) do
    records
    |> Enum.reduce(MapSet.new(), fn record, acc ->
      record
      |> Map.keys()
      |> Enum.reduce(acc, &MapSet.put(&2, &1))
    end)
    |> MapSet.to_list()
    |> Enum.sort()
  end

  defp assign_column_filenames(columns) do
    {mapping, _used} =
      Enum.reduce(Enum.with_index(columns), {%{}, MapSet.new()}, fn {col, idx}, {acc, used} ->
        candidate = default_column_filename(col)

        filename =
          if MapSet.member?(used, candidate) do
            "col_#{idx}_#{candidate}"
          else
            candidate
          end

        {Map.put(acc, col, filename), MapSet.put(used, filename)}
      end)

    mapping
  end

  defp default_column_filename(col) do
    sanitize_path_segment(to_string(col)) <> ".col"
  end

  defp sanitize_path_segment(name) do
    cleaned = String.replace(name, ~r/[^a-zA-Z0-9_-]/, "_")
    if cleaned == "", do: "col", else: cleaned
  end

  defp compute_zone_map(values, byte_size) do
    {non_nulls, null_count} =
      Enum.reduce(values, {[], 0}, fn
        @absent, {vals, nulls} -> {vals, nulls + 1}
        nil, {vals, nulls} -> {vals, nulls + 1}
        val, {vals, nulls} -> {[val | vals], nulls}
      end)

    {min_val, max_val} =
      case non_nulls do
        [] -> {nil, nil}
        [first | rest] -> Enum.reduce(rest, {first, first}, &{min(&1, elem(&2, 0)), max(&1, elem(&2, 1))})
      end

    %{
      min: min_val,
      max: max_val,
      null_count: null_count,
      byte_size: byte_size
    }
  end

  defp reconstruct_rows([], row_count), do: List.duplicate(%{}, row_count)

  defp reconstruct_rows(col_vectors, row_count) do
    columns = Enum.map(col_vectors, &elem(&1, 0))
    vectors = Enum.map(col_vectors, &elem(&1, 1))

    vectors
    |> Enum.zip()
    |> Enum.take(row_count)
    |> Enum.map(fn row_tuple ->
      columns
      |> Enum.zip(Tuple.to_list(row_tuple))
      |> Enum.reduce(%{}, fn
        {_col, @absent}, acc -> acc
        {col, val}, acc -> Map.put(acc, col, val)
      end)
    end)
  end
end

