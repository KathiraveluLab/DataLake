defmodule DataLake.Compactor do
  @moduledoc """
  Compaction worker that drains records from `DataLake.HotStore`, optionally
  clusters them by a sort column, writes compressed columnar segments to
  `DataLake.ColdStore`, and commits immutable manifests to `DataLake.Catalog`.
  """

  use GenServer

  alias DataLake.{Catalog, ColdStore, HotStore}

  @type table_name :: atom() | String.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Synchronously drains the hot buffer for `table`, writes a new columnar
  segment, and registers its manifest in the catalog.

  ## Options

    * `:sort_by` - optional column key to sort drained records before writing
      the segment so zone maps form tighter min/max bounds.
  """
  @spec compact(table_name(), keyword()) :: {:ok, map() | :noop}
  def compact(table, opts \\ []) do
    GenServer.call(__MODULE__, {:compact, table, opts})
  end

  @doc """
  Triggers asynchronous compaction when the hot buffer count for `table`
  exceeds the configured `compaction_threshold`.
  """
  @spec maybe_compact(table_name()) :: :ok
  def maybe_compact(table) do
    GenServer.cast(__MODULE__, {:maybe_compact, table})
  end

  @impl true
  def init(opts) do
    threshold = Keyword.get(opts, :compaction_threshold, 100)
    {:ok, %{compaction_threshold: threshold}}
  end

  @impl true
  def handle_call({:compact, table, opts}, _from, state) do
    result = do_compact(table, opts)
    {:reply, result, state}
  end

  @impl true
  def handle_cast({:maybe_compact, table}, state) do
    if HotStore.count(table).count > state.compaction_threshold do
      do_compact(table, [])
    end

    {:noreply, state}
  end

  defp do_compact(table, opts) do
    case HotStore.drain(table, :all) do
      [] ->
        {:ok, :noop}

      records ->
        sorted_records = maybe_sort(records, Keyword.get(opts, :sort_by))
        segment_id = generate_segment_id()
        {:ok, segment_metadata} = ColdStore.write_segment(table, segment_id, sorted_records)
        Catalog.register_segment(table, segment_metadata)
    end
  end

  defp maybe_sort(records, nil), do: records

  defp maybe_sort(records, sort_col) do
    Enum.sort_by(records, fn rec ->
      val = Map.get(rec, sort_col)
      {is_nil(val), val}
    end)
  end

  defp generate_segment_id do
    ts = System.system_time(:microsecond)
    seq = System.unique_integer([:monotonic, :positive])
    padded_ts = ts |> Integer.to_string() |> String.pad_leading(20, "0")
    padded_seq = seq |> Integer.to_string() |> String.pad_leading(10, "0")
    "seg_#{padded_ts}_#{padded_seq}"
  end
end
