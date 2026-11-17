defmodule Mix.Tasks.DataLake.Bench do
  @shortdoc "Benchmarks holistic 4D query cost across unpruned and hybrid scans"

  @moduledoc """
  Runs a synthetic multi-segment telemetry workload and prints a four-dimensional
  holistic cost comparison between an unpruned full-column scan and a
  zone-map pruned, column-projected hybrid scan.
  """

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("app.start")
    DataLake.reset!()

    regions = ["us-west", "us-east", "eu-central", "ap-south"]

    for seg <- 0..4 do
      batch =
        for i <- 1..200 do
          id = seg * 200 + i

          %{
            id: id,
            ts: id * 10,
            region: Enum.at(regions, rem(id, 4)),
            service: "engine-#{rem(id, 5)}",
            latency_ms: 5.0 + rem(id, 45) * 0.5,
            status_code: if(rem(id, 20) == 0, do: 500, else: 200),
            trace_payload: String.duplicate("span_#{id}_", 16),
            raw_json: String.duplicate("{\"event_id\":#{id},\"ok\":true}", 8)
          }
        end

      {:ok, 200} = DataLake.ingest(:bench_telemetry, batch, auto_compact: false)
      {:ok, _manifest} = DataLake.compact(:bench_telemetry, sort_by: :ts)
    end

    hot_batch =
      for i <- 1001..1025 do
        %{
          id: i,
          ts: i * 10,
          region: "us-west",
          service: "engine-0",
          latency_ms: 8.5,
          status_code: 200,
          trace_payload: "live_hot_span_#{i}",
          raw_json: "{\"event_id\":#{i}}"
        }
      end

    {:ok, 25} = DataLake.ingest(:bench_telemetry, hot_batch, auto_compact: false)
    :ok = DataLake.rocksdb_flush()
    rdb_info = DataLake.rocksdb_info()

    {:ok, report} =
      DataLake.benchmark_holistic_cost(:bench_telemetry,
        where: [{:ts, :between, {4200, 4600}}],
        select: [:id, :ts, :region, :latency_ms]
      )

    naive = report.naive_full_scan
    hybrid = report.hybrid_pruned_scan
    savings = report.savings

    bytes_pct = pct_saved(naive.storage_bytes_scanned, hybrid.storage_bytes_scanned)
    cpu_pct = pct_saved(naive.cpu_reductions, hybrid.cpu_reductions)

    Mix.shell().info("")
    Mix.shell().info("DataLake 4D Holistic Cost Benchmark (1,000 cold rows across 5 segments + 25 hot rows)")
    Mix.shell().info("Rows returned: #{report.rows_returned}")
    Mix.shell().info("Segments scanned: naive=#{naive.segments_scanned}/#{naive.segments_total}, hybrid=#{hybrid.segments_scanned}/#{hybrid.segments_total} (pruned=#{hybrid.segments_pruned})")
    Mix.shell().info("")
    Mix.shell().info("Dimension               | Naive Full Scan | Hybrid Pruned Scan | Savings")
    Mix.shell().info("------------------------+-----------------+--------------------+--------")
    Mix.shell().info("Startup time (us)       | #{pad(naive.startup_time_us)} | #{pad(hybrid.startup_time_us)} | #{naive.startup_time_us - hybrid.startup_time_us} us")
    Mix.shell().info("Query time (us)         | #{pad(naive.query_time_us)} | #{pad(hybrid.query_time_us)} | #{naive.query_time_us - hybrid.query_time_us} us")
    Mix.shell().info("CPU reductions          | #{pad(naive.cpu_reductions)} | #{pad(hybrid.cpu_reductions)} | #{savings.reductions_saved} (#{cpu_pct}%)")
    Mix.shell().info("Storage bytes scanned   | #{pad(naive.storage_bytes_scanned)} | #{pad(hybrid.storage_bytes_scanned)} | #{savings.bytes_saved} (#{bytes_pct}%)")
    Mix.shell().info("")
    Mix.shell().info("RocksDB Persistence Tier:")
    Mix.shell().info("  Backend: #{rdb_info.backend}")
    Mix.shell().info("  SSTables: #{length(rdb_info.sst_files)} | WAL logs: #{length(rdb_info.wal_files)} | Manifests: #{length(rdb_info.manifest_files)} | Disk bytes: #{rdb_info.total_disk_bytes}")
    Mix.shell().info("")

    DataLake.reset!()
  end

  defp pad(val) do
    val |> to_string() |> String.pad_leading(15)
  end

  defp pct_saved(0, _hybrid), do: 0.0

  defp pct_saved(naive, hybrid) do
    Float.round((naive - hybrid) / naive * 100.0, 1)
  end
end
