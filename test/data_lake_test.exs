defmodule DataLakeTest do
  use ExUnit.Case, async: false

  setup do
    DataLake.reset!()
    :ok
  end

  test "schema-on-read ingestion into hot store, point lookup, and immediate query" do
    {:ok, 1} = DataLake.ingest(:events, %{id: 1, event: "login", region: "us-west"})
    {:ok, 1} = DataLake.ingest(:events, %{id: 2, event: "purchase", amount: 49.95})

    stats = DataLake.stats(:events)
    assert stats.hot.count == 2
    assert stats.hot.memory_bytes > 0
    assert stats.cold.segments == 0

    assert {:ok, %{id: 2, event: "purchase", amount: 49.95}} = DataLake.get(:events, 2)
    assert :not_found = DataLake.get(:events, 999)

    {:ok, %{rows: rows, metrics: metrics}} =
      DataLake.query(:events, where: [{:id, :eq, 2}], select: [:id, :amount])

    assert rows == [%{id: 2, amount: 49.95}]
    assert metrics.hot_rows_scanned == 2
    assert metrics.segments_scanned == 0
  end

  test "sorted compaction writes columnar segments, updates zone maps, and supports cold point lookup" do
    records =
      for i <- [5, 1, 8, 3, 10, 2, 7, 4, 9, 6] do
        %{id: i, ts: 1000 + i, region: "ak", payload: String.duplicate("x", 40)}
      end

    {:ok, 10} = DataLake.ingest(:telemetry, records)
    {:ok, manifest} = DataLake.compact(:telemetry, sort_by: :ts)

    assert manifest.row_count == 10
    assert manifest.snapshot_version == 1
    assert manifest.zone_maps[:ts].min == 1001
    assert manifest.zone_maps[:ts].max == 1010
    assert manifest.zone_maps[:ts].byte_size > 0
    assert File.exists?(Path.join(manifest.file_path, "ts.col"))
    assert File.exists?(Path.join(manifest.file_path, "_header.bin"))

    assert {:ok, rec} = DataLake.get(:telemetry, 7)
    assert Map.take(rec, [:id, :ts, :region]) == %{id: 7, ts: 1007, region: "ak"}

    {:ok, %{rows: ordered_rows}} = DataLake.query(:telemetry, select: [:ts], include_hot: false)
    assert Enum.map(ordered_rows, & &1.ts) == Enum.to_list(1001..1010)

    stats = DataLake.stats(:telemetry)
    assert stats.hot.count == 0
    assert stats.cold.segments == 1
    assert stats.cold.rows == 10
    assert stats.snapshot.version == 1
  end

  test "zone-map partition pruning skips non-matching segments" do
    batch1 = for i <- 1..10, do: %{id: i, ts: i, region: "us-west"}
    batch2 = for i <- 101..110, do: %{id: i, ts: i, region: "us-east"}
    batch3 = for i <- 201..210, do: %{id: i, ts: i, region: "eu-central"}

    {:ok, 10} = DataLake.ingest(:logs, batch1)
    {:ok, _} = DataLake.compact(:logs)

    {:ok, 10} = DataLake.ingest(:logs, batch2)
    {:ok, _} = DataLake.compact(:logs)

    {:ok, 10} = DataLake.ingest(:logs, batch3)
    {:ok, _} = DataLake.compact(:logs)

    {:ok, %{rows: rows, metrics: metrics}} =
      DataLake.query(:logs, where: [{:ts, :between, {103, 105}}], select: [:id, :ts])

    assert length(rows) == 3
    assert Enum.map(rows, & &1.id) == [103, 104, 105]
    assert metrics.segments_total == 3
    assert metrics.segments_scanned == 1
    assert metrics.segments_pruned == 2
  end

  test "physical column projection reads fewer bytes than full-column scan" do
    records =
      for i <- 1..60 do
        %{
          id: i,
          ts: 1000 + i,
          user_id: "user_#{i}",
          region: "us-west",
          blob_a: String.duplicate("alpha_#{i}_", 20),
          blob_b: String.duplicate("beta_#{i}_", 20)
        }
      end

    {:ok, 60} = DataLake.ingest(:wide_events, records, auto_compact: false)
    {:ok, _} = DataLake.compact(:wide_events)

    {:ok, %{rows: full_rows, metrics: full_metrics}} =
      DataLake.query(:wide_events, select: :all, include_hot: false)

    {:ok, %{rows: proj_rows, metrics: proj_metrics}} =
      DataLake.query(:wide_events, select: [:id, :ts], include_hot: false)

    assert length(full_rows) == 60
    assert length(proj_rows) == 60
    assert Map.keys(hd(proj_rows)) |> Enum.sort() == [:id, :ts]
    assert proj_metrics.cold_bytes_scanned < full_metrics.cold_bytes_scanned
  end

  test "hybrid query combines compacted cold segments and buffered hot records" do
    cold_batch = for i <- 1..5, do: %{id: i, ts: i, source: "cold"}
    hot_batch = for i <- 6..8, do: %{id: i, ts: i, source: "hot"}

    {:ok, 5} = DataLake.ingest(:hybrid_stream, cold_batch)
    {:ok, _} = DataLake.compact(:hybrid_stream)
    {:ok, 3} = DataLake.ingest(:hybrid_stream, hot_batch)

    {:ok, %{rows: rows, metrics: metrics}} =
      DataLake.query(:hybrid_stream, where: [{:ts, :gte, 4}], select: [:id, :source])

    assert Enum.sort_by(rows, & &1.id) == [
             %{id: 4, source: "cold"},
             %{id: 5, source: "cold"},
             %{id: 6, source: "hot"},
             %{id: 7, source: "hot"},
             %{id: 8, source: "hot"}
           ]

    assert metrics.segments_scanned == 1
    assert metrics.hot_rows_scanned == 3
  end

  test "snapshot time-travel queries isolate historical states" do
    {:ok, 2} = DataLake.ingest(:orders, [%{id: 1, amount: 10}, %{id: 2, amount: 20}])
    {:ok, m1} = DataLake.compact(:orders)
    assert m1.snapshot_version == 1

    {:ok, 2} = DataLake.ingest(:orders, [%{id: 3, amount: 30}, %{id: 4, amount: 40}])
    {:ok, m2} = DataLake.compact(:orders)
    assert m2.snapshot_version == 2

    {:ok, 1} = DataLake.ingest(:orders, %{id: 5, amount: 50})

    {:ok, %{rows: v1_rows, metrics: v1_metrics}} = DataLake.query(:orders, version: 1, select: [:id])
    {:ok, %{rows: v2_rows}} = DataLake.query(:orders, version: 2, select: [:id])
    {:ok, %{rows: live_rows}} = DataLake.query(:orders, select: [:id])

    assert Enum.map(v1_rows, & &1.id) == [1, 2]
    assert v1_metrics.hot_rows_scanned == 0
    assert Enum.map(v2_rows, & &1.id) == [1, 2, 3, 4]
    assert Enum.map(live_rows, & &1.id) == [1, 2, 3, 4, 5]
  end

  test "group_by and aggregations compute count, sum, avg, min, and max with column pruning" do
    records = [
      %{id: 1, ts: 100, region: "ak", latency_ms: 10.0, payload: String.duplicate("z", 120)},
      %{id: 2, ts: 120, region: "ak", latency_ms: 20.0, payload: String.duplicate("z", 120)},
      %{id: 3, ts: 140, region: "us-west", latency_ms: 30.0, payload: String.duplicate("z", 120)},
      %{id: 4, ts: 160, region: "us-west", latency_ms: 50.0, payload: String.duplicate("z", 120)}
    ]

    {:ok, 4} = DataLake.ingest(:metrics_agg, records)
    {:ok, _} = DataLake.compact(:metrics_agg)

    {:ok, %{rows: grouped, metrics: agg_metrics}} =
      DataLake.query(:metrics_agg,
        group_by: :region,
        aggregate: [
          {:count, :id},
          {:sum, :latency_ms},
          {:avg, :latency_ms},
          {:min, :ts},
          {:max, :ts}
        ]
      )

    by_region = Map.new(grouped, &{&1.region, &1})
    assert by_region["ak"] == %{region: "ak", count_id: 2, sum_latency_ms: 30.0, avg_latency_ms: 15.0, min_ts: 100, max_ts: 120}
    assert by_region["us-west"] == %{region: "us-west", count_id: 2, sum_latency_ms: 80.0, avg_latency_ms: 40.0, min_ts: 140, max_ts: 160}

    {:ok, %{metrics: full_metrics}} = DataLake.query(:metrics_agg, select: :all, include_hot: false)
    assert agg_metrics.cold_bytes_scanned < full_metrics.cold_bytes_scanned
  end

  test "schema lineage tracks evolving schema-on-read columns across snapshots" do
    {:ok, 1} = DataLake.ingest(:evolving, %{id: 1, region: "ak"})
    {:ok, _} = DataLake.compact(:evolving)

    {:ok, 1} = DataLake.ingest(:evolving, %{id: 2, region: "ak", latency_ms: 12.5})
    {:ok, _} = DataLake.compact(:evolving)

    lineage = DataLake.schema(:evolving)
    assert lineage.latest_version == 2
    assert lineage.segment_count == 2
    assert lineage.columns[:id].first_seen_version == 1
    assert lineage.columns[:id].last_seen_version == 2
    assert lineage.columns[:id].types == [:integer]
    assert lineage.columns[:latency_ms].first_seen_version == 2
    assert lineage.columns[:latency_ms].segment_count == 1
    assert lineage.columns[:latency_ms].types == [:float]
  end

  test "holistic 4D cost benchmark captures startup, query time, reductions, and bytes scanned" do
    for seg <- 0..3 do
      batch =
        for i <- 1..40 do
          id = seg * 100 + i

          %{
            id: id,
            ts: id,
            category: "cat_#{rem(id, 4)}",
            metric: id * 1.5,
            notes: String.duplicate("payload_#{id}_", 15),
            raw_json: String.duplicate("{\"k\":#{id}}", 10)
          }
        end

      {:ok, 40} = DataLake.ingest(:bench_table, batch, auto_compact: false)
      {:ok, _} = DataLake.compact(:bench_table)
    end

    {:ok, report} =
      DataLake.benchmark_holistic_cost(:bench_table,
        where: [{:ts, :between, {210, 220}}],
        select: [:id, :metric]
      )

    assert report.rows_returned == 11
    assert report.hybrid_pruned_scan.segments_pruned == 3
    assert report.hybrid_pruned_scan.segments_scanned == 1
    assert report.hybrid_pruned_scan.startup_time_us >= 0
    assert report.hybrid_pruned_scan.query_time_us >= 0
    assert report.hybrid_pruned_scan.cpu_reductions > 0
    assert report.hybrid_pruned_scan.storage_bytes_scanned > 0
    assert report.savings.bytes_saved > 0
    assert report.savings.reductions_saved > 0
  end
end
