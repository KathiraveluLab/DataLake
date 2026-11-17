# DataLake

Minimal hybrid data lake platform implemented in Elixir OTP with RocksDB as the external LSM persistence tier.

## Architecture

The platform separates ingestion, metadata management, external LSM persistence, columnar cold storage, and query planning across supervised OTP components:

1. `DataLake.RocksDB` (`lib/data_lake/rocks_db.ex` and `priv/rocksdb_port.py`): External RocksDB LSM-tree persistence layer supervised over an Erlang Port (`{:packet, 4}`). Provides atomic `WriteBatch` commits, namespace-isolated prefix range scans, MemTable flushing (`flush/0`) to immutable `.sst` SSTable files, and compaction (`compact/0`).
2. `DataLake.Catalog` (`lib/data_lake/catalog.ex`): Transactional metadata and manifest catalog backed by in-memory ETS read caches with atomic write-through persistence in RocksDB. Tracks versioned segment manifests, schema lineage, and per-column zone maps (`min`, `max`, `null_count`, `byte_size`) to enable sub-millisecond partition pruning and snapshot time-travel queries.
3. `DataLake.HotStore` (`lib/data_lake/hot_store.ex`): High-velocity write buffer backed by an `:ordered_set` ETS table and persisted durably to RocksDB on every batch ingest. Supports schema-on-read map ingestion, low-latency point lookups by ID (`get_by_id/2`), atomic draining, and crash recovery via `reload_from_disk!/0`.
4. `DataLake.ColdStore` (`lib/data_lake/cold_store.ex`): Columnar storage tier that transposes batches into compressed per-column binary files (`<column>.col`) alongside segment headers (`_header.bin`). Queries read only the column files required by projection, filter predicates, and aggregations.
5. `DataLake.Compactor` (`lib/data_lake/compactor.ex`): Compaction worker that drains hot buffers (removing drained records from RocksDB hot keys), optionally clusters records via `sort_by`, writes compressed columnar segments, and commits updated snapshot manifests in the catalog.
6. `DataLake.QueryRouter` (`lib/data_lake/query_router.ex`): Hybrid query planner and execution engine supporting zone-map pruning, physical column projection, time-travel snapshot reads (`:version`), and grouped aggregations (`:group_by`, `:aggregate`) while instrumenting four cost dimensions: startup planning time (`startup_time_us`), query execution time (`query_time_us`), BEAM CPU reductions (`cpu_reductions`), and storage bytes scanned (`storage_bytes_scanned`).

## Usage

```elixir
# Ingest schema-on-read records into the RocksDB-backed hot write buffer
{:ok, 2} = DataLake.ingest(:events, [
  %{id: 1, ts: 100, region: "us-west", amount: 19.99},
  %{id: 2, ts: 105, region: "ak", amount: 42.50}
])

# Fast point lookup across hot buffer and cold segments
{:ok, record} = DataLake.get(:events, 2)

# Flush RocksDB MemTable to immutable SSTable files and inspect storage
:ok = DataLake.rocksdb_flush()
rdb_info = DataLake.rocksdb_info()

# Compact hot records into a sorted, compressed columnar segment
{:ok, _manifest} = DataLake.compact(:events, sort_by: :ts)

# Execute a pruned, column-projected query
{:ok, %{rows: rows, metrics: metrics}} =
  DataLake.query(:events,
    where: [{:ts, :gte, 102}],
    select: [:id, :amount]
  )

# Execute a grouped aggregation and time-travel snapshot query
{:ok, %{rows: summary}} =
  DataLake.query(:events,
    version: 1,
    group_by: :region,
    aggregate: [{:count, :id}, {:sum, :amount}]
  )

# Inspect schema lineage across snapshots
lineage = DataLake.schema(:events)

# Compare unpruned full-column scan vs. pruned column-projected hybrid scan
{:ok, benchmark} =
  DataLake.benchmark_holistic_cost(:events,
    where: [{:ts, :gte, 102}],
    select: [:id, :amount]
  )
```

## Running Tests and Benchmarks

Requirements: Erlang/OTP 28+, Elixir 1.18+, and Python `rocksdict` (`python -m pip install rocksdict`).

```bash
mix compile --warnings-as-errors
mix test
mix data_lake.bench
```

## Alaska Software Internship Directory

A verified directory of software engineering, database systems, research, and remote game backend internship portals for Alaska students is maintained in [alaska_software_internships.md](alaska_software_internships.md). Run the link verification script to re-validate HTTP 200 status across all URLs:

```bash
python verify_internship_links.py
```
