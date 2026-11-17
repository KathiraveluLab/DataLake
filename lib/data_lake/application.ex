defmodule DataLake.Application do
  @moduledoc """
  OTP Application supervisor for the hybrid data lake platform.

  Supervises the metadata catalog, hot write buffer, cold columnar store,
  and background compaction worker using a `:rest_for_one` restart strategy
  so downstream tiers remain consistent with catalog metadata.
  """

  use Application

  @impl true
  def start(_type, _args) do
    storage_dir =
      Application.get_env(
        :data_lake,
        :storage_dir,
        Path.join(System.tmp_dir!(), "data_lake_store")
      )

    compaction_threshold = Application.get_env(:data_lake, :compaction_threshold, 100)

    children = [
      {DataLake.RocksDB, [storage_dir: storage_dir]},
      {DataLake.Catalog, []},
      {DataLake.HotStore, []},
      {DataLake.ColdStore, [storage_dir: storage_dir]},
      {DataLake.Compactor, [compaction_threshold: compaction_threshold]}
    ]

    opts = [strategy: :rest_for_one, name: DataLake.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

