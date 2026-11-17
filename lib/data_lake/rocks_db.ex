defmodule DataLake.RocksDB do
  @moduledoc """
  External RocksDB LSM-tree persistence layer supervised by the OTP application.

  Communicates with an embedded `librocksdb` backend (via an Erlang Port backed
  by `rocksdict` in `priv/rocksdb_port.py`) using 4-byte length-prefixed binary
  packets. Provides atomic `WriteBatch` commits, namespace-isolated prefix
  range scans, MemTable flushing to immutable `.sst` SSTable files, and LSM
  compaction.
  """

  use GenServer

  @separator <<0>>

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores `value` under `{namespace, key}` in RocksDB.
  """
  @spec put(String.t(), binary(), term()) :: :ok
  def put(namespace, key, value) when is_binary(namespace) and is_binary(key) do
    GenServer.call(__MODULE__, {:put, namespace, key, value})
  end

  @doc """
  Atomically writes a list of `{key, value}` pairs into `namespace` using a
  single RocksDB `WriteBatch`.
  """
  @spec put_batch(String.t(), [{binary(), term()}]) :: :ok
  def put_batch(_namespace, []), do: :ok

  def put_batch(namespace, kv_list) when is_binary(namespace) and is_list(kv_list) do
    GenServer.call(__MODULE__, {:put_batch, namespace, kv_list})
  end

  @doc """
  Fetches the value stored under `{namespace, key}` from RocksDB.
  """
  @spec get(String.t(), binary()) :: {:ok, term()} | :not_found
  def get(namespace, key) when is_binary(namespace) and is_binary(key) do
    GenServer.call(__MODULE__, {:get, namespace, key})
  end

  @doc """
  Atomically deletes a list of `keys` from `namespace` using a single RocksDB
  `WriteBatch`.
  """
  @spec delete_batch(String.t(), [binary()]) :: :ok
  def delete_batch(_namespace, []), do: :ok

  def delete_batch(namespace, keys) when is_binary(namespace) and is_list(keys) do
    GenServer.call(__MODULE__, {:delete_batch, namespace, keys})
  end

  @doc """
  Deletes all keys matching `prefix` in `namespace` via an atomic `WriteBatch`.
  """
  @spec clear_namespace(String.t(), binary()) :: :ok
  def clear_namespace(namespace, prefix \\ "")
      when is_binary(namespace) and is_binary(prefix) do
    GenServer.call(__MODULE__, {:clear_namespace, namespace, prefix})
  end

  @doc """
  Scans all `{key, value}` pairs in `namespace` starting with `prefix` in
  RocksDB lexicographical order, optionally bounded by `limit`.
  """
  @spec prefix_scan(String.t(), binary(), pos_integer() | :all) :: [{binary(), term()}]
  def prefix_scan(namespace, prefix \\ "", limit \\ :all)
      when is_binary(namespace) and is_binary(prefix) do
    GenServer.call(__MODULE__, {:prefix_scan, namespace, prefix, limit})
  end

  @doc """
  Flushes the active RocksDB MemTable to immutable `.sst` SSTable files on disk.
  """
  @spec flush() :: :ok
  def flush do
    GenServer.call(__MODULE__, :flush, 30_000)
  end

  @doc """
  Triggers full RocksDB LSM-tree compaction across SSTable levels.
  """
  @spec compact() :: :ok
  def compact do
    GenServer.call(__MODULE__, :compact, 30_000)
  end

  @doc """
  Returns physical RocksDB storage metrics including WAL `.log`, `.sst` SSTable,
  and `MANIFEST` files on disk.
  """
  @spec storage_info() :: map()
  def storage_info do
    GenServer.call(__MODULE__, :storage_info)
  end

  @doc """
  Deletes all keys stored in the external RocksDB instance.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset, 30_000)
  end

  @impl true
  def init(opts) do
    storage_dir =
      Keyword.get(opts, :storage_dir, Path.join(System.tmp_dir!(), "data_lake_store"))

    db_path = Keyword.get(opts, :db_path, Path.join(storage_dir, "rocksdb"))
    File.mkdir_p!(db_path)

    port = open_port_worker(db_path)
    {:ok, %{port: port, db_path: db_path}}
  end

  @impl true
  def handle_call({:put, namespace, key, value}, _from, state) do
    raw_key = encode_key(namespace, key)
    raw_val = :erlang.term_to_binary(value)

    request!(state.port, %{
      "op" => "put",
      "key" => Base.encode64(raw_key),
      "val" => Base.encode64(raw_val)
    })

    {:reply, :ok, state}
  end

  def handle_call({:put_batch, namespace, kv_list}, _from, state) do
    items =
      Enum.map(kv_list, fn {key, value} ->
        [
          Base.encode64(encode_key(namespace, key)),
          Base.encode64(:erlang.term_to_binary(value))
        ]
      end)

    request!(state.port, %{"op" => "put_batch", "items" => items})
    {:reply, :ok, state}
  end

  def handle_call({:get, namespace, key}, _from, state) do
    raw_key = encode_key(namespace, key)

    resp =
      request!(state.port, %{
        "op" => "get",
        "key" => Base.encode64(raw_key)
      })

    reply =
      case Map.get(resp, "val") do
        nil -> :not_found
        val_b64 -> {:ok, val_b64 |> Base.decode64!() |> :erlang.binary_to_term()}
      end

    {:reply, reply, state}
  end

  def handle_call({:delete_batch, namespace, keys}, _from, state) do
    encoded_keys =
      Enum.map(keys, fn key ->
        Base.encode64(encode_key(namespace, key))
      end)

    request!(state.port, %{"op" => "delete_batch", "keys" => encoded_keys})
    {:reply, :ok, state}
  end

  def handle_call({:clear_namespace, namespace, prefix}, _from, state) do
    full_prefix = encode_key(namespace, prefix)

    resp =
      request!(state.port, %{
        "op" => "prefix_scan",
        "prefix" => Base.encode64(full_prefix),
        "limit" => nil
      })

    keys = Enum.map(Map.fetch!(resp, "items"), &hd/1)

    if keys != [] do
      request!(state.port, %{"op" => "delete_batch", "keys" => keys})
    end

    {:reply, :ok, state}
  end

  def handle_call({:prefix_scan, namespace, prefix, limit}, _from, state) do
    full_prefix = encode_key(namespace, prefix)
    ns_prefix_len = byte_size(namespace) + byte_size(@separator)
    limit_val = if limit == :all, do: nil, else: limit

    resp =
      request!(state.port, %{
        "op" => "prefix_scan",
        "prefix" => Base.encode64(full_prefix),
        "limit" => limit_val
      })

    results =
      resp
      |> Map.fetch!("items")
      |> Enum.map(fn [k_b64, v_b64] ->
        full_k = Base.decode64!(k_b64)
        <<_ns::binary-size(^ns_prefix_len), key::binary>> = full_k
        val = v_b64 |> Base.decode64!() |> :erlang.binary_to_term()
        {key, val}
      end)

    {:reply, results, state}
  end

  def handle_call(:flush, _from, state) do
    request!(state.port, %{"op" => "flush"})
    {:reply, :ok, state}
  end

  def handle_call(:compact, _from, state) do
    request!(state.port, %{"op" => "compact"})
    {:reply, :ok, state}
  end

  def handle_call(:storage_info, _from, state) do
    resp = request!(state.port, %{"op" => "info"})
    info = Map.fetch!(resp, "info")

    result = %{
      backend: "rocksdb (rocksdict)",
      db_path: info["db_path"],
      files: info["files"],
      sst_files: info["sst_files"],
      wal_files: info["wal_files"],
      manifest_files: info["manifest_files"],
      total_disk_bytes: info["total_disk_bytes"]
    }

    {:reply, result, state}
  end

  def handle_call(:reset, _from, state) do
    request!(state.port, %{"op" => "clear_all"})
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    {:stop, {:rocksdb_port_exited, status}, state}
  end

  @impl true
  def terminate(_reason, %{port: port}) do
    if Port.info(port) != nil do
      Port.close(port)
    end

    :ok
  end

  defp encode_key(namespace, key) do
    namespace <> @separator <> key
  end

  defp open_port_worker(db_path) do
    python_exe =
      System.find_executable("python") ||
        System.find_executable("python3") ||
        raise "Python executable not found in PATH for RocksDB persistence port"

    script_path = resolve_script_path()

    Port.open({:spawn_executable, python_exe}, [
      :binary,
      {:packet, 4},
      :use_stdio,
      :exit_status,
      args: ["-u", script_path, db_path]
    ])
  end

  defp resolve_script_path do
    app_priv = Application.app_dir(:data_lake, "priv/rocksdb_port.py")

    if File.exists?(app_priv) do
      app_priv
    else
      Path.expand("../../priv/rocksdb_port.py", __DIR__)
    end
  end

  defp request!(port, payload) do
    Port.command(port, JSON.encode!(payload))

    receive do
      {^port, {:data, raw_resp}} ->
        resp = JSON.decode!(raw_resp)

        if resp["ok"] do
          resp
        else
          raise "RocksDB error: #{inspect(resp["error"])}"
        end

      {^port, {:exit_status, status}} ->
        raise "RocksDB port exited unexpectedly with status #{status}"
    after
      30_000 ->
        raise "RocksDB port request timed out"
    end
  end
end
