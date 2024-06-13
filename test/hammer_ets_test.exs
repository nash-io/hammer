defmodule ETSTest do
  use ExUnit.Case

  alias Hammer.Backend.ETS
  alias Hammer.Utils

  @table_name :hammer_ets_buckets

  setup _context do
    case :ets.info(@table_name) do
      :undefined ->
        nil

      _ ->
        :ets.delete(@table_name)
    end

    opts = [expiry_ms: 5, cleanup_interval_ms: 5]
    {:ok, hammer_ets_pid} = start_supervised({ETS, opts})
    {:ok, Keyword.put(opts, :pid, hammer_ets_pid)}
  end

  test "count_hit", context do
    pid = context[:pid]
    key = Utils.build_key("one", 200_000)
    assert {:ok, 1, created1} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, 2, created2} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, 3, created3} = ETS.count_hit(pid, key, 200_000)

    assert created1 == created2
    assert created2 == created3
  end

  test "get_bucket", context do
    pid = context[:pid]
    key = Utils.build_key("two", 200_000)
    # With no hits
    assert {:ok, nil} = ETS.get_bucket(pid, key)
    # With one hit
    assert {:ok, 1, _created} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, {{_, "two"}, 1, _, _}} = ETS.get_bucket(pid, key)
    # With two hits
    assert {:ok, 2, _created} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, {{_, "two"}, 2, _, _}} = ETS.get_bucket(pid, key)
  end

  test "delete_buckets", context do
    pid = context[:pid]
    key = Utils.build_key("three", 200_000)
    # With no hits
    assert {:ok, 0} = ETS.delete_buckets(pid, "three")
    # With three hits in same bucket
    assert {:ok, 1, _created} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, 2, _created} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, 3, _created} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, 1} = ETS.delete_buckets(pid, "three")
  end

  test "timeout pruning", context do
    pid = context[:pid]
    expiry_ms = context[:expiry_ms]
    key = Utils.build_key("something-pruned", 200_000)
    assert {:ok, 1, _created} = ETS.count_hit(pid, key, 200_000)
    assert {:ok, {{_, "something-pruned"}, 1, _, _}} = ETS.get_bucket(pid, key)
    :timer.sleep(expiry_ms * 5)
    assert {:ok, nil} = ETS.get_bucket(pid, key)
  end
end
