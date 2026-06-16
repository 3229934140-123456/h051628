"""
完整真实场景示例 v2 - 验证一致性未被破坏

场景清单:
1) Leader 写入 & Follower 自动转发
2) Watch 断线重连补历史 (含窗口范围/丢失数/建议resync)
3) 锁持有者崩溃 → 租约过期 → 新客户端接手
4) 事务 CAS 成功/失败 + 批量事务配置发布
5) 全链路一致性校验: 每节点key数量+关键key值对比
6) 历史压缩演示: 小窗口 + 过旧revision订阅 (补历史成功 vs 超龄失败)
"""

from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time, threading, logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s", datefmt="%H:%M:%S")
for name in ("consensus", "lease", "kvstore", "watch", "lock", "node", "client"):
    logging.getLogger(name).setLevel(logging.WARNING)

from common import ErrorCode, WatchEventType, TxnCompare, TxnCompareOp, TxnOp, TxnOpType
from node import Node, ClusterSimulator, RaftConfig
from client import DistributedClient as Client, WatchCompactedException


def build_cluster(node_count: int = 3, history_size: int = 100000) -> ClusterSimulator:
    cfg = RaftConfig(election_timeout_min=0.15, election_timeout_max=0.3, heartbeat_interval=0.05)
    node_ids = [f"node_{i+1}" for i in range(node_count)]
    nodes = []
    for i in range(node_count):
        peers = [(node_ids[j], f"addr_{node_ids[j]}") for j in range(node_count) if j != i]
        n = Node(node_ids[i], peers, raft_config=cfg, watch_history_size=history_size)
        nodes.append(n)
    return ClusterSimulator(nodes)


def sec(title: str):
    print(f"\n{'='*3} {title} {'='*55}")


def print_snapshot(node_id: str, snap: dict, role: str = ""):
    print(f"  [{role}{node_id}] revision={snap['revision']}, key_count={snap['key_count']}")
    for k in sorted(snap["keys"].keys()):
        v = snap["keys"][k]
        val_str = str(v["value"])[:40]
        lease_str = f", lease={v['lease_id']}" if v.get("lease_id", 0) > 0 else ""
        print(f"    {k} = {val_str}  (ver={v['version']}, mod_rev={v['mod_revision']}{lease_str})")


def main():
    print("\n" + "=" * 70)
    print("  分布式协调服务 - 真实场景演示 v2")
    print("=" * 70)

    sim = build_cluster(3)
    sim.start_all()
    sim.wait_for_leader(10.0)
    leader = sim.get_leader()
    print(f"  集群启动完成, Leader = {leader.node_id}")

    # ================================================================
    sec("场景 1: Leader 写入 & Follower 自动转发")
    # ================================================================
    c1 = Client(sim, session_timeout=30)
    c1.connect()
    c1.put("/config/db/host", "10.0.0.1")
    c1.put("/config/db/port", 5432)
    c1.put("/config/db/name", "mydb")
    print(f"  [写入] 3 个配置项")
    time.sleep(0.3)
    for nid in sorted(sim.nodes):
        r = sim.nodes[nid].kv_get("/config/db/host")
        print(f"  [读节点 {nid}] /config/db/host = {r.data.get('value') if r.success else 'N/A'}")

    # ================================================================
    sec("场景 2: Watch 断线重连补历史 (含窗口信息)")
    # ================================================================
    w = c1.watch_prefix("/config/db/", start_revision=0)
    print(f"  [Watch] 创建成功, watch_id={w.watch_id}")
    print(f"  [Watch] 窗口: compact_rev={w.compact_revision}, head_rev={w.head_revision}")

    events = w.collect(timeout=1.5, max_events=20)
    print(f"  [Watch] 初始历史事件 {len(events)} 条:")
    for e in events:
        print(f"    rev={e.revision} {e.event_type.value} {e.key}={e.value}")
    last_rev = w.last_revision

    print(f"  [模拟断线] 暂停消费, 期间写入 5 个新配置...")
    for i in range(1, 6):
        c1.put(f"/config/db/param{i}", f"value_{i}")
    time.sleep(1.0)

    events = w.collect(timeout=2.0, max_events=20)
    print(f"  [Watch] 恢复后收到 {len(events)} 条事件:")
    for e in events:
        print(f"    rev={e.revision} {e.event_type.value} {e.key}={e.value}")
    print(f"  [Watch] last_revision: {last_rev} -> {w.last_revision}, 窗口: [{w.compact_revision}, {w.head_revision}]")
    w.cancel()

    # ================================================================
    sec("场景 3: 锁持有者崩溃 → 租约过期 → 新客户端接手")
    # ================================================================
    ca = Client(sim, session_timeout=2)
    ca.connect()
    lock_a = ca.get_lock("/locks/database", ttl=2)
    lock_a.acquire(timeout=5)
    print(f"  [客户端 A] 获取锁成功 (TTL=2s)")
    r = leader.kv_get("/locks/database")
    print(f"  [KV] 锁键存在? {r.success}")

    print("  [模拟崩溃] 停止续期, 关闭客户端 A...")
    lock_a._stop_heartbeat.set()
    if lock_a._heartbeat_thread:
        lock_a._heartbeat_thread.join(timeout=2)
    ca.close()
    time.sleep(3.5)

    r = leader.kv_get("/locks/database")
    print(f"  [KV] 过期后锁键存在? {r.success} -> {'已删除' if not r.success else '仍在'}")

    cb = Client(sim, session_timeout=10)
    cb.connect()
    lock_b = cb.get_lock("/locks/database", ttl=5)
    got = lock_b.acquire(timeout=5)
    print(f"  [客户端 B] 接手锁: {'成功' if got else '失败'}")
    lock_b.release()
    cb.close()

    # ================================================================
    sec("场景 4: 事务 CAS + 批量事务配置发布")
    # ================================================================
    cc = Client(sim, session_timeout=10)
    cc.connect()

    r = cc.put("/app/config_ver", 1)
    ver = r.data.get("item", {}).get("version") if isinstance(r.data.get("item"), dict) else None
    print(f"  [初始化] /app/config_ver = 1, version={ver}")

    r = cc.compare_and_put("/app/config_ver", ver, 2)
    print(f"  [CAS 正确版本] succeeded={r.data.get('succeeded')}, rev={r.data.get('revision')}")

    r = cc.compare_and_put("/app/config_ver", ver, 999)
    print(f"  [CAS 过时版本] succeeded={r.data.get('succeeded')}, failed_cmp={r.data.get('failed_comparison')}")

    # 批量事务: 配置原子发布 (version 匹配后, 一次写入 3 个键)
    print()
    print("  [批量事务] 配置原子发布 (version 匹配后一次写入 3 个配置):")
    r = cc.get("/app/config_ver")
    cur_ver = r.data.get("item", {}).get("version") if isinstance(r.data.get("item"), dict) else None
    print(f"  当前 /app/config_ver version={cur_ver}")

    r = cc.batch_txn(
        comparisons=[TxnCompare(key="/app/config_ver", op=TxnCompareOp.VERSION_EQUAL, value=cur_ver)],
        success_ops=[
            TxnOp(op_type=TxnOpType.PUT, key="/app/config_ver", value=cur_ver + 1 if cur_ver else 2),
            TxnOp(op_type=TxnOpType.PUT, key="/app/feature_flag", value="enabled"),
            TxnOp(op_type=TxnOpType.PUT, key="/app/rate_limit", value=1000),
        ],
        failure_ops=[TxnOp(op_type=TxnOpType.GET, key="/app/config_ver")],
    )
    print(f"  [批量事务] succeeded={r.data.get('succeeded')}, ops_executed={r.data.get('ops_executed')}, rev={r.data.get('revision')}")
    for i, opr in enumerate(r.data.get("op_results", [])):
        print(f"    op[{i}]: {opr.get('op')} {opr.get('key')} -> success={opr.get('success')}")

    # 批量事务条件失败
    print()
    print("  [批量事务] 用过时 version 重试 (应失败):")
    r = cc.batch_txn(
        comparisons=[TxnCompare(key="/app/config_ver", op=TxnCompareOp.VERSION_EQUAL, value=cur_ver)],
        success_ops=[TxnOp(op_type=TxnOpType.PUT, key="/app/feature_flag", value="BAD")],
    )
    print(f"  [批量事务失败] succeeded={r.data.get('succeeded')}, failed_cmp={r.data.get('failed_comparison')}")
    r = cc.get("/app/feature_flag")
    print(f"  [验证] /app/feature_flag = {r.data.get('value') if r.success else 'N/A'} (应该仍是 enabled)")

    # ================================================================
    sec("场景 5: 全链路一致性校验 - 每节点快照对比")
    # ================================================================
    time.sleep(0.4)
    leader = sim.get_leader()
    leader_snap = leader.kv_snapshot()
    print(f"  --- Leader 快照 ---")
    print_snapshot(leader.node_id, leader_snap, role="Leader ")

    all_consistent = True
    for nid in sorted(sim.nodes):
        if nid == leader.node_id:
            continue
        node = sim.nodes[nid]
        snap = node.kv_snapshot()
        ok = snap["key_count"] == leader_snap["key_count"] and snap["revision"] == leader_snap["revision"]
        if not ok:
            all_consistent = False
        diff_keys = []
        for k in leader_snap["keys"]:
            lv = leader_snap["keys"][k]
            fv = snap["keys"].get(k)
            if fv is None:
                diff_keys.append(f"缺失 {k}")
            elif fv["value"] != lv["value"] or fv["version"] != lv["version"]:
                diff_keys.append(f"{k}: L={lv['value']}/v{lv['version']} vs F={fv['value']}/v{fv['version']}")
        extra = set(snap["keys"]) - set(leader_snap["keys"])
        for k in extra:
            diff_keys.append(f"多余 {k}")
        tag = "✅" if ok and not diff_keys else "❌"
        diff_str = f", 差异: {diff_keys[:3]}" if diff_keys else ""
        print(f"  --- Follower {nid} ---")
        print(f"    revision={snap['revision']}, keys={snap['key_count']} {tag}{diff_str}")
        for k in sorted(snap["keys"])[:5]:
            v = snap["keys"][k]
            print(f"    {k} = {str(v['value'])[:30]} (ver={v['version']})")
        if len(snap["keys"]) > 5:
            print(f"    ... 还有 {len(snap['keys']) - 5} 个键")

    if all_consistent:
        print(f"\n  ✅ 全部 {len(sim.nodes)} 个节点快照完全一致!")
    else:
        print(f"\n  ⚠️ 存在 revision 或 key_count 差异 (可能是选举期间 NOOP 日志导致)")

    # 清理
    cc.close()
    c1.close()

    # ================================================================
    sec("场景 6: 历史压缩演示 - 补历史成功 vs 超龄失败")
    # ================================================================
    small_sim = build_cluster(3, history_size=8)
    small_sim.start_all()
    small_sim.wait_for_leader(5.0)
    small_leader = small_sim.get_leader()
    print(f"  [小窗口集群] Leader={small_leader.node_id}, history_size=8")

    c5 = Client(small_sim, session_timeout=10)
    c5.connect()

    print("  [写入] 12 个键 (超出 history_size=8)...")
    for i in range(12):
        c5.put(f"/data/item{i}", f"val_{i}")
    time.sleep(0.3)

    status = c5.watch_status()
    compact_rev = status.get("compact_revision", 0)
    head_rev = status.get("head_revision", 0)
    print(f"  [窗口] compact_rev={compact_rev}, head_rev={head_rev}, window_size={head_rev - compact_rev}")

    # 测试A: 合法 revision
    valid_rev = compact_rev
    print(f"\n  [测试A] 用合法 start_revision={valid_rev} (== compact_rev) 订阅:")
    try:
        w5a = c5.watch_prefix("/data/", start_revision=valid_rev)
        events = w5a.collect(timeout=1.5, max_events=30)
        print(f"    ✅ 补历史成功! 收到 {len(events)} 条事件")
        if events:
            print(f"    首条: rev={events[0].revision} {events[0].key}")
            print(f"    末条: rev={events[-1].revision} {events[-1].key}")
        w5a.cancel()
    except WatchCompactedException as e:
        print(f"    ❌ 意外超龄: {e}")

    # 测试B: 过旧 revision
    stale_rev = max(1, compact_rev - 3)
    compact_exc = None
    print(f"\n  [测试B] 用过旧 start_revision={stale_rev} (< compact_rev={compact_rev}) 订阅:")
    try:
        w5b = c5.watch_prefix("/data/", start_revision=stale_rev)
        w5b.collect(timeout=0.5, max_events=1)
        print(f"    ❌ 没有触发超龄异常")
        w5b.cancel()
    except WatchCompactedException as exc:
        compact_exc = exc
        print(f"    ✅ 正确触发超龄异常!")
        print(f"    请求 revision={exc.requested_revision}")
        print(f"    compact_revision={exc.compact_revision}")
        print(f"    丢失事件数={exc.lost_events}")
        print(f"    建议从 revision={exc.suggest_resync_revision} 重新同步")
        print(f"    当前窗口大小={exc.window_size}")
        print(f"    服务端最新 revision={exc.head_revision}")

    # 测试C: 用建议的 resync_revision
    if compact_exc:
        resync_rev = compact_exc.suggest_resync_revision
        print(f"\n  [测试C] 用建议的 resync_revision={resync_rev} 重新订阅:")
        try:
            w5c = c5.watch_prefix("/data/", start_revision=resync_rev)
            events = w5c.collect(timeout=1.5, max_events=30)
            print(f"    ✅ 重连成功! 收到 {len(events)} 条事件")
            w5c.cancel()
        except WatchCompactedException as ex2:
            print(f"    ❌ 仍然超龄: {ex2}")

    c5.close()
    small_sim.stop_all()
    sim.stop_all()
    print("\n演示完毕 ✅")


if __name__ == "__main__":
    main()
