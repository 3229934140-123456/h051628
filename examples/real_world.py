"""
完整真实场景示例 - 验证一致性未被破坏

包含以下场景 (5 段独立演示):
1) Leader 写入 & Follower 自动转发写入
2) Watch 断线重连 & 按 revision 补历史事件
3) 锁持有者崩溃 (停止心跳) → 租约过期 → 新客户端接手锁
4) 事务条件失败 (Compare-And-Put version 不匹配) 与成功
5) 全链路一致性校验: 3 节点状态机最终完全一致
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import threading
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("consensus").setLevel(logging.WARNING)
logging.getLogger("lease").setLevel(logging.WARNING)
logging.getLogger("kvstore").setLevel(logging.WARNING)
logging.getLogger("watch").setLevel(logging.WARNING)

from common import ErrorCode, WatchEventType
from node import Node, ClusterSimulator, RaftConfig
from client import DistributedClient as Client, WatchCompactedException, DistributedLock


def build_cluster(node_count: int = 3) -> ClusterSimulator:
    """构建一个包含 node_count 个节点的集群"""
    cfg = RaftConfig(
        election_timeout_min=0.15,
        election_timeout_max=0.3,
        heartbeat_interval=0.05,
    )
    node_ids = [f"node_{i+1}" for i in range(node_count)]
    nodes = []
    for i in range(node_count):
        peers = [(node_ids[j], f"addr_{node_ids[j]}") for j in range(node_count) if j != i]
        node = Node(node_ids[i], peers, raft_config=cfg)
        nodes.append(node)
    sim = ClusterSimulator(nodes)
    return sim


def print_section(title: str, symbol: str = "="):
    print(f"\n{symbol * 3} {title} {symbol * 50}")


def main():
    print("\n" + "=" * 70)
    print("  分布式协调服务 - 真实场景演示")
    print("=" * 70)

    sim = build_cluster(3)
    sim.start_all()
    sim.wait_for_leader(timeout=10.0)

    # ================================================================
    print_section("场景 1: Leader 写入 & Follower 自动转发")
    # ================================================================

    # 客户端 1: 直接连接任意节点 (可能是 Follower)
    client_follower = Client(sim, session_timeout=30)
    client_follower.connect()
    print(f"  [客户端] 连接成功, session_id={client_follower.session_id[:8]}...")

    # Follower 自动转发写请求
    resp = client_follower.put("/config/db/host", "10.0.0.1")
    print(f"  [写入] /config/db/host = '10.0.0.1' -> success={resp.success}")
    resp = client_follower.put("/config/db/port", 5432)
    print(f"  [写入] /config/db/port = 5432 -> success={resp.success}")

    # 从所有节点读取都一致
    time.sleep(0.3)
    for nid in sorted(sim.nodes.keys()):
        node = sim.nodes[nid]
        gr = node.kv_get("/config/db/host")
        print(f"  [读节点 {nid}] /config/db/host = {gr.data.get('value') if gr.success else 'N/A'}")

    # ================================================================
    print_section("场景 2: Watch 断线重连 & 补历史事件 (可恢复消费)")
    # ================================================================

    watcher = client_follower.watch_prefix("/config/db/", start_revision=0)
    print(f"  [Watch] 创建 watcher, watch_id={watcher.watch_id}, start_revision=0")

    # 先读取历史事件 (create_watch 立即返回的)
    events = watcher.collect(timeout=1.5, max_events=10)
    print(f"  [Watch] 首次获取到 {len(events)} 个历史事件:")
    for ev in events:
        print(f"    -> rev={ev.revision} {ev.event_type.value} {ev.key}={ev.value}")

    last_rev = watcher.last_revision
    print(f"  [Watch] 记录当前 last_revision={last_rev}")

    # 模拟断线: 不消费, 但后端继续写入
    print("  [模拟断线] 暂停消费 1.2 秒, 期间写入 5 个新 KV ...")
    for i in range(1, 6):
        client_follower.put(f"/config/db/param{i}", f"value_{i}")
    time.sleep(1.2)

    # 继续消费 -> 补回丢失事件
    events = watcher.collect(timeout=2.5, max_events=20)
    print(f"  [Watch] 恢复连接后收到 {len(events)} 个事件:")
    for ev in events:
        print(f"    -> rev={ev.revision} {ev.event_type.value} {ev.key}={ev.value}")
    print(f"  [Watch] 恢复后 last_revision={watcher.last_revision}")

    # 测试超龄错误
    print_section("场景 2.1: Watch 超龄 (start_revision 已超出保留窗口)")
    status = client_follower.watch_status()
    print(f"  [Watch] 当前窗口: compact_rev={status.get('compact_revision')}, head_rev={status.get('head_revision')}")
    try:
        # 用一个远超过 head_revision 的 revision 触发错误
        bad_rev = max(0, status.get('head_revision', 100) + 10000)
        bad_watcher = client_follower.watch_prefix("/config/db/", start_revision=bad_rev)
        # 尝试 collect 看是否触发
        bad_watcher.collect(timeout=0.5, max_events=1)
        print("  [Watch] 没有触发超龄 (窗口较大或实现限制)")
        bad_watcher.cancel()
    except WatchCompactedException as e:
        print(f"  [Watch] ✅ 正确触发超龄异常: requested_rev={e.requested_revision}, compact_rev={e.compact_revision}, lost={e.lost_events}")
    except Exception as e:
        print(f"  [Watch] 其它异常: {type(e).__name__}: {e}")

    watcher.cancel()

    # ================================================================
    print_section("场景 3: 锁持有者崩溃释放 & 新客户端接手")
    # ================================================================

    # 客户端 A 获取锁 (带较短 TTL)
    client_a = Client(sim, session_timeout=2)
    client_a.connect()
    lock_a = client_a.get_lock("/locks/database", ttl=2)

    print("  [客户端 A] 尝试获取锁 ...")
    got_lock = lock_a.acquire(timeout=5)
    assert got_lock, "客户端 A 应该拿到锁"
    print(f"  [客户端 A] ✅ 拿到锁 (TTL=2s)")

    # 验证锁键在 KV 中
    leader = sim.get_leader()
    resp = leader.kv_get("/locks/database")
    print(f"  [KV] 锁键存在? {resp.success}, value={resp.data.get('value') if resp.success else None}")

    # 模拟崩溃: 停止锁的心跳 + 关闭客户端 (停止续期)
    print("  [模拟崩溃] 停止锁 A 的续期线程, 关闭客户端 A (模拟进程挂掉)...")
    lock_a._stop_heartbeat.set()
    if lock_a._heartbeat_thread:
        lock_a._heartbeat_thread.join(timeout=2.0)
    client_a.close()

    # 等待租约过期 (TTL=2s + 额外时间)
    print("  [等待过期] 等待租约过期 + 清理...")
    time.sleep(4.0)

    # 验证锁键已自动消失
    leader = sim.get_leader()
    resp = leader.kv_get("/locks/database")
    print(f"  [KV] 过期后锁键存在? {resp.success} -> {resp.data.get('value') if resp.success else '已删除 ✅'}")

    # 客户端 B 接手
    client_b = Client(sim, session_timeout=10)
    client_b.connect()
    lock_b = client_b.get_lock("/locks/database", ttl=5)
    print("  [客户端 B] 尝试获取同名锁 ...")
    got_lock_b = lock_b.acquire(timeout=5)
    assert got_lock_b, "客户端 B 应该在旧锁过期后拿到新锁"
    print(f"  [客户端 B] ✅ 成功接手锁!")

    # 释放
    lock_b.release()
    print("  [客户端 B] 主动释放锁")
    client_b.close()

    # ================================================================
    print_section("场景 4: 事务 Compare-And-Put 条件失败 & 成功")
    # ================================================================

    client_c = Client(sim, session_timeout=10)
    client_c.connect()

    # 初始写入
    resp = client_c.put("/counter/visits", 100)
    item = resp.data.get("item")
    current_version = item.get("version") if isinstance(item, dict) else None
    print(f"  [初始化] /counter/visits = 100, version={current_version}")

    # CAS 成功: 正确版本号
    resp_ok = client_c.compare_and_put("/counter/visits", current_version, 101)
    print(f"  [CAS 成功] version=={current_version} → 写入 101: succeeded={resp_ok.data.get('succeeded')}, revision={resp_ok.data.get('revision')}")
    assert resp_ok.data.get("succeeded") is True, "CAS 应该成功"

    # CAS 失败: 过时版本号
    resp_fail = client_c.compare_and_put("/counter/visits", current_version, 999)
    print(f"  [CAS 失败] version=={current_version} (过时) → 写入 999: succeeded={resp_fail.data.get('succeeded')}, revision={resp_fail.data.get('revision')}")
    assert resp_fail.data.get("succeeded") is False, "CAS 应该失败"
    failed_info = resp_fail.data.get("failed_comparison")
    if failed_info:
        print(f"    失败比较: key={failed_info['key']}, op={failed_info['op']}, expected={failed_info['expected']}")

    # Compare-And-Put-If-Not-Exists (选主抢占)
    print()
    print("  [选主场景] 两个节点抢占 /election/leader:")
    # 确保不存在
    try:
        client_c.delete("/election/leader")
    except Exception:
        pass
    time.sleep(0.2)

    resp1 = client_c.compare_and_put_if_not_exists("/election/leader", {"id": "server_1", "ts": time.time()}, ttl=10)
    print(f"    server_1 抢占: succeeded={resp1.data.get('succeeded')}, revision={resp1.data.get('revision')}")
    assert resp1.data.get("succeeded") is True

    resp2 = client_follower.compare_and_put_if_not_exists("/election/leader", {"id": "server_2", "ts": time.time()}, ttl=10)
    print(f"    server_2 抢占 (应失败): succeeded={resp2.data.get('succeeded')}, revision={resp2.data.get('revision')}")
    assert resp2.data.get("succeeded") is False
    print(f"    ✅ 只有 1 个抢占者成功, 一致性未破坏")

    # ================================================================
    print_section("场景 5: 全链路一致性校验 - 所有节点状态机一致")
    # ================================================================

    leader = sim.get_leader()
    leader_rev = leader.kv.revision
    print(f"  [Leader {leader.node_id}] 当前 revision = {leader_rev}")
    time.sleep(0.4)

    consistent = True
    for nid in sorted(sim.nodes.keys()):
        node = sim.nodes[nid]
        rev = node.kv.revision
        n_keys = len(node.kv.get_all().data.get("kvs", []))
        role = "Leader" if node.consensus.is_leader() else "Follower"
        print(f"  [节点 {nid} {role}] revision={rev}, keys={n_keys}")
        if rev != leader_rev:
            print(f"    ❌ revision 不一致! Leader={leader_rev}, 本节点={rev}")
            consistent = False
    if consistent:
        print(f"\n  ✅ 全部 {len(sim.nodes)} 个节点状态机 revision 完全一致!")
    else:
        print(f"\n  ❌ 存在不一致节点")

    print_section("最终状态 - Leader KV 快照")
    leader_snap = leader.kv.get_all()
    if leader_snap.success:
        for it in leader_snap.data.get("kvs", []):
            lease_str = f", lease_id={it.lease_id}" if it.lease_id > 0 else ""
            print(f"    {it.key} = {it.value}  (ver={it.version}, mod_rev={it.mod_revision}{lease_str})")

    # 清理
    client_c.close()
    client_follower.close()
    sim.stop_all()
    print("\n演示完毕 ✅")


if __name__ == "__main__":
    main()
