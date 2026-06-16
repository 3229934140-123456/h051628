"""
完整真实场景示例 v4 - 验证一致性未被破坏

场景清单:
1) Leader 写入 & Follower 自动转发
2) Watch 断线重连补历史 (含窗口信息)
3) 锁持有者崩溃 → 租约过期 → 新客户端接手
4) 事务 CAS + 批量事务配置发布与回滚 (含失败分支批量读取)
5) 全链路一致性校验: 手动制造短暂落后 → 追平, 前后两次差异报告对比
6) Watch 多客户端恢复 + 前缀/事件类型过滤组合
7) 历史压缩演示: 窗口变化过程 (真实保留事件数) + 补历史 vs 超龄
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


def print_snapshot(node_id: str, snap: dict, role: str = "", max_keys: int = 6):
    print(f"  [{role}{node_id}] revision={snap['revision']}, key_count={snap['key_count']}")
    for k in sorted(snap["keys"].keys())[:max_keys]:
        v = snap["keys"][k]
        val_str = str(v["value"])[:40]
        lease_str = f", lease={v['lease_id']}" if v.get("lease_id", 0) > 0 else ""
        print(f"    {k} = {val_str}  (ver={v['version']}, mod_rev={v['mod_revision']}{lease_str})")
    if len(snap["keys"]) > max_keys:
        print(f"    ... 还有 {len(snap['keys']) - max_keys} 个键")


def compare_snapshots(leader_snap: dict, follower_snaps: dict[str, dict]) -> tuple:
    """
    按 key 内容逐字段对比 leader 和所有 follower 的快照

    区分:
    - 实质差异: value / version / key_count 不同 (数据真正不一致)
    - 元数据差异: mod_revision / revision 不同 (不影响数据正确性)

    返回: (all_consistent, report)
    """
    all_consistent = True
    all_diff_lines = []

    for nid, snap in follower_snaps.items():
        substantive = []
        metadata = []

        if snap["revision"] != leader_snap["revision"]:
            metadata.append(f"revision: Leader={leader_snap['revision']}, Follower={snap['revision']}")

        if snap["key_count"] != leader_snap["key_count"]:
            substantive.append(f"key_count: Leader={leader_snap['key_count']}, Follower={snap['key_count']}")

        for k in sorted(set(list(leader_snap["keys"]) + list(snap["keys"]))):
            lv = leader_snap["keys"].get(k)
            fv = snap["keys"].get(k)
            if lv is None and fv is not None:
                substantive.append(f"{k}: Follower 多余")
            elif fv is None and lv is not None:
                substantive.append(f"{k}: Follower 缺失 (Leader val={lv['value']})")
            else:
                fields_sub = []
                fields_meta = []
                if lv["value"] != fv["value"]:
                    fields_sub.append(f"val: L={lv['value']} vs F={fv['value']}")
                if lv["version"] != fv["version"]:
                    fields_sub.append(f"ver: L={lv['version']} vs F={fv['version']}")
                if lv["mod_revision"] != fv["mod_revision"]:
                    fields_meta.append(f"mod_rev: L={lv['mod_revision']} vs F={fv['mod_revision']}")
                if lv.get("lease_id", 0) != fv.get("lease_id", 0):
                    fields_sub.append(f"lease: L={lv.get('lease_id',0)} vs F={fv.get('lease_id',0)}")
                if fields_sub:
                    substantive.append(f"{k}: {', '.join(fields_sub)}")
                if fields_meta:
                    metadata.append(f"{k}: {', '.join(fields_meta)}")

        if substantive:
            all_consistent = False
            all_diff_lines.append(f"  [{nid}] {len(substantive)} 项实质差异:")
            for d in substantive:
                all_diff_lines.append(f"    - {d}")
        if metadata:
            all_diff_lines.append(f"  [{nid}] {len(metadata)} 项元数据差异:")
            for d in metadata:
                all_diff_lines.append(f"    - {d}")

    report = "\n".join(all_diff_lines) if all_diff_lines else ""
    return all_consistent, report


def main():
    print("\n" + "=" * 70)
    print("  分布式协调服务 - 真实场景演示 v4")
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
    print(f"  [Watch] 创建成功, watch_id={w.watch_id}, 窗口=[{w.compact_revision}, {w.head_revision}]")

    events = w.collect(timeout=1.5, max_events=20)
    print(f"  [Watch] 初始历史事件 {len(events)} 条:")
    for e in events:
        print(f"    rev={e.revision} {e.event_type.value} {e.key}={e.value}")

    print(f"  [模拟断线] 暂停消费, 期间写入 5 个新配置...")
    for i in range(1, 6):
        c1.put(f"/config/db/param{i}", f"value_{i}")
    time.sleep(1.0)

    events = w.collect(timeout=2.0, max_events=20)
    print(f"  [Watch] 恢复后收到 {len(events)} 条事件, last_revision={w.last_revision}")
    w.cancel()

    # ================================================================
    sec("场景 3: 锁持有者崩溃 → 租约过期 → 新客户端接手")
    # ================================================================
    ca = Client(sim, session_timeout=2)
    ca.connect()
    lock_a = ca.get_lock("/locks/database", ttl=2)
    lock_a.acquire(timeout=5)
    print(f"  [客户端 A] 获取锁成功 (TTL=2s)")

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
    sec("场景 4: 事务 CAS + 批量事务配置发布与回滚")
    # ================================================================
    cc = Client(sim, session_timeout=10)
    cc.connect()

    # --- 单键 CAS ---
    r = cc.put("/app/config_ver", 1)
    ver = r.data.get("item", {}).get("version") if isinstance(r.data.get("item"), dict) else None
    print(f"  [初始化] /app/config_ver = 1, version={ver}")

    r = cc.compare_and_put("/app/config_ver", ver, 2)
    print(f"  [CAS 正确版本] succeeded={r.data.get('succeeded')}, rev={r.data.get('revision')}")

    r = cc.compare_and_put("/app/config_ver", ver, 999)
    print(f"  [CAS 过时版本] succeeded={r.data.get('succeeded')}")

    # --- 批量发布 ---
    print()
    print("  [批量发布] config_ver 匹配后一次写入 feature_flag + rate_limit + log_level:")
    r = cc.get("/app/config_ver")
    publish_ver = r.data.get("item", {}).get("version") if isinstance(r.data.get("item"), dict) else None
    print(f"  当前 /app/config_ver version={publish_ver}")

    r = cc.batch_txn(
        comparisons=[TxnCompare(key="/app/config_ver", op=TxnCompareOp.VERSION_EQUAL, value=publish_ver)],
        success_ops=[
            TxnOp(op_type=TxnOpType.PUT, key="/app/config_ver", value=publish_ver + 1 if publish_ver else 2),
            TxnOp(op_type=TxnOpType.PUT, key="/app/feature_flag", value="enabled"),
            TxnOp(op_type=TxnOpType.PUT, key="/app/rate_limit", value=1000),
            TxnOp(op_type=TxnOpType.PUT, key="/app/log_level", value="INFO"),
        ],
        failure_ops=[TxnOp(op_type=TxnOpType.GET, key="/app/config_ver")],
    )
    print(f"  [发布结果] succeeded={r.data.get('succeeded')}, rev={r.data.get('revision')}")
    for i, opr in enumerate(r.data.get("op_results", [])):
        print(f"    op[{i}]: {opr.get('op')} {opr.get('key')}={opr.get('value','')} -> success={opr.get('success')}")

    for k in ("/app/config_ver", "/app/feature_flag", "/app/rate_limit", "/app/log_level"):
        r = cc.get(k)
        print(f"  [验证] {k} = {r.data.get('value') if r.success else 'N/A'}")

    # --- 批量回滚 ---
    print()
    print(f"  [批量回滚] 用新 version={publish_ver + 1 if publish_ver else 2} 回滚:")
    r = cc.batch_txn(
        comparisons=[TxnCompare(key="/app/config_ver", op=TxnCompareOp.VERSION_EQUAL, value=publish_ver + 1 if publish_ver else 2)],
        success_ops=[
            TxnOp(op_type=TxnOpType.PUT, key="/app/config_ver", value=publish_ver),
            TxnOp(op_type=TxnOpType.PUT, key="/app/feature_flag", value="disabled"),
            TxnOp(op_type=TxnOpType.DELETE, key="/app/log_level"),
            TxnOp(op_type=TxnOpType.PUT, key="/app/rate_limit", value=500),
        ],
    )
    print(f"  [回滚结果] succeeded={r.data.get('succeeded')}, rev={r.data.get('revision')}")
    for i, opr in enumerate(r.data.get("op_results", [])):
        print(f"    op[{i}]: {opr.get('op')} {opr.get('key')}={opr.get('value','')} -> success={opr.get('success')}")

    print()
    print("  [回滚后验证]:")
    for k in ("/app/config_ver", "/app/feature_flag", "/app/rate_limit", "/app/log_level"):
        r = cc.get(k)
        val = r.data.get("value") if r.success else "(已删除)"
        print(f"    {k} = {val}")

    # --- 失败分支批量读取: 条件不满足时只读不写 ---
    print()
    print("  [失败分支读取] 用过时 version 尝试发布 (应失败), 但把当前配置读出来:")
    stale_ver = publish_ver + 1 if publish_ver else 2
    r = cc.batch_txn(
        comparisons=[TxnCompare(key="/app/config_ver", op=TxnCompareOp.VERSION_EQUAL, value=stale_ver)],
        success_ops=[TxnOp(op_type=TxnOpType.PUT, key="/app/feature_flag", value="BAD")],
        failure_ops=[
            TxnOp(op_type=TxnOpType.GET, key="/app/config_ver"),
            TxnOp(op_type=TxnOpType.GET, key="/app/feature_flag"),
            TxnOp(op_type=TxnOpType.GET, key="/app/rate_limit"),
        ],
    )
    print(f"  [结果] succeeded={r.data.get('succeeded')} (条件不满足, 走 failure_ops)")
    for i, opr in enumerate(r.data.get("op_results", [])):
        val_str = f"={opr.get('value')}" if opr.get("value") is not None else "(不存在)"
        print(f"    failure_op[{i}]: {opr.get('op')} {opr.get('key')} {val_str}")
    # 确认状态没被误改
    r = cc.get("/app/feature_flag")
    print(f"  [验证] /app/feature_flag = {r.data.get('value') if r.success else 'N/A'} (应该仍是 disabled)")

    # ================================================================
    sec("场景 5: 一致性校验 - 手动制造短暂落后再追平")
    # ================================================================
    time.sleep(0.8)
    leader = sim.get_leader()

    # 找到一个 Follower, 暂时暂停它的日志应用
    follower_ids = [nid for nid in sim.nodes if nid != leader.node_id]
    lagging_id = follower_ids[0]
    lagging_node = sim.nodes[lagging_id]

    # 记录暂停前的 revision
    pre_rev = lagging_node.kv.revision
    print(f"  [准备] 暂停 Follower {lagging_id} 的日志应用 (当前 revision={pre_rev})")
    lagging_node.consensus._paused = True

    # Leader 继续写入
    print(f"  [写入] Leader 写入 3 个新键...")
    cc.put("/consistency/test1", "value1")
    cc.put("/consistency/test2", "value2")
    cc.put("/consistency/test3", "value3")
    time.sleep(0.3)

    # 第一次校验: Follower 还没追上
    print(f"\n  --- 第 1 次校验 (Follower {lagging_id} 暂停中) ---")
    leader_snap = leader.kv_snapshot()
    follower_snaps = {}
    for nid in sorted(sim.nodes):
        if nid == leader.node_id:
            continue
        snap = sim.nodes[nid].kv_snapshot()
        follower_snaps[nid] = snap
        status = "暂停中" if nid == lagging_id else "正常"
        print(f"  [Follower {nid} ({status})] revision={snap['revision']}, keys={snap['key_count']}")

    ok1, report1 = compare_snapshots(leader_snap, follower_snaps)
    if not ok1:
        print(f"\n  ❌ 差异报告 (预期: 暂停的 Follower 落后):")
        print(report1)
    else:
        print(f"\n  ⚠️ 未检测到差异 (暂停可能未生效)")

    # 恢复 Follower
    lagging_node.consensus._paused = False
    print(f"\n  [恢复] Follower {lagging_id} 恢复日志应用")
    time.sleep(1.0)

    # 第二次校验: Follower 已追平
    print(f"\n  --- 第 2 次校验 (Follower {lagging_id} 已恢复) ---")
    leader_snap2 = leader.kv_snapshot()
    follower_snaps2 = {}
    for nid in sorted(sim.nodes):
        if nid == leader.node_id:
            continue
        snap = sim.nodes[nid].kv_snapshot()
        follower_snaps2[nid] = snap
        print(f"  [Follower {nid}] revision={snap['revision']}, keys={snap['key_count']}")

    ok2, report2 = compare_snapshots(leader_snap2, follower_snaps2)
    if ok2:
        print(f"\n  ✅ 追平后全部节点 key 内容完全一致!")
    else:
        print(f"\n  ❌ 仍有差异:")
        print(report2)

    if report2 and ok2:
        print(f"  (仅有元数据差异, 数据内容完全一致)")

    cc.close()
    c1.close()

    # ================================================================
    sec("场景 6: Watch 多客户端恢复 + 前缀/事件类型过滤")
    # ================================================================
    small_sim = build_cluster(3, history_size=10)
    small_sim.start_all()
    small_sim.wait_for_leader(5.0)
    print(f"  [小窗口集群] Leader={small_sim.get_leader().node_id}, history_size=10")

    c6 = Client(small_sim, session_timeout=10)
    c6.connect()

    # --- 前缀 + 事件类型过滤: 只看 /settings/ 前缀下的 PUT 事件 ---
    print()
    print("  [过滤演示] 只订阅 /settings/ 前缀的 PUT 事件 (过滤掉 DELETE):")
    wf = c6.watch_prefix("/settings/", start_revision=0, event_types=[WatchEventType.PUT])
    print(f"  [Watch] 创建过滤 watcher, watch_id={wf.watch_id}")

    # 写入 PUT 事件
    c6.put("/settings/theme", "dark")
    c6.put("/settings/lang", "zh")
    time.sleep(0.3)

    # 删除一个键 (不应出现在过滤后的 watcher 中)
    c6.delete("/settings/theme")
    c6.put("/settings/font", "14px")
    time.sleep(0.3)

    events = wf.collect(timeout=1.5, max_events=20)
    print(f"  [过滤结果] 收到 {len(events)} 条事件 (应该只有 PUT, 没有 DELETE):")
    for e in events:
        print(f"    rev={e.revision} {e.event_type.value} {e.key}={e.value}")

    # 对比: 不带过滤的 watcher 能看到 DELETE
    print()
    print("  [对比] 不带过滤的 watcher:")
    wall = c6.watch_prefix("/settings/", start_revision=0)
    events_all = wall.collect(timeout=1.5, max_events=20)
    delete_count = sum(1 for e in events_all if e.event_type == WatchEventType.DELETE)
    put_count = sum(1 for e in events_all if e.event_type == WatchEventType.PUT)
    print(f"  [全量结果] 收到 {len(events_all)} 条事件: PUT={put_count}, DELETE={delete_count}")
    wall.cancel()
    wf.cancel()

    # --- 多客户端恢复 (保留原来的演示) ---
    print()
    c6a = Client(small_sim, session_timeout=10)
    c6a.connect()
    c6b = Client(small_sim, session_timeout=10)
    c6b.connect()

    wa = c6a.watch_prefix("/events/", start_revision=0)
    wb = c6b.watch_prefix("/events/", start_revision=0)

    print(f"\n  [写入第 1 批] 4 个事件...")
    for i in range(4):
        c6a.put(f"/events/batch1_{i}", f"v{i}")
    time.sleep(0.3)

    ev_a = wa.collect(timeout=1.0, max_events=20)
    ev_b = wb.collect(timeout=1.0, max_events=20)
    checkpoint_a = wa.last_revision
    checkpoint_b = wb.last_revision
    print(f"  [消费者 A] 消费第 1 批: {len(ev_a)} 条, last_rev={checkpoint_a}")
    print(f"  [消费者 B] 消费第 1 批: {len(ev_b)} 条, last_rev={checkpoint_b}")

    print(f"\n  [写入第 2 批] 4 个事件 (总 8, 窗口内)...")
    for i in range(4):
        c6a.put(f"/events/batch2_{i}", f"v{i}")
    time.sleep(0.3)

    ev_a2 = wa.collect(timeout=1.0, max_events=20)
    checkpoint_a = wa.last_revision
    print(f"  [消费者 A] 消费第 2 批: {len(ev_a2)} 条, last_rev={checkpoint_a}")
    print(f"  [消费者 B] 暂停消费, checkpoint={checkpoint_b}")

    print(f"\n  [写入第 3 批] 6 个事件 (总 14, 超出窗口!)...")
    for i in range(6):
        c6a.put(f"/events/batch3_{i}", f"v{i}")
    time.sleep(0.3)

    status = c6a.watch_status()
    print(f"  [当前窗口] compact_rev={status.get('compact_revision')}, head_rev={status.get('head_revision')}")

    wa.cancel()
    try:
        wa2 = c6a.watch_prefix("/events/", start_revision=checkpoint_a)
        ev_r = wa2.collect(timeout=1.5, max_events=30)
        print(f"\n  [消费者 A] 从 checkpoint={checkpoint_a} 恢复: ✅ 补齐 {len(ev_r)} 条历史, last_rev={wa2.last_revision}")
        wa2.cancel()
    except WatchCompactedException as e:
        print(f"\n  [消费者 A] 恢复失败: {e}")

    wb.cancel()
    try:
        wb2 = c6b.watch_prefix("/events/", start_revision=checkpoint_b)
        wb2.collect(timeout=0.5, max_events=1)
        print(f"\n  [消费者 B] 从 checkpoint={checkpoint_b} 恢复: ❌ 未触发超龄")
        wb2.cancel()
    except WatchCompactedException as e:
        print(f"\n  [消费者 B] 从 checkpoint={checkpoint_b} 恢复: ✅ 超龄! 丢失={e.lost_events}, 建议从 rev={e.suggest_resync_revision} 重同步")

    c6a.close()
    c6b.close()
    c6.close()
    small_sim.stop_all()

    # ================================================================
    sec("场景 7: 历史压缩演示 - 窗口变化过程 + 补历史 vs 超龄")
    # ================================================================
    tiny_sim = build_cluster(3, history_size=6)
    tiny_sim.start_all()
    tiny_sim.wait_for_leader(5.0)
    print(f"  [极小窗口集群] Leader={tiny_sim.get_leader().node_id}, history_size=6")

    c7 = Client(tiny_sim, session_timeout=10)
    c7.connect()

    def print_window(label: str):
        s = c7.watch_status()
        cr = s.get("compact_revision", 0)
        hr = s.get("head_revision", 0)
        span = hr - cr
        kept = s.get("history_count", 0)
        cap = s.get("history_size", 0)
        evicted = max(0, kept - span) if kept > 0 else 0
        print(f"  [{label}] compact_rev={cr:>3}, head_rev={hr:>3}, 窗口跨度={span:>2}, 保留事件={kept:>2}/{cap}")

    print_window("初始状态")

    print(f"\n  [写入第 1 批] 3 个键:")
    for i in range(3):
        c7.put(f"/data/a{i}", f"v{i}")
    time.sleep(0.2)
    print_window("第 1 批后")

    print(f"  [写入第 2 批] 3 个键:")
    for i in range(3):
        c7.put(f"/data/b{i}", f"v{i}")
    time.sleep(0.2)
    print_window("第 2 批后 (窗口快满)")

    print(f"  [写入第 3 批] 3 个键 (开始淘汰!)")
    for i in range(3):
        c7.put(f"/data/c{i}", f"v{i}")
    time.sleep(0.2)
    print_window("第 3 批后 (淘汰开始)")

    print(f"  [写入第 4 批] 3 个键:")
    for i in range(3):
        c7.put(f"/data/d{i}", f"v{i}")
    time.sleep(0.2)
    print_window("第 4 批后")

    status = c7.watch_status()
    compact_rev = status.get("compact_revision", 0)

    print(f"\n  [测试 - 合法 revision] start_revision={compact_rev}:")
    try:
        w7a = c7.watch_prefix("/data/", start_revision=compact_rev)
        ev = w7a.collect(timeout=1.5, max_events=30)
        if ev:
            print(f"    ✅ 补历史成功! {len(ev)} 条事件 (rev {ev[0].revision}-{ev[-1].revision})")
        else:
            print(f"    无事件")
        w7a.cancel()
    except WatchCompactedException as e:
        print(f"    ❌ 意外超龄: {e}")

    stale = max(1, compact_rev - 2)
    print(f"\n  [测试 - 过旧 revision] start_revision={stale} (< compact_rev={compact_rev}):")
    try:
        w7b = c7.watch_prefix("/data/", start_revision=stale)
        w7b.collect(timeout=0.5, max_events=1)
        print(f"    ❌ 未触发超龄")
        w7b.cancel()
    except WatchCompactedException as e:
        print(f"    ✅ 正确超龄! 丢失={e.lost_events}, 建议从 rev={e.suggest_resync_revision} 重同步, 窗口大小={e.window_size}")

    c7.close()
    tiny_sim.stop_all()
    sim.stop_all()
    print("\n演示完毕 ✅")


if __name__ == "__main__":
    main()
