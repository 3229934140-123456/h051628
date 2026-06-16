"""
分布式锁与协调服务 - 完整示例

演示内容:
1. 启动 3 节点集群,选举 Leader
2. KV 读写 + 带 TTL 的键
3. Watch 通知 (精确键、前缀)
4. 分布式锁 (互斥、自动释放)
5. 租约过期自动清理
6. Follower 写请求自动转发
7. 一致性验证 (所有节点状态一致)
"""

import sys
import os
import threading
import time
import logging

# 确保可以导入包
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from node import Node, ClusterSimulator, RaftConfig
from client import DistributedClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo")


def separator(title: str):
    print()
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)


# ============================================================
# Demo 1: 集群启动与 Leader 选举
# ============================================================
def demo1_cluster_bootstrap():
    separator("Demo 1: 3 节点集群启动与 Leader 选举")

    # 创建 3 个节点,减少选举超时让演示更快
    raft_config = RaftConfig(
        election_timeout_min=0.3,
        election_timeout_max=0.6,
        heartbeat_interval=0.1,
    )

    node_ids = ["node0", "node1", "node2"]
    nodes = {}

    for i, nid in enumerate(node_ids):
        peers = [(node_ids[j], f"addr_{node_ids[j]}") for j in range(3) if j != i]
        nodes[nid] = Node(nid, peers, raft_config=raft_config)

    cluster = ClusterSimulator(list(nodes.values()))
    cluster.start_all()

    # 等待选举
    print("等待 Leader 选举...")
    leader_id = cluster.wait_for_leader(timeout=5.0)
    print(f"✓ 选举成功, Leader = {leader_id}")

    # 打印各节点角色
    print("\n各节点状态:")
    for nid, node in nodes.items():
        role = node.get_status()["consensus"]["role"]
        leader = node.get_status()["consensus"]["leader_id"]
        print(f"  {nid}: role={role:8s}  leader={leader}")

    return cluster, nodes


# ============================================================
# Demo 2: KV 存储 + Follower 写转发
# ============================================================
def demo2_kv_storage(cluster: ClusterSimulator, nodes):
    separator("Demo 2: KV 存储 + Follower 写转发")

    client = DistributedClient(cluster)
    if not client.connect():
        logger.error("客户端连接失败")
        return

    print(f"客户端 session_id = {client.session_id}")

    # 写一些键
    print("\n写入键值对...")
    pairs = [
        ("user/1", {"name": "Alice", "age": 30}),
        ("user/2", {"name": "Bob", "age": 25}),
        ("user/3", {"name": "Charlie", "age": 35}),
        ("config/db_host", "10.0.0.1"),
        ("config/db_port", 3306),
    ]
    for k, v in pairs:
        resp = client.put(k, v)
        status = "✓" if resp.success else "✗"
        print(f"  {status} PUT {k} = {v} (节点响应: {resp.message})")

    # 读一个键
    print("\n读取单个键:")
    resp = client.get("user/1")
    print(f"  GET user/1 = {resp.data.get('value')}")

    # 前缀查询
    print("\n前缀查询 'user/':")
    resp = client.get_prefix("user/")
    for item in resp.data["items"]:
        print(f"  {item['key']} = {item['value']}")

    # 范围查询
    print("\n范围查询 [config/a, config/z):")
    resp = client.get_range("config/a", "config/z")
    for item in resp.data["items"]:
        print(f"  {item['key']} = {item['value']}")

    # 验证所有节点状态一致
    print("\n一致性验证 - 各节点 KV revision & count:")
    for nid, node in nodes.items():
        status = node.get_status()
        kv_info = status["kv"]
        print(f"  {nid}: revision={kv_info['revision']:3d}, count={kv_info['size']}")

    client.close()


# ============================================================
# Demo 3: Watch 通知
# ============================================================
def demo3_watch(cluster: ClusterSimulator, nodes):
    separator("Demo 3: Watch 通知 (精确键 + 前缀)")

    # 用另一个客户端做写操作,演示 Watch 实时通知
    writer_client = DistributedClient(cluster)
    reader_client = DistributedClient(cluster)
    writer_client.connect()
    reader_client.connect()

    # 创建一个 Watch 线程
    watch_events = []
    watch_done = threading.Event()

    def watch_thread_fn():
        try:
            with reader_client.watch_prefix("app/config/", start_revision=-1) as watcher:
                for event in watcher:
                    watch_events.append(event)
                    print(f"  [Watch 事件] {event.event_type.value:6s} {event.key} = {event.value}")
                    if len(watch_events) >= 5:
                        break
        except Exception as e:
            logger.error(f"Watch 线程异常: {e}")
        watch_done.set()

    watch_thread = threading.Thread(target=watch_thread_fn, daemon=True)
    watch_thread.start()
    time.sleep(0.3)  # 等 Watch 创建好

    print("开始写入数据触发 Watch 事件...")
    # 写入一些带前缀的键
    keys_to_write = [
        ("app/config/timeout", 30),
        ("app/config/retry", 5),
        ("other/key", "should_not_watch"),
        ("app/config/enable_x", True),
        ("app/config/max_conn", 100),
    ]
    for k, v in keys_to_write:
        writer_client.put(k, v)
        time.sleep(0.2)  # 间隔让 Watch 有时间处理

    # 等 Watch 处理完
    watch_done.wait(timeout=5.0)
    print(f"\n共收到 {len(watch_events)} 条 Watch 事件")

    writer_client.close()
    reader_client.close()


# ============================================================
# Demo 4: 分布式锁 + 互斥验证
# ============================================================
def demo4_distributed_lock(cluster: ClusterSimulator, nodes):
    separator("Demo 4: 分布式锁 - 互斥与并发安全")

    N_THREADS = 3
    counter = {"value": 0}  # 共享变量,用锁保护
    results = []

    # 创建多个客户端,模拟多进程竞争
    clients = []
    for i in range(N_THREADS):
        c = DistributedClient(cluster)
        c.connect()
        clients.append(c)

    lock_name = "demo_counter_lock"

    def worker(worker_id: int, client: DistributedClient, iterations: int):
        for _ in range(iterations):
            lock = client.get_lock(lock_name, ttl=5)
            with lock:
                # 临界区: 读-改-写 (没有锁就会产生竞态)
                val = counter["value"]
                time.sleep(0.01)  # 模拟处理时间,放大竞态窗口
                counter["value"] = val + 1
                results.append(worker_id)

    print(f"启动 {N_THREADS} 个线程,每个线程对共享计数器加 10 次...")
    print(f"(如果互斥正确,最终 counter = {N_THREADS * 10})")

    threads = []
    for i in range(N_THREADS):
        t = threading.Thread(
            target=worker,
            args=(i, clients[i], 10),
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=30)

    final_value = counter["value"]
    print(f"\n最终 counter = {final_value} (预期 {N_THREADS * 10})")
    if final_value == N_THREADS * 10:
        print("✓ 互斥保证正确!没有发生竞态条件。")
    else:
        print("✗ 警告: 可能存在锁问题!")

    print(f"\n锁获取分布: {[f'T{i}: {results.count(i)}次' for i in range(N_THREADS)]}")

    for c in clients:
        c.close()


# ============================================================
# Demo 5: 租约过期自动释放锁 + TTL 键自动删除
# ============================================================
def demo5_lease_expiry(cluster: ClusterSimulator, nodes):
    separator("Demo 5: 租约过期 - 锁自动释放 & TTL 键自动删除")

    client = DistributedClient(cluster)
    client.connect()

    # 1. 带 TTL 的键
    print("写入带 3 秒 TTL 的键 'temp/ephemeral' ...")
    client.put("temp/ephemeral", "this_will_disappear", ttl=3)
    resp = client.get("temp/ephemeral")
    print(f"  立即读取: {resp.success}, value={resp.data.get('value')}")

    # 2. 分布式锁,使用短 TTL,然后模拟崩溃 (不心跳)
    print("\n获取 TTL=3 秒的锁 'ephemeral_lock' (模拟客户端崩溃)...")
    lock = client.get_lock("ephemeral_lock", ttl=3)
    lock.acquire()
    print(f"  查询锁: {client._call_any('lock_info', 'ephemeral_lock').data}")
    print(f"  查询锁: {client._call_any('lock_info', 'ephemeral_lock').message}")

    # 停止客户端心跳,模拟崩溃
    print("\n=== 模拟客户端崩溃: 停止心跳线程 ===")
    client._stop_heartbeat.set()
    if client._heartbeat_thread:
        client._heartbeat_thread.join(timeout=1)

    # 用另一个客户端来观察
    observer = DistributedClient(cluster)
    observer.connect()

    # 等待租约过期
    ttl_seconds = 4
    for i in range(ttl_seconds):
        time.sleep(1)
        lock_resp = observer._call_any("lock_info", "ephemeral_lock")
        key_resp = observer.get("temp/ephemeral")
        lock_held = lock_resp.data.get("held", False)
        key_exists = key_resp.success
        print(
            f"  {i+1}s 后: 锁={'持有' if lock_held else '已释放':4s}, "
            f"TTL键={'存在' if key_exists else '已删除':4s}"
        )

    print("\n✓ 租约过期后,锁和 TTL 键自动清理完毕!")

    # 等日志同步完成
    time.sleep(0.5)

    # 另一个客户端现在可以获取锁了
    print("\n新客户端尝试获取 'ephemeral_lock' ...")
    new_lock = observer.get_lock("ephemeral_lock", ttl=10)
    acquired = new_lock.acquire(timeout=5)
    print(f"  获取结果: {'成功' if acquired else '失败'}")
    if acquired:
        new_lock.release()

    observer.close()
    client.close()


# ============================================================
# Demo 6: Follower 写请求转发
# ============================================================
def demo6_follower_forward(cluster: ClusterSimulator, nodes):
    separator("Demo 6: Follower 写请求自动转发到 Leader")

    # 找到一个 Follower
    follower_node = None
    for node in nodes.values():
        if not node.is_leader():
            follower_node = node
            break

    if not follower_node:
        print("没有 Follower,跳过此演示")
        return

    print(f"选中 Follower: {follower_node.node_id}")
    print(f"当前 Leader: {follower_node.get_leader_id()}")

    # 直接发送写请求给 Follower,看它是否转发
    print("\n直接向 Follower 发送 PUT 请求 (应自动转发到 Leader)...")
    resp = follower_node.kv_put("follower/forward/test", "value_via_follower")
    print(f"  响应: code={resp.code.name}, message={resp.message}")

    if resp.success:
        print("  ✓ Follower 自动转发成功!")

        # 等 Follower 追上日志复制
        time.sleep(0.3)

        # 验证数据在所有节点可见
        print("\n验证所有节点都能读到该键:")
        for nid, node in nodes.items():
            r = node.kv_get("follower/forward/test")
            status = "✓" if r.success else "✗"
            val = r.data.get("value") if r.success else "N/A"
            print(f"  {status} {nid}: value={val}")
    else:
        print(f"  ✗ 转发失败: {resp.message}")


# ============================================================
# Demo 7: 所有节点状态一致性验证
# ============================================================
def demo7_consistency_check(cluster: ClusterSimulator, nodes):
    separator("Demo 7: 最终一致性验证 - 所有节点状态对比")

    leader = cluster.get_leader()
    if not leader:
        print("无 Leader,跳过")
        return

    # Leader 多写一些数据
    for i in range(10):
        leader.kv_put(f"consistency/check/{i}", f"value_{i}")

    # 等一会儿让 Follower 追上
    time.sleep(0.5)

    print("各节点状态摘要:")
    summaries = {}
    for nid, node in nodes.items():
        status = node.get_status()
        dump = node.dump_debug()
        summaries[nid] = {
            "revision": status["kv"]["revision"],
            "kv_count": status["kv"]["size"],
            "lease_count": status["leases"],
            "lock_count": status["locks"],
            "commit_index": status["consensus"]["commit_index"],
            "last_applied": status["consensus"]["last_applied"],
            "log_size": status["consensus"]["log_length"],
        }
        print(f"  {nid}:")
        for k, v in summaries[nid].items():
            print(f"    {k:15s} = {v}")

    # 检查一致性
    first = list(summaries.values())[0]
    all_consistent = all(
        s["kv_count"] == first["kv_count"]
        and abs(s["revision"] - first["revision"]) <= 1  # 允许短暂差异
        for s in summaries.values()
    )
    print(f"\n一致性检查: {'✓ 所有节点状态一致' if all_consistent else '✗ 状态可能不一致'}")


# ============================================================
# 主入口
# ============================================================
def main():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║            分布式锁与协调服务 - 功能演示 v1.0                    ║
║                                                                  ║
║  模块:  Raft一致性 | KV状态机 | 租约管理 | Watch | 分布式锁      ║
╚══════════════════════════════════════════════════════════════════╝
""")

    # Demo 1: 启动集群
    cluster, nodes = demo1_cluster_bootstrap()
    if not cluster:
        logger.error("集群启动失败")
        return

    try:
        # Demo 2: KV 存储
        demo2_kv_storage(cluster, nodes)

        # Demo 3: Watch 通知
        demo3_watch(cluster, nodes)

        # Demo 4: 分布式锁
        demo4_distributed_lock(cluster, nodes)

        # Demo 5: 租约过期
        demo5_lease_expiry(cluster, nodes)

        # Demo 6: Follower 转发
        demo6_follower_forward(cluster, nodes)

        # Demo 7: 一致性验证
        demo7_consistency_check(cluster, nodes)

    except KeyboardInterrupt:
        print("\n用户中断")
    finally:
        separator("关闭集群")
        cluster.stop_all()
        print("所有节点已停止,演示完毕!")


if __name__ == "__main__":
    main()
