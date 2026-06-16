"""
节点核心模块 - Node

将一致性、KV状态机、租约、Watch、锁服务整合为一个完整节点。

职责:
1. 初始化并协调所有子模块
2. 处理客户端请求 (KV/租约/锁/Watch)
3. 处理 Leader/Follower 转发: Follower 自动将写请求转发给 Leader
4. 提供节点间 RPC 通信接口 (模拟网络)
5. 提供对外 API 接口

写请求转发流程:
- 客户端请求任意节点
- 如果是 Follower: 返回 NOT_LEADER + leader_id, 客户端自动重定向
- 如果是 Leader: 处理请求 (提交 Raft 日志)
- 也可以由节点内部直接转发 (forward_to_leader)
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple
import threading
import time
import logging

from common import (
    LogEntry, LogEntryType, Response, ErrorCode, WatchEventType,
    generate_id
)
from consensus import ConsensusModule, RaftConfig
from kvstore import KVStateMachine
from lease import LeaseManager
from watch import WatchManager
from lock import LockService

logger = logging.getLogger(__name__)


class Node:
    """
    分布式协调节点 - 整合所有子模块

    典型使用:
        # 创建 3 节点集群
        nodes = {}
        for i in range(3):
            node_id = f"node{i}"
            peers = [(f"node{j}", f"addr{j}") for j in range(3) if j != i]
            nodes[node_id] = Node(node_id, peers)
            nodes[node_id].start()

        # 关联节点间通信 (模拟网络)
        cluster = ClusterSimulator(list(nodes.values()))

        # 使用
        leader_id = cluster.wait_for_leader()
        leader = nodes[leader_id]
        resp = leader.kv_put("key1", "value1")
    """

    def __init__(
        self,
        node_id: str,
        peers: List[Tuple[str, str]],
        raft_config: RaftConfig = None,
    ):
        self.node_id = node_id
        self.raft_config = raft_config or RaftConfig()
        self._lock = threading.RLock()

        # 集群中其他节点的引用 (模拟网络, ClusterSimulator 会填充)
        self._peer_nodes: Dict[str, "Node"] = {}

        # ===== 初始化子模块 =====
        self.kv = KVStateMachine()
        self.lease_mgr = LeaseManager(kv_state_machine=self.kv)
        self.watch_mgr = WatchManager()
        self.lock_svc = LockService(
            kv_state_machine=self.kv,
            lease_manager=self.lease_mgr,
            watch_manager=self.watch_mgr,
        )
        self.consensus = ConsensusModule(
            node_id=node_id,
            peers=peers,
            config=self.raft_config,
        )

        # ===== 注册回调 =====
        # 1. Raft 提交日志 → 分发给各子模块应用
        self.consensus.set_apply_callback(self._apply_committed_entries)

        # 2. Leader 变更
        self.consensus.set_leader_change_callback(self._on_leader_change)

        # 3. KV 变更 → Watch
        self.kv.add_change_callback(self._on_kv_change)

        # 4. 租约变更 → 回调 (可扩展)
        self.lease_mgr.add_lease_callback(self._on_lease_change)

        # 5. 锁变更 → 回调 (可扩展)
        self.lock_svc.add_lock_callback(self._on_lock_change)

        # ===== 注入子模块依赖 =====
        self.lease_mgr.set_submit_write_fn(self._submit_write)
        self.lease_mgr.set_is_leader_fn(self.consensus.is_leader)

        self.lock_svc.set_submit_write_fn(self._submit_write)
        self.lock_svc.set_is_leader_fn(self.consensus.is_leader)

        # ===== 注册 RPC 处理函数 =====
        self.consensus.register_rpc_handler(
            "request_vote", self._rpc_request_vote
        )
        self.consensus.register_rpc_handler(
            "append_entries", self._rpc_append_entries
        )

        # 统计
        self._stats = {
            "kv_put_count": 0,
            "kv_delete_count": 0,
            "lock_acquire_count": 0,
            "lock_release_count": 0,
            "forward_count": 0,
        }

    # ======== 启动/停止 ========

    def start(self):
        """启动节点"""
        self.lease_mgr.start()
        self.consensus.start()
        logger.info(f"节点 {self.node_id} 已启动")

    def stop(self):
        """停止节点"""
        self.consensus.stop()
        self.lease_mgr.stop()
        logger.info(f"节点 {self.node_id} 已停止")

    # ======== 内部: 应用已提交日志 ========

    def _apply_committed_entries(self, entries: List[LogEntry]):
        """
        Raft 已提交的日志,按顺序应用到各个子模块

        注意: 应用顺序很重要! 先租约, 再KV, 最后锁
        (因为锁依赖 KV 和租约)
        """
        # 1. 租约
        self.lease_mgr.apply_entries(entries)

        # 2. KV 状态机 (同时会触发 Watch 回调)
        self.kv.apply_entries(entries)

        # 3. 锁服务
        self.lock_svc.apply_entries(entries)

    # ======== 内部: 变更回调 ========

    def _on_kv_change(self, key: str, event, revision: int):
        """KV 变更 → 推送给 WatchManager"""
        self.watch_mgr.on_state_change(key, event, revision)

    def _on_leader_change(self, node_id: str, new_role):
        logger.warning(
            f"[节点 {self.node_id}] Leader 变更: {node_id} 变为 {new_role.value}"
        )

    def _on_lease_change(self, lease, action: str):
        # 可以扩展: 推送租约相关事件
        pass

    def _on_lock_change(self, lock_info, action: str):
        # 可以扩展: 推送锁变更事件
        pass

    # ======== 内部: 提交写操作 (供子模块调用) ========

    def _submit_write(self, entry_type: LogEntryType, **kwargs) -> Response:
        """
        提交写操作到 Raft 一致性模块
        如果不是 Leader, 尝试转发到 Leader
        """
        if self.consensus.is_leader():
            return self.consensus.submit_write(entry_type, **kwargs)
        else:
            # Follower: 转发到 Leader
            return self._forward_to_leader(entry_type, **kwargs)

    def _forward_to_leader(self, entry_type: LogEntryType, **kwargs) -> Response:
        """Follower 转发写请求到 Leader"""
        leader_id = self.consensus.get_leader_id()
        if not leader_id:
            return Response(
                ErrorCode.NOT_LEADER,
                "当前无可用 Leader,集群可能正在选举",
                leader_id=None,
            )

        if leader_id == self.node_id:
            # 我就是 Leader (可能刚变)
            return self.consensus.submit_write(entry_type, **kwargs)

        leader_node = self._peer_nodes.get(leader_id)
        if not leader_node:
            return Response(
                ErrorCode.NOT_LEADER,
                f"Leader {leader_id} 不可达",
                leader_id=leader_id,
            )

        self._stats["forward_count"] += 1
        logger.debug(
            f"[节点 {self.node_id}] Follower 转发写请求到 Leader {leader_id}"
        )

        # 直接调用 Leader 节点的 submit_write (模拟 RPC)
        # 注意: 实际生产中这里会是网络调用
        return leader_node.consensus.submit_write(entry_type, **kwargs)

    # ======== RPC 模拟 (节点间通信) ========

    def set_peer_nodes(self, peer_nodes: Dict[str, "Node"]):
        """设置其他节点的引用 (模拟网络连接)"""
        with self._lock:
            self._peer_nodes = dict(peer_nodes)
            if self.node_id in self._peer_nodes:
                del self._peer_nodes[self.node_id]

    def _rpc_request_vote(self, from_id: str, to_id: str, **kwargs):
        """RequestVote RPC"""
        target = self._peer_nodes.get(to_id)
        if not target and to_id == self.node_id:
            target = self
        if not target:
            return {"term": kwargs.get("term", 0), "vote_granted": False}
        return target.consensus.handle_request_vote(**kwargs)

    def _rpc_append_entries(self, from_id: str, to_id: str, **kwargs):
        """AppendEntries RPC"""
        target = self._peer_nodes.get(to_id)
        if not target and to_id == self.node_id:
            target = self
        if not target:
            return {"term": kwargs.get("term", 0), "success": False}
        return target.consensus.handle_append_entries(**kwargs)

    # ================================================================
    # 对外 API: KV 操作
    # ================================================================

    def kv_put(self, key: str, value: Any, lease_id: int = 0) -> Response:
        """写入键值对"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].kv_put(key, value, lease_id)
            return Response(
                ErrorCode.NOT_LEADER,
                f"当前节点不是 Leader",
                leader_id=leader_id,
            )

        resp = self._submit_write(
            LogEntryType.KV_PUT,
            key=key, value=value, lease_id=lease_id,
        )
        if resp.success:
            self._stats["kv_put_count"] += 1
            resp.data["revision"] = self.kv.revision
        return resp

    def kv_delete(self, key: str) -> Response:
        """删除键"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].kv_delete(key)
            return Response(
                ErrorCode.NOT_LEADER,
                f"当前节点不是 Leader",
                leader_id=leader_id,
            )

        resp = self._submit_write(
            LogEntryType.KV_DELETE, key=key,
        )
        if resp.success:
            self._stats["kv_delete_count"] += 1
        return resp

    def kv_get(self, key: str) -> Response:
        """读取键 (读操作可以直接读本地状态机)"""
        return self.kv.get(key)

    def kv_get_prefix(self, prefix: str) -> Response:
        """前缀查询"""
        return self.kv.get_prefix(prefix)

    def kv_get_range(self, start: str, end: str) -> Response:
        """范围查询"""
        return self.kv.get_range(start, end)

    def kv_get_all(self) -> Response:
        """获取所有键"""
        return self.kv.get_all()

    # ================================================================
    # 对外 API: 租约操作
    # ================================================================

    def lease_grant(self, ttl: int, session_id: str = "") -> Response:
        """创建租约"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].lease_grant(ttl, session_id)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        return self.lease_mgr.grant_lease(ttl, session_id)

    def lease_keepalive(self, lease_id: int) -> Response:
        """租约续期 (仅 Leader)"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].lease_keepalive(lease_id)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        return self.lease_mgr.keepalive_lease(lease_id)

    def lease_revoke(self, lease_id: int) -> Response:
        """撤销租约"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].lease_revoke(lease_id)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        return self.lease_mgr.revoke_lease(lease_id)

    def lease_ttl(self, lease_id: int) -> Response:
        """查询租约 TTL"""
        return self.lease_mgr.get_lease(lease_id)

    def lease_list(self) -> Response:
        """列出所有租约"""
        return self.lease_mgr.list_leases()

    # ================================================================
    # 对外 API: 会话操作
    # ================================================================

    def session_create(self, timeout: int = 10) -> Response:
        """创建会话"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].session_create(timeout)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        return self.lease_mgr.create_session(timeout)

    def session_heartbeat(self, session_id: str) -> Response:
        """会话心跳"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].session_heartbeat(session_id)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        return self.lease_mgr.session_heartbeat(session_id)

    # ================================================================
    # 对外 API: Watch 操作
    # ================================================================

    def watch_create(
        self,
        exact_key: str = None,
        prefix: str = None,
        range_start: str = None,
        range_end: str = None,
        pattern: str = None,
        event_types: List[str] = None,
        start_revision: int = 0,
    ) -> Response:
        """创建 Watch"""
        etypes = None
        if event_types:
            etypes = [WatchEventType(et) for et in event_types]

        return self.watch_mgr.create_watch(
            exact_key=exact_key,
            prefix=prefix,
            range_start=range_start,
            range_end=range_end,
            pattern=pattern,
            event_types=etypes,
            start_revision=start_revision,
        )

    def watch_cancel(self, watch_id: int) -> Response:
        """取消 Watch"""
        return self.watch_mgr.cancel_watch(watch_id)

    def watch_fetch(self, watch_id: int, max_count: int = 100, timeout: float = 0) -> Response:
        """拉取 Watch 事件"""
        return self.watch_mgr.fetch_events(watch_id, max_count, timeout)

    def watch_list(self) -> Response:
        """列出所有 Watch"""
        return self.watch_mgr.list_watches()

    # ================================================================
    # 对外 API: 锁操作
    # ================================================================

    def lock_try_acquire(self, lock_name: str, session_id: str, ttl: int = 10) -> Response:
        """非阻塞获取锁"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].lock_try_acquire(lock_name, session_id, ttl)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        resp = self.lock_svc.try_acquire(lock_name, session_id, ttl)
        if resp.success:
            self._stats["lock_acquire_count"] += 1
        return resp

    def lock_acquire(
        self, lock_name: str, session_id: str, ttl: int = 10, timeout: float = -1
    ) -> Response:
        """阻塞获取锁"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].lock_acquire(lock_name, session_id, ttl, timeout)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        resp = self.lock_svc.acquire(lock_name, session_id, ttl, timeout)
        if resp.success:
            self._stats["lock_acquire_count"] += 1
        return resp

    def lock_release(self, lock_name: str, session_id: str) -> Response:
        """释放锁"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].lock_release(lock_name, session_id)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        resp = self.lock_svc.release(lock_name, session_id)
        if resp.success:
            self._stats["lock_release_count"] += 1
        return resp

    def lock_refresh(self, lock_name: str, session_id: str) -> Response:
        """给锁续期"""
        if not self.consensus.is_leader():
            leader_id = self.consensus.get_leader_id()
            if leader_id and leader_id != self.node_id and leader_id in self._peer_nodes:
                return self._peer_nodes[leader_id].lock_refresh(lock_name, session_id)
            return Response(ErrorCode.NOT_LEADER, "当前节点不是 Leader", leader_id=leader_id)
        return self.lock_svc.refresh(lock_name, session_id)

    def lock_info(self, lock_name: str) -> Response:
        """查询锁信息"""
        return self.lock_svc.get_lock_info(lock_name)

    def lock_list(self) -> Response:
        """列出所有锁"""
        return self.lock_svc.list_locks()

    # ================================================================
    # 辅助 API
    # ================================================================

    def is_leader(self) -> bool:
        return self.consensus.is_leader()

    def get_leader_id(self) -> Optional[str]:
        return self.consensus.get_leader_id()

    def get_status(self) -> Dict[str, Any]:
        """获取节点状态"""
        return {
            "node_id": self.node_id,
            "consensus": self.consensus.get_status(),
            "kv": {
                "size": self.kv.size,
                "revision": self.kv.revision,
            },
            "leases": self.lease_mgr.list_leases().data.get("count", 0),
            "watches": self.watch_mgr.list_watches().data.get("count", 0),
            "locks": self.lock_svc.list_locks().data.get("count", 0),
            "stats": self._stats.copy(),
        }

    def dump_debug(self) -> Dict[str, Any]:
        """调试信息"""
        return {
            "status": self.get_status(),
            "kv_dump": self.kv.dump(),
            "lease_dump": self.lease_mgr.dump(),
            "watch_dump": self.watch_mgr.dump(),
            "lock_dump": self.lock_svc.dump(),
        }


class ClusterSimulator:
    """
    集群模拟器 - 模拟节点间的网络通信

    实际生产中会用 gRPC/HTTP 等网络通信,这里为了演示直接在内存中调用。
    """

    def __init__(self, nodes: List[Node]):
        self.nodes: Dict[str, Node] = {n.node_id: n for n in nodes}
        self._connect_nodes()

    def _connect_nodes(self):
        """让所有节点互相知道对方"""
        for node in self.nodes.values():
            node.set_peer_nodes(self.nodes.copy())

    def wait_for_leader(self, timeout: float = 10.0) -> Optional[str]:
        """等待集群选出 Leader"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            leaders = set()
            for node in self.nodes.values():
                lid = node.get_leader_id()
                if lid:
                    leaders.add(lid)
            if len(leaders) == 1:
                leader_id = list(leaders)[0]
                logger.info(f"集群已选出 Leader: {leader_id}")
                return leader_id
            time.sleep(0.1)
        logger.warning("等待 Leader 超时")
        return None

    def get_leader(self) -> Optional[Node]:
        """获取 Leader 节点"""
        lid = None
        for node in self.nodes.values():
            if node.is_leader():
                return node
            if not lid:
                lid = node.get_leader_id()
        if lid and lid in self.nodes:
            return self.nodes[lid]
        return None

    def get_node(self, node_id: str) -> Optional[Node]:
        return self.nodes.get(node_id)

    def start_all(self):
        """启动所有节点"""
        for node in self.nodes.values():
            node.start()

    def stop_all(self):
        """停止所有节点"""
        for node in self.nodes.values():
            node.stop()
