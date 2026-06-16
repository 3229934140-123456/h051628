"""
客户端 SDK - 简化访问分布式协调服务的接口

封装了与服务节点的交互,提供了:
1. 自动 Leader 发现: 请求 Follower 时自动重定向到 Leader
2. 会话管理: 自动心跳续期,会话重建
3. 锁的上下文管理器: with lock: 自动获取/释放
4. Watch 迭代器: for event in watcher: 方便消费事件
5. 失败重试: 短暂失败自动重试
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
import threading
import time
import logging

from common import Response, ErrorCode, WatchEvent, WatchEventType, generate_id
from node import Node, ClusterSimulator

logger = logging.getLogger(__name__)


class DistributedLock:
    """
    分布式锁上下文管理器

    用法:
        lock = client.get_lock("my_resource", ttl=10)
        with lock:
            # 临界区代码
            do_something_protected()
        # 自动释放
    """

    def __init__(
        self,
        client: "DistributedClient",
        lock_name: str,
        ttl: int = 10,
    ):
        self._client = client
        self.lock_name = lock_name
        self.ttl = ttl
        self._acquired = False
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_heartbeat = threading.Event()

    def acquire(self, timeout: float = -1, blocking: bool = True) -> bool:
        """获取锁"""
        if self._acquired:
            return True

        resp = self._client._call_leader(
            "lock_acquire",
            self.lock_name,
            self._client.session_id,
            self.ttl,
            timeout,
        ) if blocking else self._client._call_leader(
            "lock_try_acquire",
            self.lock_name,
            self._client.session_id,
            self.ttl,
        )

        if resp.success:
            self._acquired = True
            self._start_heartbeat()
            logger.info(f"[客户端] 已获取锁: {self.lock_name}")
            return True
        else:
            logger.warning(f"[客户端] 获取锁失败: {self.lock_name}, {resp.message}")
            return False

    def release(self) -> bool:
        """释放锁"""
        if not self._acquired:
            return True

        self._stop_heartbeat.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2.0)

        resp = self._client._call_leader(
            "lock_release",
            self.lock_name,
            self._client.session_id,
        )

        self._acquired = False
        if resp.success:
            logger.info(f"[客户端] 已释放锁: {self.lock_name}")
            return True
        else:
            logger.warning(f"[客户端] 释放锁失败: {self.lock_name}, {resp.message}")
            return False

    def _start_heartbeat(self):
        """启动心跳线程,定期给锁续期"""
        self._stop_heartbeat.clear()
        interval = max(1, self.ttl // 3)

        def heartbeat_loop():
            while not self._stop_heartbeat.is_set():
                try:
                    resp = self._client._call_leader(
                        "lock_refresh",
                        self.lock_name,
                        self._client.session_id,
                    )
                    if not resp.success:
                        logger.warning(f"[客户端] 锁续期失败: {resp.message}")
                except Exception as e:
                    logger.warning(f"[客户端] 锁续期异常: {e}")
                self._stop_heartbeat.wait(interval)

        self._heartbeat_thread = threading.Thread(
            target=heartbeat_loop, daemon=True, name=f"lock-heartbeat-{self.lock_name}"
        )
        self._heartbeat_thread.start()

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError(f"无法获取锁: {self.lock_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False

    @property
    def acquired(self) -> bool:
        return self._acquired


class Watcher:
    """
    Watch 事件迭代器

    用法:
        with client.watch_prefix("my/key/") as watcher:
            for event in watcher:
                print(f"事件: {event.event_type} {event.key}")
    """

    def __init__(self, client: "DistributedClient", watch_id: int):
        self._client = client
        self.watch_id = watch_id
        self._cancelled = False

    def __iter__(self) -> Iterator[WatchEvent]:
        while not self._cancelled:
            resp = self._client._call_any(
                "watch_fetch",
                self.watch_id,
                max_count=100,
                timeout=1.0,
            )
            if not resp.success:
                logger.warning(f"[客户端] Watch 拉取失败: {resp.message}")
                time.sleep(0.5)
                continue

            for ed in resp.data.get("events", []):
                try:
                    yield WatchEvent(
                        event_type=WatchEventType(ed["event_type"]),
                        key=ed["key"],
                        value=ed.get("value"),
                        revision=ed.get("revision", 0),
                        lease_id=ed.get("lease_id", 0),
                    )
                except Exception as e:
                    logger.warning(f"[客户端] 解析 Watch 事件失败: {e}")

            if resp.data.get("has_more"):
                continue

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            self._client._call_any("watch_cancel", self.watch_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cancel()
        return False


class DistributedClient:
    """
    分布式协调服务客户端

    封装了所有服务接口,提供:
    - 自动 Leader 发现与请求路由
    - 自动会话心跳
    - 失败重试
    """

    def __init__(
        self,
        cluster: ClusterSimulator,
        session_timeout: int = 10,
    ):
        self._cluster = cluster
        self._session_timeout = session_timeout
        self.session_id = ""
        self._lease_id: int = 0
        self._leader_id: Optional[str] = None

        # 会话心跳线程
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_heartbeat = threading.Event()

        # 活跃的 watchers
        self._watchers: Dict[int, Watcher] = {}

    # ======== 会话管理 ========

    def connect(self) -> bool:
        """连接到集群,创建会话"""
        # 等待 Leader
        leader_id = self._cluster.wait_for_leader(timeout=10.0)
        if not leader_id:
            logger.error("[客户端] 无法连接到集群: 没有 Leader")
            return False

        # 创建会话
        leader = self._cluster.get_leader()
        resp = leader.session_create(self._session_timeout)
        if not resp.success:
            logger.error(f"[客户端] 创建会话失败: {resp.message}")
            return False

        self.session_id = resp.data["session_id"]
        self._lease_id = resp.data.get("lease_id", 0)
        self._leader_id = leader_id

        # 启动会话心跳
        self._start_session_heartbeat()

        logger.info(
            f"[客户端] 已连接到集群, session_id={self.session_id}, "
            f"leader={self._leader_id}"
        )
        return True

    def _start_session_heartbeat(self):
        """启动会话心跳线程"""
        self._stop_heartbeat.clear()
        interval = max(1, self._session_timeout // 3)

        def loop():
            while not self._stop_heartbeat.is_set():
                try:
                    resp = self._call_leader(
                        "session_heartbeat",
                        self.session_id,
                    )
                    if not resp.success:
                        logger.warning(f"[客户端] 会话心跳失败: {resp.message}")
                except Exception as e:
                    logger.warning(f"[客户端] 会话心跳异常: {e}")
                self._stop_heartbeat.wait(interval)

        self._heartbeat_thread = threading.Thread(
            target=loop, daemon=True, name="client-session-heartbeat"
        )
        self._heartbeat_thread.start()

    def close(self):
        """断开连接"""
        self._stop_heartbeat.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2.0)

        # 取消所有 watchers
        for w in list(self._watchers.values()):
            w.cancel()

        logger.info(f"[客户端] 已断开连接, session_id={self.session_id}")

    # ======== 内部: 请求路由 ========

    def _call_leader(self, method: str, *args, **kwargs) -> Response:
        """调用 Leader 节点的方法,自动处理重定向"""
        max_redirects = 5
        for _ in range(max_redirects):
            leader = self._cluster.get_leader()
            if not leader:
                time.sleep(0.2)
                continue

            fn = getattr(leader, method, None)
            if not fn:
                return Response(ErrorCode.KEY_NOT_FOUND, f"方法不存在: {method}")

            resp = fn(*args, **kwargs)

            if resp.code == ErrorCode.NOT_LEADER:
                # 重定向
                new_leader = resp.data.get("leader_id")
                if new_leader and new_leader in self._cluster.nodes:
                    self._leader_id = new_leader
                    continue
                time.sleep(0.2)
                continue

            return resp

        return Response(ErrorCode.NO_QUORUM, "多次重定向后仍无法找到 Leader")

    def _call_any(self, method: str, *args, **kwargs) -> Response:
        """调用任意节点 (读操作可以用这个)"""
        # 优先用 Leader
        leader = self._cluster.get_leader()
        if leader:
            fn = getattr(leader, method, None)
            if fn:
                return fn(*args, **kwargs)

        # 否则遍历所有节点
        for node in self._cluster.nodes.values():
            fn = getattr(node, method, None)
            if fn:
                resp = fn(*args, **kwargs)
                if resp.success:
                    return resp
        return Response(ErrorCode.NO_QUORUM, "没有可用节点")

    # ======== KV API ========

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> Response:
        """写入键值,可选设置 TTL (通过租约)"""
        lease_id = 0
        if ttl and ttl > 0:
            resp = self._call_leader("lease_grant", ttl, self.session_id)
            if resp.success:
                lease_id = resp.data["lease_id"]

        return self._call_leader("kv_put", key, value, lease_id)

    def delete(self, key: str) -> Response:
        return self._call_leader("kv_delete", key)

    def get(self, key: str) -> Response:
        return self._call_any("kv_get", key)

    def get_prefix(self, prefix: str) -> Response:
        return self._call_any("kv_get_prefix", prefix)

    def get_range(self, start: str, end: str) -> Response:
        return self._call_any("kv_get_range", start, end)

    def get_all(self) -> Response:
        return self._call_any("kv_get_all")

    # ======== 租约 API ========

    def grant_lease(self, ttl: int) -> Response:
        return self._call_leader("lease_grant", ttl, self.session_id)

    def keepalive_lease(self, lease_id: int) -> Response:
        return self._call_leader("lease_keepalive", lease_id)

    def revoke_lease(self, lease_id: int) -> Response:
        return self._call_leader("lease_revoke", lease_id)

    def get_lease_ttl(self, lease_id: int) -> Response:
        return self._call_any("lease_ttl", lease_id)

    # ======== 锁 API ========

    def get_lock(self, lock_name: str, ttl: int = 10) -> DistributedLock:
        """获取一个锁对象 (不会立即获取,使用 acquire 或 with 语句)"""
        return DistributedLock(self, lock_name, ttl)

    def try_lock(self, lock_name: str, ttl: int = 10) -> DistributedLock:
        """尝试非阻塞获取锁,返回锁对象 (检查 acquired 属性判断是否成功)"""
        lock = DistributedLock(self, lock_name, ttl)
        lock.acquire(blocking=False)
        return lock

    # ======== Watch API ========

    def watch(
        self,
        key: str,
        start_revision: int = 0,
    ) -> Watcher:
        """Watch 单个键"""
        resp = self._call_any(
            "watch_create",
            exact_key=key,
            start_revision=start_revision,
        )
        if not resp.success:
            raise RuntimeError(f"创建 Watch 失败: {resp.message}")
        watcher = Watcher(self, resp.data["watch_id"])
        self._watchers[watcher.watch_id] = watcher
        return watcher

    def watch_prefix(
        self,
        prefix: str,
        start_revision: int = 0,
    ) -> Watcher:
        """Watch 前缀"""
        resp = self._call_any(
            "watch_create",
            prefix=prefix,
            start_revision=start_revision,
        )
        if not resp.success:
            raise RuntimeError(f"创建 Watch 失败: {resp.message}")
        watcher = Watcher(self, resp.data["watch_id"])
        self._watchers[watcher.watch_id] = watcher
        return watcher

    # ======== 状态查询 ========

    def cluster_status(self) -> Dict[str, Any]:
        """查询集群状态"""
        result = {}
        for nid, node in self._cluster.nodes.items():
            result[nid] = node.get_status()
        return result
