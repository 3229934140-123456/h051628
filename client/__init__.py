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

from common import (
    Response, ErrorCode, WatchEvent, WatchEventType, generate_id,
    TxnRequest, TxnCompare, TxnCompareOp, TxnOp, TxnOpType,
)
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
    Watch 事件迭代器 - 支持断线可恢复消费

    关键特性:
    1. 自动记录 last_revision: 消费到的最新事件 revision
    2. 断线重连: 如果 Watch 失效 (watch_id 不存在), 自动用 last_revision 重新订阅
    3. 超龄提示: 如果 last_revision 太旧, 会抛出 WatchCompactedException
    4. last_revision 可通过 checkpoint() 保存, 用于进程重启后恢复

    用法:
        # 初始订阅 (不重放历史)
        watcher = client.watch_prefix("app/config/")
        for event in watcher:
            print(event)
            # 定期 checkpoint, 进程重启时可恢复
            # watcher.checkpoint()

        # 断线恢复: 用上次保存的 revision 重新创建
        watcher = client.watch_prefix("app/config/", start_revision=saved_rev)
    """

    def __init__(
        self,
        client: "DistributedClient",
        watch_id: int,
        start_revision: int = 0,
        watch_params: Dict[str, Any] = None,
        historical_events: Optional[List[WatchEvent]] = None,
    ):
        self._client = client
        self.watch_id = watch_id
        self._start_revision = start_revision
        self._last_revision = start_revision  # 消费到的最新 revision
        self._cancelled = False
        self._params = watch_params or {}
        self._compact_revision = 0
        self._head_revision = 0
        self._error_count = 0
        # create_watch 返回的立即历史事件, 在迭代器里优先消费
        self._pending_historical: List[WatchEvent] = list(historical_events or [])

    @property
    def last_revision(self) -> int:
        """当前消费到的最新 revision,可用于断线恢复"""
        return self._last_revision

    @property
    def compact_revision(self) -> int:
        """服务端历史窗口的最早 revision"""
        return self._compact_revision

    @property
    def head_revision(self) -> int:
        """服务端最新 revision"""
        return self._head_revision

    def checkpoint(self) -> int:
        """手动 checkpoint, 返回当前 last_revision"""
        return self._last_revision

    def _reconnect(self) -> bool:
        """
        Watch 失效时重新订阅 (用 last_revision 恢复)

        Returns:
            True: 重连成功
            False: 重连失败 (如历史超龄)

        Raises:
            WatchCompactedException: last_revision 超出历史窗口
        """
        logger.info(
            f"[客户端 Watch] 尝试恢复订阅, start_revision={self._last_revision}"
        )
        resp = self._client._call_any(
            "watch_create",
            start_revision=self._last_revision,
            **self._params,
        )

        if not resp.success:
            if resp.code == ErrorCode.TIMEOUT:
                # 历史超龄
                compact_rev = resp.data.get("compact_revision", 0)
                lost = resp.data.get("lost_events", 0)
                raise WatchCompactedException(
                    self._last_revision,
                    compact_rev,
                    lost,
                    f"Watch 历史已丢失: 请求 rev={self._last_revision}, "
                    f"但服务端仅保留 rev≥{compact_rev}, 丢失了约 {lost} 条事件",
                    suggest_resync_revision=resp.data.get("suggest_resync_revision", compact_rev),
                    window_size=resp.data.get("window_size", 0),
                    head_revision=resp.data.get("head_revision", 0),
                )
            logger.warning(f"[客户端 Watch] 重连失败: {resp.message}")
            return False

        # 更新状态
        self.watch_id = resp.data["watch_id"]
        self._compact_revision = resp.data.get("compact_revision", 0)
        self._head_revision = resp.data.get("head_revision", 0)
        self._error_count = 0

        # 处理重放的历史事件
        historical = self._client._parse_events_from_response(resp)
        if historical:
            self._pending_historical.extend(historical)
            logger.info(
                f"[客户端 Watch] 恢复成功,重放 {len(historical)} 条历史事件 "
                f"(rev={self._last_revision})"
            )

        return True

    def __iter__(self) -> Iterator[WatchEvent]:
        # 先消费 create_watch 立即返回的历史事件
        while self._pending_historical and not self._cancelled:
            ev = self._pending_historical.pop(0)
            if ev.revision > self._last_revision:
                self._last_revision = ev.revision
            yield ev

        while not self._cancelled:
            try:
                resp = self._client._call_any(
                    "watch_fetch",
                    self.watch_id,
                    max_count=100,
                    timeout=1.0,
                )

                # Watch 不存在或失效, 尝试重连
                if not resp.success and resp.code == ErrorCode.KEY_NOT_FOUND:
                    self._error_count += 1
                    if self._error_count >= 3:
                        logger.error("[客户端 Watch] 多次失败,尝试重连...")
                        self._reconnect()
                    else:
                        time.sleep(0.2)
                    continue

                if not resp.success:
                    logger.warning(f"[客户端 Watch] 拉取失败: {resp.message}")
                    time.sleep(0.5)
                    continue

                self._error_count = 0
                self._compact_revision = resp.data.get("compact_revision", 0)
                self._head_revision = resp.data.get("head_revision", 0)

                events = resp.data.get("events", [])
                for ed in events:
                    try:
                        ev = WatchEvent(
                            event_type=WatchEventType(ed["event_type"]),
                            key=ed["key"],
                            value=ed.get("value"),
                            revision=ed.get("revision", 0),
                            lease_id=ed.get("lease_id", 0),
                        )
                        # 更新消费进度
                        if ev.revision > self._last_revision:
                            self._last_revision = ev.revision
                        yield ev
                    except Exception as e:
                        logger.warning(f"[客户端 Watch] 解析事件失败: {e}")

                # has_more 立即继续拉
                if resp.data.get("has_more"):
                    continue

            except WatchCompactedException:
                # 历史超龄, 不捕获, 让调用者处理
                raise
            except Exception as e:
                self._error_count += 1
                logger.warning(f"[客户端 Watch] 迭代异常: {e}")
                if self._error_count >= 5:
                    logger.error("[客户端 Watch] 异常过多,尝试重连...")
                    try:
                        self._reconnect()
                    except WatchCompactedException:
                        raise
                    time.sleep(0.5)
                else:
                    time.sleep(0.2)

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            try:
                self._client._call_any("watch_cancel", self.watch_id)
            except:
                pass

    def collect(
        self,
        timeout: float = 2.0,
        max_events: int = 100,
    ) -> List[WatchEvent]:
        """
        在 timeout 秒内收集最多 max_events 个事件后返回。

        特性:
        - 先消费立即可用的历史事件 (create_watch/_reconnect 带过来的)
        - 然后最多等待 timeout 秒, 有新事件就立即返回
        - 如果没有任何事件 (历史+实时都为空), 会一直等到 timeout
        """
        events: List[WatchEvent] = []
        deadline = time.time() + timeout

        # 1) 先吃掉 pending_historical
        while self._pending_historical and not self._cancelled and len(events) < max_events:
            ev = self._pending_historical.pop(0)
            if ev.revision > self._last_revision:
                self._last_revision = ev.revision
            events.append(ev)

        # 2) 进入长轮询收集实时事件
        remaining = deadline - time.time()
        while not self._cancelled and len(events) < max_events and remaining > 0:
            try:
                resp = self._client._call_any(
                    "watch_fetch",
                    self.watch_id,
                    max_count=max_events - len(events),
                    timeout=min(remaining, 1.0),
                )

                if not resp.success and resp.code == ErrorCode.KEY_NOT_FOUND:
                    self._error_count += 1
                    if self._error_count >= 3:
                        logger.error("[客户端 Watch] collect 多次失败,尝试重连...")
                        self._reconnect()
                    else:
                        time.sleep(0.1)
                    remaining = deadline - time.time()
                    continue

                if not resp.success:
                    logger.warning(f"[客户端 Watch] collect 拉取失败: {resp.message}")
                    time.sleep(0.2)
                    remaining = deadline - time.time()
                    continue

                self._error_count = 0
                self._compact_revision = resp.data.get("compact_revision", 0)
                self._head_revision = resp.data.get("head_revision", 0)

                ev_list = resp.data.get("events", [])
                for ed in ev_list:
                    try:
                        ev = WatchEvent(
                            event_type=WatchEventType(ed["event_type"]),
                            key=ed["key"],
                            value=ed.get("value"),
                            revision=ed.get("revision", 0),
                            lease_id=ed.get("lease_id", 0),
                        )
                        if ev.revision > self._last_revision:
                            self._last_revision = ev.revision
                        events.append(ev)
                        if len(events) >= max_events:
                            break
                    except Exception as e:
                        logger.warning(f"[客户端 Watch] collect 解析事件失败: {e}")

                # 有事件就返回, 没事件继续等
                if len(events) > 0 and not resp.data.get("has_more"):
                    break
                if len(events) >= max_events:
                    break
            except WatchCompactedException:
                raise
            except Exception as e:
                self._error_count += 1
                logger.warning(f"[客户端 Watch] collect 异常: {e}")
                time.sleep(0.1)
            remaining = deadline - time.time()

        return events

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cancel()
        # WatchCompactedException 不吞, 让调用者知道
        if isinstance(exc_val, WatchCompactedException):
            return False
        return False


class WatchCompactedException(Exception):
    """
    Watch 历史已被压缩 (超龄) 异常

    表示客户端请求的 start_revision 早于服务端保留的 compact_revision,
    中间有事件已经丢失, 无法完整恢复。

    属性:
    - requested_revision: 客户端请求的起始 revision
    - compact_revision:   服务端当前可用的最早 revision
    - lost_events:        请求 revision 和 compact_revision 之间丢失的事件数
    - suggest_resync_revision: 建议从此 revision 开始做全量同步
    - window_size:        服务端当前历史窗口大小 (head - compact)
    - head_revision:      服务端最新 revision

    处理方式:
    1. 记录告警, 业务层可能需要全量同步
    2. 用 suggest_resync_revision 重新创建 Watch (从当前可用历史开始)
    """

    def __init__(self, requested_revision: int, compact_revision: int,
                 lost_events: int, message: str,
                 suggest_resync_revision: int = 0,
                 window_size: int = 0,
                 head_revision: int = 0):
        super().__init__(message)
        self.requested_revision = requested_revision
        self.compact_revision = compact_revision
        self.lost_events = lost_events
        self.suggest_resync_revision = suggest_resync_revision or compact_revision
        self.window_size = window_size
        self.head_revision = head_revision


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

    # ======== 事务 (CAS) API ========

    def txn(self, txn_request: TxnRequest) -> Response:
        """执行事务: IF comparisons THEN success_ops ELSE failure_ops"""
        return self._call_leader("txn", txn_request)

    def compare_and_put(
        self,
        key: str,
        expected_version: int,
        new_value: Any,
        lease_id: int = 0,
    ) -> Response:
        """
        Compare-And-Put (CAS): 当 key.version == expected_version 时才写入 new_value

        返回: resp.succeeded = True 表示写入成功
        """
        return self._call_leader("compare_and_put", key, expected_version, new_value, lease_id)

    def compare_and_delete(self, key: str, expected_version: int) -> Response:
        """
        Compare-And-Delete: 当 key.version == expected_version 时才删除

        返回: resp.succeeded = True 表示删除成功
        """
        return self._call_leader("compare_and_delete", key, expected_version)

    def compare_and_put_if_not_exists(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> Response:
        """
        Compare-And-Put-If-Not-Exists: 键不存在时才写入

        典型用法: 配置抢占、选主注册
        返回: resp.succeeded = True 表示写入成功
        """
        lease_id = 0
        if ttl and ttl > 0:
            resp = self._call_leader("lease_grant", ttl, self.session_id)
            if resp.success:
                lease_id = resp.data["lease_id"]
        return self._call_leader("compare_and_put_if_not_exists", key, value, lease_id)

    def batch_txn(
        self,
        comparisons: List[TxnCompare],
        success_ops: List[TxnOp],
        failure_ops: Optional[List[TxnOp]] = None,
    ) -> Response:
        """
        批量事务: 一次 compare 后执行多条 put/delete

        返回:
            succeeded: 条件是否满足
            op_results: 每个操作结果 [{"op":"put","key":"k","success":True}, ...]
            revision: 当前 revision
        """
        return self._call_leader("batch_txn", comparisons, success_ops, failure_ops or [])

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
        """Watch 单个键, 支持断线恢复"""
        params = {"exact_key": key}
        resp = self._call_any(
            "watch_create",
            exact_key=key,
            start_revision=start_revision,
        )
        if not resp.success:
            if resp.code == ErrorCode.TIMEOUT:
                raise WatchCompactedException(
                    start_revision,
                    resp.data.get("compact_revision", 0),
                    resp.data.get("lost_events", 0),
                    resp.message,
                    suggest_resync_revision=resp.data.get("suggest_resync_revision", 0),
                    window_size=resp.data.get("window_size", 0),
                    head_revision=resp.data.get("head_revision", 0),
                )
            raise RuntimeError(f"创建 Watch 失败: {resp.message}")

        # 解析 create_watch 返回的历史事件
        historical = self._parse_events_from_response(resp)

        watcher = Watcher(
            self, resp.data["watch_id"],
            start_revision=start_revision,
            watch_params=params,
            historical_events=historical,
        )
        watcher._compact_revision = resp.data.get("compact_revision", 0)
        watcher._head_revision = resp.data.get("head_revision", 0)
        self._watchers[watcher.watch_id] = watcher
        return watcher

    def watch_prefix(
        self,
        prefix: str,
        start_revision: int = 0,
    ) -> Watcher:
        """Watch 前缀, 支持断线恢复"""
        params = {"prefix": prefix}
        resp = self._call_any(
            "watch_create",
            prefix=prefix,
            start_revision=start_revision,
        )
        if not resp.success:
            if resp.code == ErrorCode.TIMEOUT:
                raise WatchCompactedException(
                    start_revision,
                    resp.data.get("compact_revision", 0),
                    resp.data.get("lost_events", 0),
                    resp.message,
                    suggest_resync_revision=resp.data.get("suggest_resync_revision", 0),
                    window_size=resp.data.get("window_size", 0),
                    head_revision=resp.data.get("head_revision", 0),
                )
            raise RuntimeError(f"创建 Watch 失败: {resp.message}")

        # 解析 create_watch 返回的历史事件
        historical = self._parse_events_from_response(resp)

        watcher = Watcher(
            self, resp.data["watch_id"],
            start_revision=start_revision,
            watch_params=params,
            historical_events=historical,
        )
        watcher._compact_revision = resp.data.get("compact_revision", 0)
        watcher._head_revision = resp.data.get("head_revision", 0)
        self._watchers[watcher.watch_id] = watcher
        return watcher

    def _parse_events_from_response(self, resp) -> List[WatchEvent]:
        """把 response data['events'] 解析成 WatchEvent 列表"""
        events: List[WatchEvent] = []
        for ed in resp.data.get("events", []) or []:
            try:
                events.append(WatchEvent(
                    event_type=WatchEventType(ed["event_type"]),
                    key=ed["key"],
                    value=ed.get("value"),
                    revision=ed.get("revision", 0),
                    lease_id=ed.get("lease_id", 0),
                ))
            except Exception as e:
                logger.warning(f"[客户端 Watch] 解析历史事件失败: {e}")
        return events

    def watch_status(self) -> Dict[str, Any]:
        """获取 Watch 历史窗口状态"""
        resp = self._call_any("watch_status")
        return resp.data

    # ======== 状态查询 ========

    def cluster_status(self) -> Dict[str, Any]:
        """查询集群状态"""
        result = {}
        for nid, node in self._cluster.nodes.items():
            result[nid] = node.get_status()
        return result
