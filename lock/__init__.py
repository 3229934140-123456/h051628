"""
分布式锁服务模块

核心设计:
锁 = 带租约的键 (Leased Key)

获取锁流程:
1. 客户端 A 请求 acquire("my_lock", session_id=A, ttl=10)
2. Leader 创建 Raft 日志: LOCK_ACQUIRE, key="/locks/my_lock", lease_id=X, session=A
3. 日志复制到多数节点后提交
4. 状态机应用日志: 检查 "/locks/my_lock" 是否已存在
   - 不存在: 创建该键,绑定到租约 X → 获取成功
   - 已存在: 记录失败原因 → 获取失败
5. 返回结果给客户端

释放锁:
1. 客户端 A 请求 release("my_lock")
2. Leader 创建 Raft 日志: LOCK_RELEASE, key="/locks/my_lock"
3. 提交后状态机删除该键 → 锁释放

崩溃自动释放:
- 锁键绑定的租约 X 由客户端会话定期心跳续期
- 如果 A 崩溃或网络断开 → 租约 X 不再续期 → 过期
- Leader 检测到租约过期 → 提交 LEASE_REVOKE 日志
- 状态机应用 → 删除 "/locks/my_lock" → 锁自动释放

如何避免脑裂 (两把锁)?
1. 锁获取是 Raft 写操作 → 需要多数节点确认
2. 网络分区后,旧 Leader 所在分区不足多数 → 无法提交新日志
3. 因此旧分区内任何 acquire 请求都会超时失败
4. 新分区选出新 Leader 后,才能继续处理锁请求
5. 旧持有者的锁键有租约,即使旧 Leader 不认新 Leader,
   锁键最终会因租约过期而删除 → 不会死锁
6. 结论: 任何时刻最多只有一个客户端能"成功获取"锁
   (因为成功获取的定义是: Raft 日志已被多数提交)
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import deque
import threading
import time
import logging

from common import (
    LockInfo, LogEntry, LogEntryType, Response, ErrorCode,
    WatchEvent, WatchEventType, generate_id
)

logger = logging.getLogger(__name__)

# 锁键的统一前缀
LOCK_KEY_PREFIX = "/locks/"


def make_lock_key(name: str) -> str:
    """将锁名转换为内部 KV 键"""
    if name.startswith(LOCK_KEY_PREFIX):
        return name
    return LOCK_KEY_PREFIX + name


class LockWaiter:
    """等待锁的客户端 (阻塞式获取锁时使用)"""

    def __init__(self, lock_key: str, session_id: str, lease_id: int):
        self.lock_key = lock_key
        self.session_id = session_id
        self.lease_id = lease_id
        self.event = threading.Event()
        self.result: Optional[Response] = None


class LockService:
    """
    分布式锁服务

    特性:
    - 互斥锁: 同一时刻只有一个持有者
    - 自动释放: 持有者崩溃后租约过期自动释放
    - 非阻塞获取: try_acquire 立即返回成功/失败
    - 阻塞获取: acquire 等待直到成功或超时
    - 可查询: 查询锁的当前持有者
    - 公平性: 简单 FCFS (先来先服务),避免极端饥饿
    - 与会话绑定: 会话过期自动释放其持有的所有锁
    """

    def __init__(self, kv_state_machine=None, lease_manager=None, watch_manager=None):
        self._lock = threading.RLock()
        self._locks: Dict[str, LockInfo] = {}

        # 外部依赖
        self._kv = kv_state_machine
        self._lease = lease_manager
        self._watch = watch_manager

        self._submit_write_fn: Optional[Callable[..., Response]] = None
        self._is_leader_fn: Optional[Callable[[], bool]] = None

        # 等待队列: lock_key -> deque of LockWaiter
        self._wait_queues: Dict[str, deque] = {}

        # 锁变更回调
        self._lock_callbacks: List[Callable[[LockInfo, str], None]] = []

    # ======== 注入依赖 ========

    def set_kv_state_machine(self, kv):
        self._kv = kv

    def set_lease_manager(self, lease):
        self._lease = lease

    def set_watch_manager(self, watch):
        self._watch = watch

    def set_submit_write_fn(self, fn: Callable[..., Response]):
        self._submit_write_fn = fn

    def set_is_leader_fn(self, fn: Callable[[], bool]):
        self._is_leader_fn = fn

    def add_lock_callback(self, cb: Callable[[LockInfo, str], None]):
        """锁变更回调: cb(info, "acquired" | "released")"""
        self._lock_callbacks.append(cb)

    # ======== 核心: 应用 Raft 日志 ========

    def apply_entries(self, entries: List[LogEntry]):
        """应用 Raft 已提交的锁相关日志"""
        for entry in entries:
            if entry.entry_type == LogEntryType.LOCK_ACQUIRE:
                self._apply_acquire(entry)
            elif entry.entry_type == LogEntryType.LOCK_RELEASE:
                self._apply_release(entry)
            elif entry.entry_type == LogEntryType.KV_DELETE:
                # 租约过期导致的键删除 = 锁自动释放
                lock_key = entry.key
                if lock_key.startswith(LOCK_KEY_PREFIX) and lock_key in self._locks:
                    self._apply_release_by_key(lock_key, "lease_expired")
            elif entry.entry_type == LogEntryType.LEASE_REVOKE:
                # 租约撤销 = 可能有锁被释放
                # 稍后在状态机删除键时会通过 KV_DELETE 触发
                pass

    def _apply_acquire(self, entry: LogEntry):
        """
        应用 LOCK_ACQUIRE 日志 (所有节点一致执行)

        这就是"比较并交换"的确定性版本:
        - 因为所有节点按相同顺序执行相同日志
        - 只有第一个执行到某个 lock_key 的 ACQUIRE 会成功
        - 后续的都会发现锁已被持有 → 失败
        """
        lock_key = entry.key
        lease_id = entry.lease_id
        session_id = entry.session_id

        if lock_key in self._locks:
            # 锁已被持有 → 这次获取失败
            logger.debug(
                f"[锁] 获取失败: {lock_key} 已被 "
                f"{self._locks[lock_key].holder_session} 持有"
            )
            self._notify_waiter(lock_key, session_id, Response(
                ErrorCode.LOCK_EXISTS,
                f"锁 {lock_key} 已被持有",
                lock_key=lock_key,
                current_holder=self._locks[lock_key].holder_session,
            ))
            return

        # 获取成功
        info = LockInfo(
            lock_key=lock_key,
            holder_session=session_id,
            lease_id=lease_id,
            acquire_revision=entry.index,
            acquire_time=entry.timestamp,
        )
        self._locks[lock_key] = info

        # 记录键租约关联
        if self._lease:
            self._lease.associate_key_to_lease(lease_id, lock_key)

        # 会话绑定
        if self._lease:
            self._lease.bind_lock_to_session(session_id, lock_key, lease_id)

        logger.info(
            f"[锁] 已获取: {lock_key}, holder={session_id}, "
            f"lease={lease_id}, rev={entry.index}"
        )

        # 通知等待者 (如果有)
        self._notify_waiter(lock_key, session_id, Response(
            ErrorCode.OK, "ok",
            lock_key=lock_key,
            holder_session=session_id,
            lease_id=lease_id,
            acquire_revision=entry.index,
        ))

        # 回调
        for cb in self._lock_callbacks:
            try:
                cb(info, "acquired")
            except Exception as e:
                logger.error(f"锁回调异常: {e}")

    def _apply_release(self, entry: LogEntry):
        """应用 LOCK_RELEASE 日志 (主动释放)"""
        lock_key = entry.key
        session_id = entry.session_id

        info = self._locks.get(lock_key)
        if not info:
            self._notify_waiter_any(lock_key)
            return

        # 检查释放者是否是持有者
        if session_id and info.holder_session != session_id:
            logger.warning(
                f"[锁] 释放失败: {session_id} 不是 {lock_key} 的持有者 "
                f"(持有者是 {info.holder_session})"
            )
            self._notify_waiter(lock_key, session_id, Response(
                ErrorCode.LOCK_NOT_HELD,
                f"会话 {session_id} 不持有锁 {lock_key}",
            ))
            return

        self._apply_release_by_key(lock_key, "released")

    def _apply_release_by_key(self, lock_key: str, reason: str):
        """执行释放 (内部调用)"""
        info = self._locks.pop(lock_key, None)
        if not info:
            return

        # 解除租约关联
        if self._lease:
            self._lease.disassociate_key_from_lease(info.lease_id, lock_key)

        # 解除会话绑定
        if self._lease:
            self._lease.unbind_lock_from_session(
                info.holder_session, lock_key, info.lease_id
            )

        logger.warning(
            f"[锁] 已释放: {lock_key}, 原因={reason}, "
            f"原 holder={info.holder_session}"
        )

        # 回调
        for cb in self._lock_callbacks:
            try:
                cb(info, "released")
            except Exception as e:
                logger.error(f"锁回调异常: {e}")

        # 通知等待队列中的下一个等待者去尝试获取
        self._notify_waiter_any(lock_key)

    # ======== 等待者通知 ========

    def _notify_waiter(self, lock_key: str, session_id: str, result: Response):
        """通知某个特定等待者"""
        with self._lock:
            q = self._wait_queues.get(lock_key)
            if not q:
                return
            for waiter in list(q):
                if waiter.session_id == session_id:
                    waiter.result = result
                    waiter.event.set()
                    q.remove(waiter)
                    return

    def _notify_waiter_any(self, lock_key: str):
        """锁释放后,通知等待队列中有人可以尝试了 (唤醒第一个)"""
        with self._lock:
            q = self._wait_queues.get(lock_key)
            if not q:
                return
            # 唤醒第一个等待者,让它重试
            for waiter in q:
                if not waiter.event.is_set():
                    # 不直接给结果,只唤醒让它重试
                    # (因为还有其他客户端可能在它之前请求)
                    waiter.event.set()
                    break

    # ======== 客户端 API: 非阻塞获取 ========

    def try_acquire(
        self,
        lock_name: str,
        session_id: str,
        ttl: int = 10,
    ) -> Response:
        """
        尝试获取锁 (非阻塞,立即返回)

        关键: 这是一个 Raft 写操作,必须多数提交
              因此即使网络分区,旧 Leader 也无法"假成功"
        """
        lock_key = make_lock_key(lock_name)

        # 1. 为这次锁请求创建一个独立租约
        #    (或者复用已有的会话租约)
        if self._lease:
            # 尝试先给会话创建一个租约
            lease_resp = self._lease.grant_lease(ttl=ttl, session_id=session_id)
            if not lease_resp.success:
                return lease_resp
            lease_id = lease_resp.data["lease_id"]
        else:
            from common import generate_lease_id
            lease_id = generate_lease_id()

        # 2. 提交 LOCK_ACQUIRE 日志到 Raft
        if not self._submit_write_fn:
            return Response(ErrorCode.NO_QUORUM, "系统未就绪")

        resp = self._submit_write_fn(
            LogEntryType.LOCK_ACQUIRE,
            key=lock_key,
            lease_id=lease_id,
            session_id=session_id,
            timestamp=time.time(),
        )

        if not resp.success:
            # Raft 提交失败 (如不是 Leader, 超时等)
            # 尝试撤销刚才创建的租约
            if self._lease and resp.code != ErrorCode.NOT_LEADER:
                try:
                    self._lease.revoke_lease(lease_id)
                except:
                    pass
            return resp

        # 3. 根据状态机结果判断是否成功
        #    (状态机已在 apply_entries 中处理)
        # 检查状态机中锁的持有者是否是我们
        info = self._locks.get(lock_key)
        if info and info.holder_session == session_id:
            return Response(
                ErrorCode.OK, "ok",
                lock_name=lock_name,
                lock_key=lock_key,
                lease_id=lease_id,
                session_id=session_id,
                ttl=ttl,
                acquired=True,
            )
        else:
            # 获取失败,清理租约
            if self._lease:
                try:
                    self._lease.revoke_lease(lease_id)
                except:
                    pass
            current_holder = info.holder_session if info else "unknown"
            return Response(
                ErrorCode.LOCK_EXISTS,
                f"锁 {lock_name} 已被 {current_holder} 持有",
                lock_name=lock_name,
                lock_key=lock_key,
                current_holder=current_holder,
                acquired=False,
            )

    # ======== 客户端 API: 阻塞获取 ========

    def acquire(
        self,
        lock_name: str,
        session_id: str,
        ttl: int = 10,
        timeout: float = -1,
    ) -> Response:
        """
        获取锁 (阻塞直到成功或超时)

        timeout: -1 表示无限等待
        """
        lock_key = make_lock_key(lock_name)
        deadline = time.time() + timeout if timeout > 0 else float("inf")

        while time.time() < deadline:
            # 先尝试非阻塞获取
            resp = self.try_acquire(lock_name, session_id, ttl)

            if resp.success or resp.code != ErrorCode.LOCK_EXISTS:
                return resp

            # 锁被持有,进入等待队列
            # 用 Watch 等待锁释放事件
            remaining = deadline - time.time()
            if remaining <= 0:
                break

            # 方案: 用一个带超时的事件等待锁释放通知
            waiter = LockWaiter(lock_key, session_id, 0)
            with self._lock:
                if lock_key not in self._wait_queues:
                    self._wait_queues[lock_key] = deque()
                self._wait_queues[lock_key].append(waiter)

            # 等待锁释放 (或超时)
            wait_timeout = min(remaining, 5.0)
            waiter.event.wait(timeout=wait_timeout)

            # 被唤醒: 要么是获得锁,要么是锁释放了,重试
            if waiter.result and waiter.result.success:
                return waiter.result

        return Response(
            ErrorCode.TIMEOUT,
            f"获取锁 {lock_name} 超时",
            lock_name=lock_name,
            acquired=False,
        )

    # ======== 客户端 API: 释放锁 ========

    def release(self, lock_name: str, session_id: str) -> Response:
        """
        释放锁 (走 Raft 复制)

        只有持有者才能释放
        """
        lock_key = make_lock_key(lock_name)

        # 检查是否真的持有 (本地快速检查)
        info = self._locks.get(lock_key)
        if not info:
            return Response(
                ErrorCode.LOCK_NOT_HELD,
                f"锁 {lock_name} 当前未被持有",
                lock_name=lock_name,
            )
        if info.holder_session != session_id:
            return Response(
                ErrorCode.LOCK_NOT_HELD,
                f"会话 {session_id} 不持有锁 {lock_name}",
                lock_name=lock_name,
                current_holder=info.holder_session,
            )

        # 提交 LOCK_RELEASE 日志
        if not self._submit_write_fn:
            return Response(ErrorCode.NO_QUORUM, "系统未就绪")

        resp = self._submit_write_fn(
            LogEntryType.LOCK_RELEASE,
            key=lock_key,
            lease_id=info.lease_id,
            session_id=session_id,
            timestamp=time.time(),
        )

        if not resp.success:
            return resp

        # 同时释放租约 (可选: 客户端也可以继续持有租约)
        if self._lease and info.lease_id:
            try:
                self._lease.revoke_lease(info.lease_id)
            except:
                pass

        return Response(
            ErrorCode.OK, "ok",
            lock_name=lock_name,
            lock_key=lock_key,
            released=True,
        )

    # ======== 客户端 API: 续期锁 (给锁关联的租约续期) ========

    def refresh(self, lock_name: str, session_id: str) -> Response:
        """给锁续期 (续约对应的租约)"""
        lock_key = make_lock_key(lock_name)

        info = self._locks.get(lock_key)
        if not info:
            return Response(
                ErrorCode.LOCK_NOT_HELD,
                f"锁 {lock_name} 当前未被持有",
            )
        if info.holder_session != session_id:
            return Response(
                ErrorCode.LOCK_NOT_HELD,
                f"会话 {session_id} 不持有锁 {lock_name}",
            )

        if not self._lease:
            return Response(ErrorCode.OK, "ok")

        return self._lease.keepalive_lease(info.lease_id)

    # ======== 查询 API ========

    def get_lock_info(self, lock_name: str) -> Response:
        """查询锁的当前状态"""
        lock_key = make_lock_key(lock_name)
        info = self._locks.get(lock_key)
        if not info:
            return Response(
                ErrorCode.OK, "ok",
                lock_name=lock_name,
                lock_key=lock_key,
                held=False,
            )
        return Response(
            ErrorCode.OK, "ok",
            lock_name=lock_name,
            lock_key=lock_key,
            held=True,
            holder_session=info.holder_session,
            lease_id=info.lease_id,
            acquire_revision=info.acquire_revision,
            acquire_time=info.acquire_time,
            held_duration=time.time() - info.acquire_time,
        )

    def list_locks(self) -> Response:
        """列出所有当前持有的锁"""
        locks_info = []
        for key, info in self._locks.items():
            # 去掉前缀
            name = key[len(LOCK_KEY_PREFIX):] if key.startswith(LOCK_KEY_PREFIX) else key
            locks_info.append({
                "lock_name": name,
                "lock_key": key,
                "holder_session": info.holder_session,
                "lease_id": info.lease_id,
                "acquire_revision": info.acquire_revision,
                "held_duration": time.time() - info.acquire_time,
            })
        return Response(
            ErrorCode.OK, "ok",
            count=len(locks_info),
            locks=locks_info,
        )

    # ======== 调试 ========

    def dump(self) -> Dict[str, Any]:
        return {
            "locks_count": len(self._locks),
            "wait_queues": {
                k: [
                    {"session": w.session_id, "notified": w.event.is_set()}
                    for w in q
                ]
                for k, q in self._wait_queues.items()
            },
            "locks": {
                k: info.to_dict() for k, info in self._locks.items()
            },
        }
