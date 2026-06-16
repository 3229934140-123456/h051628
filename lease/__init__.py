"""
租约管理模块

核心机制:
1. 租约(Lease)是一个有时效性的凭证,客户端通过心跳(KeepAlive)续期
2. 键可以绑定到租约上,租约过期后所有绑定的键自动删除
3. 租约的创建/续期/撤销都通过 Raft 复制,保证所有节点一致
4. Leader 负责检测过期租约,提交 LEASE_REVOKED 日志触发级联删除
5. 租约与会话绑定: 会话停止心跳 → 租约不续期 → 自动过期

为什么需要租约?
- 分布式锁: 锁持有者崩溃后需要自动释放,租约保证这一点
- 临时节点: 服务注册/发现,服务下线后自动注销
- 会话管理: 客户端连接断开后自动清理资源

与 Raft 的交互:
- Grant: 走 Raft → 多数提交 → 状态机创建租约
- KeepAlive: Leader 本地处理 (只更新 expire_time,不提交日志,续期在内存中)
  如果是 Follower,转发到 Leader
  周期性地批量提交 LEASE_KEEPALIVE 日志,保证 Follower 上的租约也续期
- Revoke: 走 Raft → 多数提交 → 状态机删除租约及关联键
- 过期检测: Leader 本地检测,提交 LEASE_REVOKED 日志 → 全局生效
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import deque
import threading
import time
import logging

from common import (
    Lease, LogEntry, LogEntryType, Session, Response, ErrorCode,
    generate_lease_id, generate_id
)

logger = logging.getLogger(__name__)


class LeaseManager:
    """
    租约管理器

    租约生命周期:
    1. GRANTED (创建)  ← 客户端请求 Grant
    2. ACTIVE  (活跃)  ← 客户端定期 KeepAlive 续期
    3. EXPIRING(即将过期) ← 超过一段时间没收到 KeepAlive
    4. REVOKED (已撤销) ← Leader 检测到过期,提交撤销日志
    """

    def __init__(self, kv_state_machine=None):
        self._lock = threading.RLock()
        self._leases: Dict[int, Lease] = {}
        self._sessions: Dict[str, Session] = {}

        # 租约 ID → 会话 ID 映射
        self._lease_to_session: Dict[int, str] = {}

        # 外部依赖 (后续通过 setter 注入)
        self._kv = kv_state_machine
        self._submit_write_fn: Optional[Callable[..., Response]] = None
        self._is_leader_fn: Optional[Callable[[], bool]] = None

        # 租约变更回调
        self._lease_callbacks: List[Callable[[Lease, str], None]] = []

        # 后台过期检查
        self._running: bool = False
        self._check_thread: Optional[threading.Thread] = None
        self._keepalive_batch_thread: Optional[threading.Thread] = None

        # 续期批处理队列 (用于批量同步到 Follower)
        self._pending_keepalives: Set[int] = set()

    # ======== 注入外部依赖 ========

    def set_kv_state_machine(self, kv):
        self._kv = kv
        # 注册回调: 状态机需要检查租约是否有效
        if self._kv:
            self._kv.set_lease_check_callback(self.is_lease_valid)

    def set_submit_write_fn(self, fn: Callable[..., Response]):
        """设置提交写操作的函数 (通常是节点的 submit_write)"""
        self._submit_write_fn = fn

    def set_is_leader_fn(self, fn: Callable[[], bool]):
        """设置判断是否为 Leader 的函数"""
        self._is_leader_fn = fn

    def add_lease_callback(self, cb: Callable[[Lease, str], None]):
        """
        添加租约变更回调
        cb(lease, action): action ∈ {"granted", "renewed", "revoked"}
        """
        self._lease_callbacks.append(cb)

    # ======== 启动/停止 ========

    def start(self):
        self._running = True
        self._check_thread = threading.Thread(
            target=self._expiry_check_loop, daemon=True, name="lease-expiry"
        )
        self._check_thread.start()

        self._keepalive_batch_thread = threading.Thread(
            target=self._keepalive_batch_loop, daemon=True, name="lease-keepalive-batch"
        )
        self._keepalive_batch_thread.start()

        logger.info("租约管理器已启动")

    def stop(self):
        self._running = False
        for t in [self._check_thread, self._keepalive_batch_thread]:
            if t:
                t.join(timeout=2.0)
        logger.info("租约管理器已停止")

    # ======== 核心: 日志应用 (由 Raft 的 apply_callback 调用) ========

    def apply_entries(self, entries: List[LogEntry]):
        """应用 Raft 已提交的日志到租约管理器"""
        for entry in entries:
            self._apply_single_entry(entry)

    def _apply_single_entry(self, entry: LogEntry):
        if entry.entry_type == LogEntryType.LEASE_GRANT:
            self._apply_grant(entry)
        elif entry.entry_type == LogEntryType.LEASE_KEEPALIVE:
            self._apply_keepalive(entry)
        elif entry.entry_type == LogEntryType.LEASE_REVOKE:
            self._apply_revoke(entry)

    def _apply_grant(self, entry: LogEntry):
        """应用 LEASE_GRANT 日志 (所有节点都会执行)"""
        lease_id = entry.lease_id
        ttl = entry.data.get("ttl", 10)
        session_id = entry.session_id

        now = entry.timestamp
        lease = Lease(
            lease_id=lease_id,
            ttl=ttl,
            grant_time=now,
            expire_time=now + ttl,
            session_id=session_id,
            keys=set(),
            revoked=False,
        )
        self._leases[lease_id] = lease
        self._lease_to_session[lease_id] = session_id

        # 关联到会话
        if session_id and session_id not in self._sessions:
            self._sessions[session_id] = Session(
                session_id=session_id,
                create_time=now,
                last_heartbeat=now,
                timeout=ttl,
            )
        if session_id:
            self._sessions[session_id].lease_ids.add(lease_id)

        logger.info(
            f"[租约] 已应用 GRANT: lease_id={lease_id}, ttl={ttl}s, "
            f"session={session_id}, expire={lease.expire_time:.1f}"
        )

        for cb in self._lease_callbacks:
            try:
                cb(lease, "granted")
            except Exception as e:
                logger.error(f"租约回调异常: {e}")

    def _apply_keepalive(self, entry: LogEntry):
        """应用 LEASE_KEEPALIVE 日志 (批量续期)"""
        lease_ids = entry.data.get("lease_ids", [])
        ttl = entry.data.get("ttl", None)

        for lid in lease_ids:
            if lid not in self._leases:
                continue
            lease = self._leases[lid]
            if lease.revoked:
                continue
            # 使用指定的 TTL 或原始 TTL
            effective_ttl = ttl if ttl else lease.ttl
            lease.expire_time = max(
                lease.expire_time,
                time.time() + effective_ttl
            )
            logger.debug(
                f"[租约] 已应用 KEEPALIVE: lease_id={lid}, "
                f"新过期时间={lease.expire_time:.1f}"
            )

            # 更新会话的心跳时间
            sid = self._lease_to_session.get(lid)
            if sid and sid in self._sessions:
                self._sessions[sid].last_heartbeat = time.time()

            for cb in self._lease_callbacks:
                try:
                    cb(lease, "renewed")
                except Exception as e:
                    logger.error(f"租约回调异常: {e}")

    def _apply_revoke(self, entry: LogEntry):
        """
        应用 LEASE_REVOKE 日志 (所有节点都会执行)
        这会级联触发 KV 状态机删除关联键
        """
        lease_id = entry.lease_id

        if lease_id not in self._leases:
            return

        lease = self._leases[lease_id]
        lease.revoked = True

        # 从会话中移除
        sid = self._lease_to_session.pop(lease_id, None)
        if sid and sid in self._sessions:
            self._sessions[sid].lease_ids.discard(lease_id)
            self._sessions[sid].lock_keys.difference_update(
                {k for k in self._sessions[sid].lock_keys if k.startswith(f"/lock/{lease_id}/")}
            )

        # 通知 KV 状态机删除关联键
        if self._kv:
            self._kv.expire_lease_keys(lease_id)

        logger.warning(
            f"[租约] 已应用 REVOKE: lease_id={lease_id}, "
            f"删除 {len(lease.keys)} 个关联键"
        )

        for cb in self._lease_callbacks:
            try:
                cb(lease, "revoked")
            except Exception as e:
                logger.error(f"租约回调异常: {e}")

    # ======== 客户端 API: 创建租约 ========

    def grant_lease(self, ttl: int, session_id: str = "") -> Response:
        """
        请求创建租约 (走 Raft 复制)

        流程:
        1. 生成唯一 lease_id
        2. 通过 submit_write_fn 提交 LEASE_GRANT 日志
        3. Raft 复制到多数节点后提交
        4. apply_entries 创建租约对象
        5. 返回成功
        """
        if not session_id:
            session_id = generate_id()

        lease_id = generate_lease_id()
        now = time.time()

        if not self._submit_write_fn:
            return Response(ErrorCode.NO_QUORUM, "系统未就绪")

        resp = self._submit_write_fn(
            LogEntryType.LEASE_GRANT,
            key="",
            lease_id=lease_id,
            session_id=session_id,
            timestamp=now,
            data={"ttl": ttl},
        )

        if not resp.success:
            return resp

        resp.data.update({
            "lease_id": lease_id,
            "ttl": ttl,
            "session_id": session_id,
        })
        return resp

    # ======== 客户端 API: 续期租约 ========

    def keepalive_lease(self, lease_id: int) -> Response:
        """
        租约心跳续期

        注意: KeepAlive 有优化:
        - Leader 在内存中直接更新 expire_time (低延迟)
        - 同时将 lease_id 加入批处理队列
        - 批处理线程定期合并多个续期为一条 LEASE_KEEPALIVE 日志,走 Raft 复制
        - 这减少了 Raft 日志数量,但保证所有节点最终一致

        为什么不每条续期都走 Raft?
        - KeepAlive 非常频繁 (每秒可能多次)
        - 大多数续期只是延长过期时间,延迟几秒复制问题不大
        - 但需要保证: 如果 Leader 切换,新 Leader 上的租约不会立即过期
        """
        with self._lock:
            lease = self._leases.get(lease_id)
            if not lease:
                return Response(ErrorCode.LEASE_NOT_FOUND, f"租约不存在: {lease_id}")
            if lease.revoked:
                return Response(ErrorCode.LEASE_NOT_FOUND, f"租约已撤销: {lease_id}")

            # 内存中立即续期 (Leader 本地)
            lease.expire_time = max(
                lease.expire_time,
                time.time() + lease.ttl
            )

            # 加入批处理队列,稍后同步到 Follower
            self._pending_keepalives.add(lease_id)

            # 更新会话心跳
            sid = self._lease_to_session.get(lease_id)
            if sid and sid in self._sessions:
                self._sessions[sid].last_heartbeat = time.time()

            logger.debug(
                f"[租约] KeepAlive 本地续期: lease_id={lease_id}, "
                f"expire={lease.expire_time:.1f}, 剩余 TTL={lease.remaining_ttl()}s"
            )

            return Response(
                ErrorCode.OK, "ok",
                lease_id=lease_id,
                ttl=lease.ttl,
                remaining_ttl=lease.remaining_ttl(),
            )

    def _keepalive_batch_loop(self):
        """
        批量续期线程: 定期将内存中的续期同步到 Raft
        保证即使 Leader 切换,新 Leader 上的租约也不会过期
        """
        while self._running:
            try:
                # 仅 Leader 执行批量同步
                if self._is_leader_fn and self._is_leader_fn():
                    with self._lock:
                        pending = list(self._pending_keepalives)
                        self._pending_keepalives.clear()

                    if pending and self._submit_write_fn:
                        # 过滤掉已经撤销的
                        valid_ids = []
                        for lid in pending:
                            lease = self._leases.get(lid)
                            if lease and not lease.revoked:
                                valid_ids.append(lid)

                        if valid_ids:
                            resp = self._submit_write_fn(
                                LogEntryType.LEASE_KEEPALIVE,
                                lease_id=0,
                                timestamp=time.time(),
                                data={"lease_ids": valid_ids},
                            )
                            if not resp.success and resp.code == ErrorCode.NOT_LEADER:
                                # 不是 Leader 了,清空批处理队列
                                with self._lock:
                                    self._pending_keepalives.clear()
                                logger.info("[租约] Leader 变更,清空续期批处理队列")

                time.sleep(0.5)  # 每 500ms 批量同步一次
            except Exception as e:
                logger.error(f"批量续期线程异常: {e}", exc_info=True)
                time.sleep(0.5)

    # ======== 客户端 API: 撤销租约 ========

    def revoke_lease(self, lease_id: int) -> Response:
        """
        主动撤销租约 (走 Raft 复制)
        会触发删除所有关联键
        """
        with self._lock:
            lease = self._leases.get(lease_id)
            if not lease:
                return Response(ErrorCode.LEASE_NOT_FOUND, f"租约不存在: {lease_id}")

        if not self._submit_write_fn:
            return Response(ErrorCode.NO_QUORUM, "系统未就绪")

        return self._submit_write_fn(
            LogEntryType.LEASE_REVOKE,
            key="",
            lease_id=lease_id,
            timestamp=time.time(),
        )

    # ======== 过期检测线程 ========

    def _expiry_check_loop(self):
        """
        Leader 负责检测过期租约

        为什么只有 Leader 检测?
        - 避免多个节点同时发起 LEASE_REVOKE,造成冲突
        - Leader 拥有最新的提交日志,判断最准确

        检测流程:
        1. 遍历所有租约
        2. 如果 now >= expire_time 且未撤销 → 提交 LEASE_REVOKE 日志
        3. 日志应用后所有节点一致删除租约及关联键
        """
        while self._running:
            try:
                # 仅 Leader 执行过期检测
                if self._is_leader_fn and self._is_leader_fn():
                    now = time.time()
                    with self._lock:
                        expiring_ids = [
                            lid for lid, lease in self._leases.items()
                            if not lease.revoked and now >= lease.expire_time
                        ]

                    for lid in expiring_ids:
                        logger.warning(
                            f"[租约] 检测到租约过期: lease_id={lid}, "
                            f"提交 LEASE_REVOKE 日志..."
                        )
                        if self._submit_write_fn:
                            resp = self._submit_write_fn(
                                LogEntryType.LEASE_REVOKE,
                                key="",
                                lease_id=lid,
                                timestamp=now,
                            )
                            if not resp.success:
                                logger.debug(
                                    f"[租约] 撤销租约 {lid} 失败: {resp.message}"
                                )

                # 同时检查会话过期 (会话绑定的租约也会过期)
                self._check_sessions()

                time.sleep(0.2)
            except Exception as e:
                logger.error(f"过期检测线程异常: {e}", exc_info=True)
                time.sleep(0.2)

    def _check_sessions(self):
        """
        检查会话是否过期
        会话过期 → 撤销其所有租约 → 释放所有锁和临时键
        """
        if not (self._is_leader_fn and self._is_leader_fn()):
            return

        now = time.time()
        with self._lock:
            expired_sessions = [
                sid for sid, sess in self._sessions.items()
                if not sess.is_alive(now)
            ]

        for sid in expired_sessions:
            sess = self._sessions.get(sid)
            if not sess:
                continue
            logger.warning(
                f"[会话] 会话 {sid} 已过期 (last_heartbeat="
                f"{now - sess.last_heartbeat:.1f}s 前), 清理其资源"
            )

            # 撤销会话的所有租约
            for lid in list(sess.lease_ids):
                if self._submit_write_fn:
                    self._submit_write_fn(
                        LogEntryType.LEASE_REVOKE,
                        key="",
                        lease_id=lid,
                        timestamp=now,
                    )

            with self._lock:
                self._sessions.pop(sid, None)

    # ======== 会话管理 ========

    def create_session(self, timeout: int = 10) -> Response:
        """创建会话 + 绑定一个根租约"""
        resp = self.grant_lease(ttl=timeout, session_id="")
        if not resp.success:
            return resp

        session_id = resp.data["session_id"]
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].timeout = timeout

        return Response(
            ErrorCode.OK, "ok",
            session_id=session_id,
            lease_id=resp.data["lease_id"],
            timeout=timeout,
        )

    def session_heartbeat(self, session_id: str) -> Response:
        """会话心跳 (续期其所有租约)"""
        with self._lock:
            sess = self._sessions.get(session_id)
            if not sess:
                return Response(ErrorCode.SESSION_EXPIRED, f"会话不存在或已过期: {session_id}")
            sess.last_heartbeat = time.time()
            lease_ids = list(sess.lease_ids)

        # 续期每个租约
        last_result = Response()
        for lid in lease_ids:
            last_result = self.keepalive_lease(lid)
            if not last_result.success:
                break

        return last_result if last_result else Response(ErrorCode.OK, "ok")

    def get_session(self, session_id: str) -> Optional[Session]:
        with self._lock:
            return self._sessions.get(session_id)

    def bind_lock_to_session(self, session_id: str, lock_key: str, lease_id: int):
        """将锁绑定到会话 (会话过期时自动释放锁)"""
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].lock_keys.add(lock_key)
                self._sessions[session_id].lease_ids.add(lease_id)
            self._lease_to_session[lease_id] = session_id

    def unbind_lock_from_session(self, session_id: str, lock_key: str, lease_id: int):
        """解除锁与会话的绑定"""
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].lock_keys.discard(lock_key)
                self._sessions[session_id].lease_ids.discard(lease_id)
            if self._lease_to_session.get(lease_id) == session_id:
                pass  # 保留映射,租约撤销时会清理

    # ======== 查询 ========

    def is_lease_valid(self, lease_id: int) -> bool:
        """
        检查租约是否有效 (未过期且未撤销)
        由 KV 状态机调用
        """
        with self._lock:
            lease = self._leases.get(lease_id)
            if not lease:
                return False
            return not lease.is_expired()

    def get_lease(self, lease_id: int) -> Response:
        with self._lock:
            lease = self._leases.get(lease_id)
            if not lease:
                return Response(ErrorCode.LEASE_NOT_FOUND, f"租约不存在: {lease_id}")
            return Response(
                ErrorCode.OK, "ok",
                lease_id=lease_id,
                ttl=lease.ttl,
                remaining_ttl=lease.remaining_ttl(),
                granted_time=lease.grant_time,
                expire_time=lease.expire_time,
                session_id=lease.session_id,
                keys=list(lease.keys),
                revoked=lease.revoked,
            )

    def list_leases(self) -> Response:
        with self._lock:
            return Response(
                ErrorCode.OK, "ok",
                count=len(self._leases),
                leases=[
                    {
                        "lease_id": l.lease_id,
                        "ttl": l.ttl,
                        "remaining_ttl": l.remaining_ttl(),
                        "session_id": l.session_id,
                        "keys_count": len(l.keys),
                        "revoked": l.revoked,
                    }
                    for l in self._leases.values()
                ],
            )

    # ======== KV 状态机关联: 记录键绑定到哪个租约 ========

    def associate_key_to_lease(self, lease_id: int, key: str):
        """记录键租约关联 (由 LockService/KVService 调用)"""
        with self._lock:
            if lease_id in self._leases:
                self._leases[lease_id].keys.add(key)

    def disassociate_key_from_lease(self, lease_id: int, key: str):
        with self._lock:
            if lease_id in self._leases:
                self._leases[lease_id].keys.discard(key)

    # ======== 调试 ========

    def dump(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "leases_count": len(self._leases),
                "sessions_count": len(self._sessions),
                "leases": {
                    str(lid): {
                        "ttl": l.ttl,
                        "remaining_ttl": l.remaining_ttl(),
                        "expire_time": l.expire_time,
                        "session_id": l.session_id,
                        "keys": list(l.keys),
                        "revoked": l.revoked,
                    }
                    for lid, l in self._leases.items()
                },
                "pending_keepalives": list(self._pending_keepalives),
            }
