"""
一致性复制模块 - 简化 Raft 协议实现

核心机制:
1. Leader 选举: 通过 term 和投票机制选举唯一 Leader
2. 日志复制: Leader 接收写请求,复制到多数节点后才提交
3. 安全性: 保证已提交日志不会丢失,不同节点同一 index 日志一致
4. Quorum: 写操作需要多数节点(N/2 + 1)确认才提交,保证一致性
5. 脑裂防护: 通过 term 递增 + 多数投票,网络分区时旧 Leader 无法提交

关键保证:
- 写操作: Leader → 复制到 Follower → 收到多数确认 → 提交 → 应用到状态机
- 读操作: 可直接从 Leader 读(线性一致)或从 Follower 读(可能稍旧)
- 分区处理: 旧 Leader 因无多数节点支持,无法提交新日志,自动降级
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import deque
import threading
import time
import random
import logging

from common import (
    LogEntry, LogEntryType, NodeRole, Response, ErrorCode, generate_id
)

logger = logging.getLogger(__name__)


class RaftConfig:
    """Raft 配置参数"""

    def __init__(
        self,
        election_timeout_min: float = 1.0,
        election_timeout_max: float = 2.0,
        heartbeat_interval: float = 0.2,
        replication_chunk_size: int = 100,
    ):
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        self.replication_chunk_size = replication_chunk_size


class PeerNode:
    """集群中其他节点的表示"""

    def __init__(self, node_id: str, address: str):
        self.node_id = node_id
        self.address = address
        self.next_index: int = 1
        self.match_index: int = 0
        self.last_heartbeat: float = 0.0
        self.active: bool = True


class ConsensusModule:
    """
    一致性复制模块 - 简化 Raft

    写操作提交流程:
    1. 客户端发送写请求到 Leader
    2. Leader 创建 LogEntry,写入本地 log
    3. Leader 并发发送 AppendEntries RPC 给所有 Follower
    4. 每个 Follower 收到后写入本地 log,返回成功
    5. Leader 收到多数节点(N/2 + 1)确认后:
       - 标记该日志为 committed (commit_index 更新)
       - 应用到状态机 (apply)
       - 返回成功给客户端
    6. Leader 在下一次心跳中通知 Follower 新的 commit_index
    7. Follower 也应用已提交的日志到状态机

    一致性保证:
    - 只有被提交的日志才会应用到状态机
    - 不同节点同一 index/term 的日志内容一定相同
    - 已提交的日志不会丢失 (持久化后)
    """

    def __init__(
        self,
        node_id: str,
        peers: List[Tuple[str, str]],
        config: RaftConfig = None,
    ):
        self.node_id = node_id
        self.config = config or RaftConfig()
        self._lock = threading.RLock()

        # Persistent state (需要持久化)
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = [
            LogEntry(index=0, term=0, entry_type=LogEntryType.NOOP)
        ]

        # Volatile state (所有节点)
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Volatile state (仅 Leader)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # 节点状态
        self.role: NodeRole = NodeRole.FOLLOWER
        self.leader_id: Optional[str] = None
        self.election_deadline: float = 0.0
        self._paused: bool = False

        # 集群信息
        self.peers: Dict[str, PeerNode] = {}
        self._all_node_ids: Set[str] = {node_id}
        for pid, addr in peers:
            self.peers[pid] = PeerNode(pid, addr)
            self._all_node_ids.add(pid)
            self.next_index[pid] = 1
            self.match_index[pid] = 0

        # 应用回调
        self._apply_callback: Optional[Callable[[List[LogEntry]], None]] = None
        self._leader_change_callback: Optional[Callable[[str, NodeRole], None]] = None
        self._rpc_handlers: Dict[str, Callable] = {}

        # 挂起的写操作 (index -> (event, response))
        self._pending_writes: Dict[int, Tuple[threading.Event, Response]] = {}

        # 后台线程
        self._running: bool = False
        self._worker_thread: Optional[threading.Thread] = None

    @property
    def quorum_size(self) -> int:
        """多数节点数量 = N/2 + 1"""
        return len(self._all_node_ids) // 2 + 1

    @property
    def cluster_size(self) -> int:
        return len(self._all_node_ids)

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    # ======== 初始化与启动 ========

    def set_apply_callback(self, callback: Callable[[List[LogEntry]], None]):
        """设置日志应用回调 (将日志应用到状态机)"""
        self._apply_callback = callback

    def set_leader_change_callback(self, callback: Callable[[str, NodeRole], None]):
        """设置 Leader 变更回调"""
        self._leader_change_callback = callback

    def register_rpc_handler(self, rpc_name: str, handler: Callable):
        """注册 RPC 处理函数 (模拟 RPC)"""
        self._rpc_handlers[rpc_name] = handler

    def start(self):
        """启动一致性模块"""
        self._running = True
        self._reset_election_deadline()
        self._worker_thread = threading.Thread(
            target=self._worker_loop, daemon=True, name=f"raft-{self.node_id}"
        )
        self._worker_thread.start()
        logger.info(f"节点 {self.node_id} 一致性模块已启动")

    def stop(self):
        """停止一致性模块"""
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=3.0)
        logger.info(f"节点 {self.node_id} 一致性模块已停止")

    # ======== 写操作入口 ========

    def submit_write(self, entry_type: LogEntryType, **kwargs) -> Response:
        """
        提交写操作 (仅 Leader 可处理)

        流程:
        1. 检查当前节点是否为 Leader
        2. 创建 LogEntry,追加到本地 log
        3. 立即触发日志复制
        4. 等待多数节点确认 (commit_index >= log_index)
        5. 返回结果

        Follower 收到写请求时应返回 NOT_LEADER,由客户端重定向到 Leader
        """
        with self._lock:
            if self.role != NodeRole.LEADER:
                return Response(
                    ErrorCode.NOT_LEADER,
                    f"当前节点 {self.node_id} 不是 Leader, Leader 是 {self.leader_id}",
                    leader_id=self.leader_id
                )

            # 创建新日志条目
            new_index = self.last_log_index + 1
            entry = LogEntry(
                index=new_index,
                term=self.current_term,
                entry_type=entry_type,
                **kwargs
            )
            self.log.append(entry)

            # 创建等待事件
            event = threading.Event()
            self._pending_writes[new_index] = (event, Response())

            logger.info(
                f"[Leader {self.node_id}] 提交写操作 index={new_index}, "
                f"type={entry_type.value}, 等待多数节点确认..."
            )

        # 立即触发复制
        self._trigger_replication()

        # 等待提交 (带超时)
        timeout = 5.0
        if not event.wait(timeout=timeout):
            with self._lock:
                self._pending_writes.pop(new_index, None)
            return Response(ErrorCode.TIMEOUT, "写操作超时,未获得多数节点确认")

        with self._lock:
            _, resp = self._pending_writes.pop(new_index, (None, Response(ErrorCode.NO_QUORUM, "未知错误")))
            return resp

    # ======== 日志提交与应用 ========

    def _advance_commit_index(self):
        """
        Leader 根据 match_index 推进 commit_index
        找到最大的 N,使得:
        - N > commit_index
        - 多数节点的 match_index >= N
        - log[N].term == current_term (安全检查,防止提交旧 term 的日志)
        """
        if self.role != NodeRole.LEADER:
            return

        # 从高到低检查
        for n in range(self.last_log_index, self.commit_index, -1):
            if self.log[n].term != self.current_term:
                continue

            # 计算有多少节点 match_index >= n (包括自己)
            count = 1  # 自己算一个
            for pid, peer in self.peers.items():
                if peer.match_index >= n:
                    count += 1

            if count >= self.quorum_size:
                self.commit_index = n
                logger.info(
                    f"[Leader {self.node_id}] 日志已获多数节点确认 ({count}/{self.cluster_size}), "
                    f"commit_index 推进到 {n}"
                )
                break

        # 应用已提交日志
        self._apply_committed_logs()

    def _apply_committed_logs(self):
        """将 [last_applied+1, commit_index] 区间的日志应用到状态机"""
        if getattr(self, '_paused', False):
            return

        with self._lock:
            if self.commit_index <= self.last_applied:
                return

            entries_to_apply = self.log[self.last_applied + 1 : self.commit_index + 1]
            start_idx = self.last_applied + 1
            end_idx = self.commit_index
            self.last_applied = self.commit_index

        apply_error = None
        # 应用到状态机 (在锁外调用,避免死锁)
        if self._apply_callback and entries_to_apply:
            logger.info(
                f"[节点 {self.node_id}] 应用日志 index=[{start_idx}, {end_idx}] "
                f"共 {len(entries_to_apply)} 条到状态机"
            )
            try:
                self._apply_callback(entries_to_apply)
            except Exception as e:
                apply_error = e
                logger.error(
                    f"[节点 {self.node_id}] 应用日志到状态机出错: {e}",
                    exc_info=True
                )

        # 通知挂起的写操作 - 确保始终执行,即使状态机应用出错也不影响提交
        with self._lock:
            for idx in range(start_idx, end_idx + 1):
                if idx in self._pending_writes:
                    event, resp = self._pending_writes[idx]
                    if apply_error is None:
                        resp.code = ErrorCode.OK
                        resp.message = "ok"
                    else:
                        resp.code = ErrorCode.NO_QUORUM
                        resp.message = f"状态机应用异常: {apply_error}"
                    resp.data["index"] = idx
                    resp.data["term"] = self.current_term
                    event.set()

    # ======== 选举相关 ========

    def _reset_election_deadline(self):
        """重置选举超时时间 (随机化避免选票分裂)"""
        self.election_deadline = time.time() + random.uniform(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        )

    def _start_election(self):
        """开始选举 (从 Follower 变为 Candidate)"""
        self.current_term += 1
        self.role = NodeRole.CANDIDATE
        self.voted_for = self.node_id
        self.leader_id = None
        self._reset_election_deadline()

        logger.info(
            f"[Candidate {self.node_id}] 开始选举, term={self.current_term}"
        )

        # 给自己投票
        votes_received = {self.node_id}

        # 并发发送 RequestVote RPC
        for pid in list(self.peers.keys()):
            threading.Thread(
                target=self._send_request_vote,
                args=(pid, votes_received),
                daemon=True,
            ).start()

    def _send_request_vote(self, peer_id: str, votes_received: Set[str]):
        """发送 RequestVote RPC"""
        try:
            handler = self._rpc_handlers.get("request_vote")
            if not handler:
                return

            result = handler(
                self.node_id,
                peer_id,
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=self.last_log_index,
                last_log_term=self.last_log_term,
            )

            with self._lock:
                if self.role != NodeRole.CANDIDATE or self.current_term != result.get("term", 0):
                    if result.get("term", 0) > self.current_term:
                        self._become_follower(result["term"])
                    return

                if result.get("vote_granted"):
                    votes_received.add(peer_id)
                    logger.info(
                        f"[Candidate {self.node_id}] 收到 {peer_id} 的投票, "
                        f"票数 {len(votes_received)}/{self.quorum_size}"
                    )

                    if len(votes_received) >= self.quorum_size:
                        self._become_leader()

        except Exception as e:
            logger.debug(f"RequestVote RPC 失败: {e}")

    def _become_leader(self):
        """成为 Leader"""
        self.role = NodeRole.LEADER
        self.leader_id = self.node_id

        # 初始化 next_index 和 match_index
        for pid in self.peers:
            self.next_index[pid] = self.last_log_index + 1
            self.match_index[pid] = 0

        logger.warning(
            f"===== [Leader {self.node_id}] 选举成功, term={self.current_term} ====="
        )

        # 立即发送一条 NOOP 日志,用于快速确定 commit_index
        noop_entry = LogEntry(
            index=self.last_log_index + 1,
            term=self.current_term,
            entry_type=LogEntryType.NOOP,
        )
        self.log.append(noop_entry)

        if self._leader_change_callback:
            self._leader_change_callback(self.node_id, NodeRole.LEADER)

        # 立即触发心跳和复制
        self._send_heartbeats()

    def _become_follower(self, new_term: int):
        """变为 Follower"""
        old_role = self.role
        self.current_term = new_term
        self.role = NodeRole.FOLLOWER
        self.voted_for = None
        self._reset_election_deadline()

        if old_role != NodeRole.FOLLOWER:
            logger.info(
                f"[节点 {self.node_id}] 从 {old_role.value} 降级为 Follower, term={new_term}"
            )
            if self._leader_change_callback and old_role == NodeRole.LEADER:
                self._leader_change_callback(self.node_id, NodeRole.FOLLOWER)

    # ======== 心跳与复制 ========

    def _worker_loop(self):
        """后台工作线程: 心跳、选举、日志复制"""
        while self._running:
            try:
                with self._lock:
                    role = self.role

                if role == NodeRole.LEADER:
                    # Leader: 定期发送心跳 + 复制日志
                    self._send_heartbeats()
                    self._advance_commit_index()
                    self._apply_committed_logs()
                    time.sleep(self.config.heartbeat_interval)

                else:
                    # Follower / Candidate: 检查选举超时
                    with self._lock:
                        if time.time() >= self.election_deadline:
                            logger.info(
                                f"[节点 {self.node_id}] 选举超时, Leader={self.leader_id}, "
                                f"启动选举"
                            )
                            self._start_election()

                    # 应用已提交的日志
                    self._apply_committed_logs()
                    time.sleep(0.05)

            except Exception as e:
                logger.error(f"Raft worker 循环异常: {e}", exc_info=True)
                time.sleep(0.1)

    def _trigger_replication(self):
        """触发日志复制 (异步,不阻塞)"""
        threading.Thread(target=self._send_heartbeats, daemon=True).start()

    def _send_heartbeats(self):
        """
        Leader 发送心跳/AppendEntries RPC 到所有 Follower
        包含: 新的日志条目 + 提交进度
        """
        if self.role != NodeRole.LEADER:
            return

        for pid in list(self.peers.keys()):
            threading.Thread(
                target=self._send_append_entries,
                args=(pid,),
                daemon=True,
            ).start()

    def _send_append_entries(self, peer_id: str):
        """向指定 Follower 发送 AppendEntries RPC"""
        try:
            with self._lock:
                if self.role != NodeRole.LEADER:
                    return

                ni = self.next_index.get(peer_id, 1)
                prev_log_index = ni - 1
                prev_log_term = (
                    self.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.log) else 0
                )

                # 提取需要发送的日志
                entries = []
                if ni <= self.last_log_index:
                    end_idx = min(ni + self.config.replication_chunk_size, self.last_log_index + 1)
                    entries = [e.to_dict() for e in self.log[ni:end_idx]]

                term = self.current_term
                leader_commit = self.commit_index

            handler = self._rpc_handlers.get("append_entries")
            if not handler:
                return

            result = handler(
                self.node_id,
                peer_id,
                term=term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=leader_commit,
            )

            with self._lock:
                if self.role != NodeRole.LEADER:
                    if result.get("term", 0) > self.current_term:
                        self._become_follower(result["term"])
                    return

                # term 落后,降级
                if result.get("term", 0) > self.current_term:
                    self._become_follower(result["term"])
                    return

                if result.get("success"):
                    # 复制成功,更新 next_index 和 match_index
                    self.next_index[peer_id] = prev_log_index + len(entries) + 1
                    self.match_index[peer_id] = self.next_index[peer_id] - 1
                    self.peers[peer_id].next_index = self.next_index[peer_id]
                    self.peers[peer_id].match_index = self.match_index[peer_id]
                    self.peers[peer_id].last_heartbeat = time.time()
                else:
                    # 日志不一致,递减 next_index 重试
                    self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)
                    logger.debug(
                        f"[Leader {self.node_id}] {peer_id} 日志不一致, "
                        f"next_index 回退到 {self.next_index[peer_id]}"
                    )

                # 每次收到响应后尝试推进 commit_index
                self._advance_commit_index()

        except Exception as e:
            logger.debug(f"AppendEntries RPC 失败到 {peer_id}: {e}")

    # ======== RPC 处理函数 (被远程节点调用) ========

    def handle_request_vote(
        self,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> Dict[str, Any]:
        """
        处理 RequestVote RPC (Follower 收到投票请求)

        投票规则:
        1. 如果请求 term < current_term,拒绝
        2. 如果还没投票 (voted_for is None) 或已投票给同一 candidate
        3. 并且 candidate 的日志至少和自己一样新:
           - last_log_term > 自己的,新
           - last_log_term 相同但 last_log_index >= 自己的,新
        """
        with self._lock:
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                self._become_follower(term)

            # 检查 candidate 日志是否至少和自己一样新
            log_ok = (last_log_term > self.last_log_term) or (
                last_log_term == self.last_log_term and last_log_index >= self.last_log_index
            )

            can_vote = (self.voted_for is None or self.voted_for == candidate_id) and log_ok

            if can_vote:
                self.voted_for = candidate_id
                self._reset_election_deadline()
                logger.info(
                    f"[Follower {self.node_id}] 投票给 {candidate_id}, term={term}"
                )

            return {"term": self.current_term, "vote_granted": can_vote}

    def handle_append_entries(
        self,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[Dict],
        leader_commit: int,
    ) -> Dict[str, Any]:
        """
        处理 AppendEntries RPC (Follower 收到 Leader 的心跳/日志)

        一致性检查:
        1. term < current_term → 拒绝
        2. 日志中 prev_log_index 位置必须存在且 term 等于 prev_log_term
           (保证 Leader 和 Follower 在该位置之前的日志完全一致)
        3. 如果有冲突 (已存在的条目和新条目 term 不同),删除该位置及之后的所有条目
        4. 追加所有不在日志中的新条目
        5. 更新 commit_index = min(leader_commit, last_log_index)
        """
        with self._lock:
            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            if term > self.current_term:
                self._become_follower(term)

            self._reset_election_deadline()
            self.leader_id = leader_id
            self.role = NodeRole.FOLLOWER

            # 日志一致性检查
            if prev_log_index >= len(self.log):
                logger.debug(
                    f"[Follower {self.node_id}] 日志不足, prev_log_index={prev_log_index}, "
                    f"本地长度={len(self.log)}"
                )
                return {"term": self.current_term, "success": False}

            if prev_log_index >= 0 and self.log[prev_log_index].term != prev_log_term:
                logger.debug(
                    f"[Follower {self.node_id}] 日志在 index={prev_log_index} 处 term 冲突, "
                    f"本地 term={self.log[prev_log_index].term}, 请求 term={prev_log_term}"
                )
                # 截断不一致的部分
                self.log = self.log[:prev_log_index]
                return {"term": self.current_term, "success": False}

            # 追加新日志 (去重和冲突处理)
            for i, ed in enumerate(entries):
                entry = LogEntry.from_dict(ed)
                idx = prev_log_index + 1 + i

                if idx < len(self.log):
                    if self.log[idx].term != entry.term:
                        # 冲突,截断
                        self.log = self.log[:idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # 更新 commit_index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.last_log_index)
                logger.debug(
                    f"[Follower {self.node_id}] commit_index 更新到 {self.commit_index}"
                )

            return {"term": self.current_term, "success": True}

    # ======== 读操作辅助 ========

    def get_leader_id(self) -> Optional[str]:
        """获取当前 Leader ID"""
        return self.leader_id

    def is_leader(self) -> bool:
        return self.role == NodeRole.LEADER

    def get_status(self) -> Dict[str, Any]:
        """获取节点状态"""
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_log_index": self.last_log_index,
            "log_length": len(self.log),
            "cluster_size": self.cluster_size,
            "quorum_size": self.quorum_size,
        }
