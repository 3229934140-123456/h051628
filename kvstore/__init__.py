"""
键值状态机模块

核心思想:
- 状态机是确定性的: 同一组日志按相同顺序应用 → 得到相同状态
- 所有写操作先经过 Raft 复制,多数提交后再应用到状态机
- 这保证了所有节点的状态机状态最终一致

日志到状态机的映射:
LogEntryType.KV_PUT    → KVStateMachine.put()
LogEntryType.KV_DELETE → KVStateMachine.delete()
LogEntryType.LEASE_*   → 由 LeaseManager 处理 (通过回调)
LogEntryType.LOCK_*    → 由 LockService 处理 (通过回调)

注意: 状态机只处理已 COMMITTED 的日志,绝不处理未提交的日志
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from collections import OrderedDict
import threading
import logging
import re

from common import (
    KVItem, LogEntry, LogEntryType, WatchEvent, WatchEventType, Response, ErrorCode,
    TxnCompare, TxnCompareOp, TxnOp, TxnOpType, TxnRequest,
)

logger = logging.getLogger(__name__)


class KVStateMachine:
    """
    键值状态机 - 复制状态机模式的核心实现

    保证:
    1. 只有已提交的日志才会被应用
    2. 所有节点按相同顺序应用相同日志 → 状态一致
    3. 每一次修改都递增 revision (全局单调递增)
    4. 支持按前缀/范围查询

    数据模型:
    - key -> KVItem { value, create_rev, mod_rev, version, lease_id }
    - revision 是全局的,每次应用任何写操作都会递增
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._store: "OrderedDict[str, KVItem]" = OrderedDict()
        self._revision: int = 0

        # 变更事件回调: (key, event, revision) -> None
        # 用于 Watch 模块订阅变更
        self._change_callbacks: List[Callable[[str, WatchEvent, int], None]] = []

        # 租约相关回调: 状态机需要知道键关联的租约是否过期
        self._lease_check_callback: Optional[Callable[[int], bool]] = None

        # 最近 TXN 执行结果: log_index -> (succeeded, failed_comparison_info)
        # 节点 txn() 接口在日志应用后通过 log_index 取结果, 避免重新 evaluate
        self._txn_results: "OrderedDict[int, Tuple[bool, Dict[str, Any]]]" = OrderedDict()
        self._max_txn_results = 500

    # ======== 回调注册 ========

    def add_change_callback(self, cb: Callable[[str, WatchEvent, int], None]):
        """
        注册变更回调 (Watch 模块使用)
        保证: 每次状态机变更都会触发,不会丢失事件
        """
        self._change_callbacks.append(cb)

    def set_lease_check_callback(self, cb: Callable[[int], bool]):
        """设置租约有效性检查回调"""
        self._lease_check_callback = cb

    # ======== 核心属性 ========

    @property
    def revision(self) -> int:
        """当前全局 revision (单调递增)"""
        return self._revision

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._store)

    # ======== 日志应用入口 (由 ConsensusModule 的 apply_callback 调用) ========

    def apply_entries(self, entries: List[LogEntry]):
        """
        应用一批已提交的日志到状态机

        这是复制状态机模式的核心:
        - 日志是确定性的输入
        - 状态机是确定性的转换
        - 相同输入 → 相同输出状态
        """
        for entry in entries:
            self._apply_single_entry(entry)

    def _apply_single_entry(self, entry: LogEntry):
        """应用单条日志"""
        with self._lock:
            self._revision += 1
            rev = self._revision

            if entry.entry_type == LogEntryType.KV_PUT:
                self._do_put(entry.key, entry.value, entry.lease_id, rev, entry.index)

            elif entry.entry_type == LogEntryType.KV_DELETE:
                self._do_delete(entry.key, rev)

            elif entry.entry_type == LogEntryType.LEASE_REVOKE:
                # 租约被撤销,级联删除所有关联的键
                lease_id = entry.lease_id
                keys_to_delete = [
                    k for k, v in self._store.items() if v.lease_id == lease_id
                ]
                for k in keys_to_delete:
                    self._do_delete(k, rev)

            elif entry.entry_type == LogEntryType.TXN_COMMIT:
                # 事务提交: comparisons → success_ops / failure_ops
                txn_req = TxnRequest.from_dict(entry.data.get("txn", {}))
                self._do_txn(txn_req, rev, entry.index)

    # ======== 事务: Compare-And-Swap 核心 ========

    def evaluate_txn(self, txn: TxnRequest) -> Tuple[bool, Dict[str, Any]]:
        """
        仅评估事务是否满足条件 (不执行前检查, 不修改状态, 供 Leader 在提交前本地检查

        返回: (succeeded, info)
            succeeded: 所有 comparisons 是否为 True
            info: 调试信息
        """
        with self._lock:
            return self._eval_comparisons(txn.comparisons)

    def _eval_comparisons(self, comparisons: List[TxnCompare]) -> Tuple[bool, Dict[str, Any]]:
        """评估条件比较 (必须在锁内)"""
        info = {"failed_comparison": None, "succeeded": True}
        for i, cmp in enumerate(comparisons):
            ok = self._eval_single_compare(cmp)
            if not ok:
                info["succeeded"] = False
                info["failed_comparison"] = {
                    "index": i,
                    "key": cmp.key,
                    "op": cmp.op.value,
                    "expected": cmp.value,
                }
                return False, info
        return True, info

    def _eval_single_compare(self, cmp: TxnCompare) -> bool:
        """评估单个比较条件 (必须在锁内)"""
        key = cmp.key
        item = self._store.get(key)
        exists = item is not None
        # 检查租约有效性
        lease_ok = True
        if exists and item.lease_id > 0 and self._lease_check_callback:
            lease_ok = self._lease_check_callback(item.lease_id)

        op = cmp.op
        if op == TxnCompareOp.KEY_EXISTS:
            return exists and lease_ok
        if op == TxnCompareOp.KEY_NOT_EXISTS:
            return not (exists and lease_ok)
        if not exists:
            # 以下比较需要键存在才有可能为 True
            return False
        if op == TxnCompareOp.VERSION_EQUAL:
            return item.version == cmp.value
        if op == TxnCompareOp.VERSION_NOT_EQUAL:
            return item.version != cmp.value
        if op == TxnCompareOp.MOD_REVISION_EQUAL:
            return item.mod_revision == cmp.value
        if op == TxnCompareOp.CREATE_REVISION_EQUAL:
            return item.create_revision == cmp.value
        if op == TxnCompareOp.VALUE_EQUAL:
            return item.value == cmp.value
        if op == TxnCompareOp.VALUE_NOT_EQUAL:
            return item.value != cmp.value
        if op == TxnCompareOp.LEASE_VALID:
            return item.lease_id == cmp.value and lease_ok
        return False

    def _do_txn(self, txn: TxnRequest, revision: int, log_index: int):
        """执行事务: 先评估条件, 然后执行对应分支 (必须在锁内)"""
        succeeded, info = self._eval_comparisons(txn.comparisons)
        ops_to_exec = txn.success_ops if succeeded else txn.failure_ops
        op_results = []
        for op in ops_to_exec:
            result = self._exec_single_op(op, revision, log_index)
            op_results.append(result)
        info["op_results"] = op_results
        info["ops_executed"] = len(ops_to_exec)
        self._txn_results[log_index] = (succeeded, info)
        if len(self._txn_results) > self._max_txn_results:
            self._txn_results.popitem(last=False)
        logger.info(
            f"[状态机] 事务执行(log_index={log_index}): succeeded={succeeded}, "
            f"执行了 {len(ops_to_exec)} 个操作"
        )
        return succeeded, op_results


    def get_txn_result(self, log_index: int) -> Optional[Tuple[bool, Dict[str, Any]]]:
        """查询指定 log_index 的事务执行结果"""
        with self._lock:
            return self._txn_results.get(log_index)

    def _exec_single_op(self, op: TxnOp, revision: int, log_index: int) -> Dict[str, Any]:
        """执行单个事务操作 (必须在锁内)"""
        if op.op_type == TxnOpType.PUT:
            self._do_put(op.key, op.value, op.lease_id, revision, log_index)
            return {"op": "put", "key": op.key, "value": op.value, "success": True}
        elif op.op_type == TxnOpType.DELETE:
            self._do_delete(op.key, revision)
            return {"op": "delete", "key": op.key, "success": True}
        elif op.op_type == TxnOpType.GET:
            item = self._store.get(op.key)
            return {"op": "get", "key": op.key, "value": item.value if item else None, "success": item is not None}
        return {"op": op.op_type.value, "success": False}

    # ======== 内部操作 ========

    def _do_put(self, key: str, value: Any, lease_id: int, revision: int, log_index: int):
        """执行 PUT 操作 (必须在锁内调用)"""
        event_type = WatchEventType.PUT

        if key in self._store:
            # 更新已有键
            old = self._store[key]
            old.lease_id = lease_id
            old.value = value
            old.mod_revision = revision
            old.version += 1
            item = old
        else:
            # 创建新键
            item = KVItem(
                key=key,
                value=value,
                create_revision=revision,
                mod_revision=revision,
                version=1,
                lease_id=lease_id,
            )
            self._store[key] = item

        logger.debug(
            f"[状态机] PUT key={key}, value={value}, revision={revision}, "
            f"lease_id={lease_id}"
        )

        # 通知 Watch 回调 (在锁内,保证事件顺序)
        event = WatchEvent(
            event_type=event_type,
            key=key,
            value=value,
            revision=revision,
            lease_id=lease_id,
        )
        for cb in self._change_callbacks:
            try:
                cb(key, event, revision)
            except Exception as e:
                logger.error(f"变更回调异常: {e}", exc_info=True)

    def _do_delete(self, key: str, revision: int):
        """执行 DELETE 操作 (必须在锁内调用)"""
        if key not in self._store:
            return

        item = self._store.pop(key)

        logger.debug(
            f"[状态机] DELETE key={key}, revision={revision}, "
            f"was_lease_id={item.lease_id}"
        )

        event = WatchEvent(
            event_type=WatchEventType.DELETE,
            key=key,
            value=None,
            revision=revision,
            lease_id=item.lease_id,
        )
        for cb in self._change_callbacks:
            try:
                cb(key, event, revision)
            except Exception as e:
                logger.error(f"变更回调异常: {e}", exc_info=True)

    # ======== 读操作 (不需要走 Raft,直接访问本地状态机) ========

    def get(self, key: str, check_lease_expired: bool = True) -> Response:
        """
        读取单个键

        注意: 从本地状态机读可能会读到稍旧的数据 (Follower 上)
        如需线性一致读,应该:
        1. 从 Leader 读
        2. Leader 在响应前确认自己仍然是 Leader (心跳确认)
        """
        with self._lock:
            item = self._store.get(key)
            if not item:
                return Response(ErrorCode.KEY_NOT_FOUND, f"键不存在: {key}")

            # 检查关联租约是否已过期 (过期的话相当于键不存在)
            if check_lease_expired and item.lease_id > 0 and self._lease_check_callback:
                if not self._lease_check_callback(item.lease_id):
                    return Response(ErrorCode.KEY_NOT_FOUND, f"键所属租约已过期: {key}")

            return Response(
                ErrorCode.OK, "ok",
                key=key,
                value=item.value,
                item=item.to_dict(),
                revision=self._revision,
            )

    def get_range(self, start_key: str, end_key: str) -> Response:
        """范围查询 [start_key, end_key)"""
        with self._lock:
            result = []
            for k in sorted(self._store.keys()):
                if start_key <= k < end_key:
                    item = self._store[k]
                    result.append(item.to_dict())
            return Response(
                ErrorCode.OK, "ok",
                keys=[r["key"] for r in result],
                items=result,
                count=len(result),
                revision=self._revision,
            )

    def get_prefix(self, prefix: str) -> Response:
        """前缀查询"""
        with self._lock:
            result = []
            for k, v in self._store.items():
                if k.startswith(prefix):
                    result.append(v.to_dict())
            return Response(
                ErrorCode.OK, "ok",
                keys=[r["key"] for r in result],
                items=result,
                count=len(result),
                revision=self._revision,
            )

    def get_all(self) -> Response:
        """获取所有键"""
        with self._lock:
            items = [v.to_dict() for v in self._store.values()]
            return Response(
                ErrorCode.OK, "ok",
                keys=[v["key"] for v in items],
                items=items,
                count=len(items),
                revision=self._revision,
            )

    def exists(self, key: str) -> bool:
        with self._lock:
            item = self._store.get(key)
            if not item:
                return False
            if item.lease_id > 0 and self._lease_check_callback:
                return self._lease_check_callback(item.lease_id)
            return True

    # ======== 租约相关 ========

    def get_keys_by_lease(self, lease_id: int) -> List[str]:
        """获取绑定到指定租约的所有键"""
        with self._lock:
            return [k for k, v in self._store.items() if v.lease_id == lease_id]

    def expire_lease_keys(self, lease_id: int) -> List[str]:
        """
        租约过期时删除所有关联键 (由 LeaseManager 调用)

        注意: 这个操作是本地的,但是是应用 LEASE_REVOKED 日志后的结果
        保证: 所有节点都会一致地删除这些键 (因为 LEASE_REVOKED 是 Raft 提交的)
        """
        with self._lock:
            self._revision += 1
            rev = self._revision
            deleted = []
            keys_to_delete = [k for k, v in self._store.items() if v.lease_id == lease_id]
            for k in keys_to_delete:
                item = self._store.pop(k)
                deleted.append(k)
                event = WatchEvent(
                    event_type=WatchEventType.EXPIRE,
                    key=k,
                    value=None,
                    revision=rev,
                    lease_id=lease_id,
                )
                for cb in self._change_callbacks:
                    try:
                        cb(k, event, rev)
                    except Exception as e:
                        logger.error(f"变更回调异常: {e}", exc_info=True)

            logger.info(
                f"[状态机] 租约 {lease_id} 过期,删除 {len(deleted)} 个键"
            )
            return deleted

    # ======== 调试 ========

    def dump(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "revision": self._revision,
                "count": len(self._store),
                "store": {k: v.to_dict() for k, v in self._store.items()},
            }
