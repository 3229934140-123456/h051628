"""
公共类型定义与接口
所有模块共享的基础数据结构、枚举和接口定义
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import uuid


class NodeRole(Enum):
    """节点角色"""
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


class LogEntryType(Enum):
    """日志条目类型"""
    NOOP = "noop"
    KV_PUT = "kv_put"
    KV_DELETE = "kv_delete"
    LEASE_GRANT = "lease_grant"
    LEASE_REVOKE = "lease_revoke"
    LEASE_KEEPALIVE = "lease_keepalive"
    LOCK_ACQUIRE = "lock_acquire"
    LOCK_RELEASE = "lock_release"
    CONFIG_CHANGE = "config_change"


class WatchEventType(Enum):
    """Watch事件类型"""
    PUT = "put"
    DELETE = "delete"
    EXPIRE = "expire"
    LEASE_REVOKED = "lease_revoked"


class ErrorCode(Enum):
    """错误码"""
    OK = 0
    NOT_LEADER = 1
    NO_QUORUM = 2
    LEASE_NOT_FOUND = 3
    LOCK_NOT_HELD = 4
    LOCK_EXISTS = 5
    KEY_NOT_FOUND = 6
    TIMEOUT = 7
    SESSION_EXPIRED = 8


@dataclass
class LogEntry:
    """复制日志条目"""
    index: int
    term: int
    entry_type: LogEntryType
    key: str = ""
    value: Any = None
    lease_id: int = 0
    session_id: str = ""
    timestamp: float = field(default_factory=time.time)
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "term": self.term,
            "entry_type": self.entry_type.value,
            "key": self.key,
            "value": self.value,
            "lease_id": self.lease_id,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "data": self.data,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LogEntry":
        return cls(
            index=d["index"],
            term=d["term"],
            entry_type=LogEntryType(d["entry_type"]),
            key=d.get("key", ""),
            value=d.get("value"),
            lease_id=d.get("lease_id", 0),
            session_id=d.get("session_id", ""),
            timestamp=d.get("timestamp", time.time()),
            data=d.get("data", {}),
        )


@dataclass
class KVItem:
    """键值存储项"""
    key: str
    value: Any
    create_revision: int
    mod_revision: int
    version: int = 1
    lease_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "create_revision": self.create_revision,
            "mod_revision": self.mod_revision,
            "version": self.version,
            "lease_id": self.lease_id,
        }


@dataclass
class Lease:
    """租约对象"""
    lease_id: int
    ttl: int
    grant_time: float
    expire_time: float
    session_id: str = ""
    keys: Set[str] = field(default_factory=set)
    revoked: bool = False

    def is_expired(self, now: Optional[float] = None) -> bool:
        now = now or time.time()
        return self.revoked or now >= self.expire_time

    def remaining_ttl(self, now: Optional[float] = None) -> int:
        now = now or time.time()
        if self.revoked:
            return 0
        return max(0, int(self.expire_time - now))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lease_id": self.lease_id,
            "ttl": self.ttl,
            "grant_time": self.grant_time,
            "expire_time": self.expire_time,
            "session_id": self.session_id,
            "keys": list(self.keys),
            "revoked": self.revoked,
        }


@dataclass
class WatchEvent:
    """Watch事件"""
    event_type: WatchEventType
    key: str
    value: Any = None
    revision: int = 0
    lease_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "key": self.key,
            "value": self.value,
            "revision": self.revision,
            "lease_id": self.lease_id,
        }


@dataclass
class Session:
    """客户端会话"""
    session_id: str
    create_time: float
    last_heartbeat: float
    lease_ids: Set[int] = field(default_factory=set)
    lock_keys: Set[str] = field(default_factory=set)
    timeout: int = 10

    def is_alive(self, now: Optional[float] = None) -> bool:
        now = now or time.time()
        return (now - self.last_heartbeat) < self.timeout


@dataclass
class LockInfo:
    """锁信息"""
    lock_key: str
    holder_session: str
    lease_id: int
    acquire_revision: int
    acquire_time: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lock_key": self.lock_key,
            "holder_session": self.holder_session,
            "lease_id": self.lease_id,
            "acquire_revision": self.acquire_revision,
            "acquire_time": self.acquire_time,
        }


class Response:
    """统一响应对象"""

    def __init__(self, code: ErrorCode = ErrorCode.OK, message: str = "ok", **kwargs):
        self.code = code
        self.message = message
        self.data = kwargs

    @property
    def success(self) -> bool:
        return self.code == ErrorCode.OK

    def to_dict(self) -> Dict[str, Any]:
        result = {"code": self.code.value, "message": self.message}
        result.update(self.data)
        return result

    def __repr__(self) -> str:
        return f"Response(code={self.code}, message={self.message}, data={self.data})"


def generate_id() -> str:
    return uuid.uuid4().hex


def generate_lease_id() -> int:
    return uuid.uuid4().int & 0xFFFFFFFFFFFFFFFF
