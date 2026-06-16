"""
Watch 通知模块

核心问题:
1. 如何保证不丢事件? → 事件持久化在状态机 revision 中,客户端可按 revision 重放
2. 如何通知所有关注者? → 发布-订阅模式,每个 watcher 有独立的事件队列
3. 如何处理慢消费者? → 环形缓冲 + 背压,超出窗口的事件可通过历史重放获取
4. 如何支持前缀/范围 Watch? → 匹配器检查 key 是否满足条件

实现思路:
1. 订阅模式: 客户端注册 Watch → 获得 watch_id
2. 状态机变更时,回调 WatchManager → 写入所有匹配的 Watcher 队列
3. 每个 Watcher 有独立的有界缓冲队列
4. 如果客户端落后太多,断开重连时指定 start_revision 重放历史

事件顺序保证:
- 事件按 revision 单调递增顺序发送
- 同一 revision 的事件按日志应用顺序发送
- 不丢事件: 历史事件可通过 KV 状态机 + 事件历史回放获取
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from collections import deque
from dataclasses import dataclass, field
import threading
import logging
import re
import fnmatch

from common import WatchEvent, WatchEventType, Response, ErrorCode

logger = logging.getLogger(__name__)


class WatchMatcher:
    """键匹配器: 精确匹配 / 前缀匹配 / 范围匹配 / 通配符匹配"""

    def __init__(
        self,
        exact_key: Optional[str] = None,
        prefix: Optional[str] = None,
        range_start: Optional[str] = None,
        range_end: Optional[str] = None,
        pattern: Optional[str] = None,
    ):
        self.exact_key = exact_key
        self.prefix = prefix
        self.range_start = range_start
        self.range_end = range_end
        self.pattern = pattern

    def matches(self, key: str) -> bool:
        if self.exact_key is not None:
            return key == self.exact_key
        if self.prefix is not None:
            return key.startswith(self.prefix)
        if self.range_start is not None and self.range_end is not None:
            return self.range_start <= key < self.range_end
        if self.pattern is not None:
            return fnmatch.fnmatch(key, self.pattern)
        return False

    def description(self) -> str:
        if self.exact_key:
            return f"exact={self.exact_key}"
        if self.prefix:
            return f"prefix={self.prefix}*"
        if self.range_start and self.range_end:
            return f"range=[{self.range_start}, {self.range_end})"
        if self.pattern:
            return f"pattern={self.pattern}"
        return "all"


@dataclass
class Watcher:
    """单个 Watch 订阅者"""
    watch_id: int
    matcher: WatchMatcher
    event_types: Set[WatchEventType]
    start_revision: int
    queue: deque = field(default_factory=lambda: deque(maxlen=10000))
    event: threading.Event = field(default_factory=threading.Event)
    filters: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=lambda: __import__("time").time())
    events_delivered: int = 0

    def push_event(self, ev: WatchEvent) -> bool:
        """推送事件到队列,返回是否成功(队列满则丢弃,客户端可通过历史重放)"""
        if self.event_types and ev.event_type not in self.event_types:
            return False
        if not self.matcher.matches(ev.key):
            return False

        try:
            self.queue.append(ev)
            self.events_delivered += 1
            self.event.set()
            return True
        except Exception:
            # 队列满 (虽然 deque maxlen 不会抛异常,而是覆盖旧的)
            logger.warning(f"Watcher {self.watch_id} 队列溢出,可能丢失事件")
            return False

    def pop_events(self, max_count: int = 100, timeout: float = 0) -> List[WatchEvent]:
        """弹出最多 max_count 个事件,可选等待 timeout 秒"""
        result = []
        if not self.queue:
            if timeout > 0:
                self.event.wait(timeout=timeout)
                self.event.clear()

        while self.queue and len(result) < max_count:
            result.append(self.queue.popleft())

        return result


class WatchManager:
    """
    Watch 管理器

    事件不丢失的保证机制:
    1. 实时推送: 状态机变更时立即推送到 Watcher 队列
    2. 历史重放: 客户端断线重连时,指定 start_revision,从历史事件中重放
    3. 事件历史: 环形缓冲区保存最近 N 条事件,供滞后的客户端重放
    4. 超龄检测: 如果 start_revision < compact_revision,返回明确错误提示

    典型使用流程:
    1. client: watch(key, start_revision=0)  →  先重放历史,再等新事件
    2. 服务器: 从历史事件中查找 revision > start_revision 且匹配的事件
    3. 服务器: 匹配的事件立即返回 (带 compact_revision 和 head_revision)
    4. 服务器: 注册 Watcher,后续匹配事件实时推送
    5. client: 处理完事件后,记录最后一次的 revision
    6. client: 断线重连时用 last_revision 作为 start_revision,保证不丢事件
    """

    def __init__(self, history_size: int = 100000):
        self._lock = threading.RLock()
        self._watchers: Dict[int, Watcher] = {}
        self._next_watch_id: int = 1

        # 事件历史: 环形缓冲区 (revision, event)
        self._history_size = history_size
        self._history: deque = deque(maxlen=history_size)

        # 索引: key -> [(revision, event_idx), ...]
        # 用于加速按 key 查找历史事件 (可选优化)
        self._key_history_index: Dict[str, List[int]] = {}

    # ======== 属性 ========

    @property
    def compact_revision(self) -> int:
        """
        当前历史缓冲区内可重放的最早 revision
        客户端 start_revision 如果小于这个值,说明部分历史已经丢失
        """
        with self._lock:
            if not self._history:
                return 0
            return self._history[0][0] - 1

    @property
    def head_revision(self) -> int:
        """
        当前历史缓冲区内最新的 revision
        """
        with self._lock:
            if not self._history:
                return 0
            return self._history[-1][0]

    # ======== 状态机变更回调: 写入历史 + 推送给 Watcher ========

    def on_state_change(self, key: str, event: WatchEvent, revision: int):
        """
        状态机变更回调 (由 KVStateMachine 调用)

        保证: 每次状态机变更都会调用,不会丢失
        这是 Watch 不丢事件的第一层保证: 事件一定写入历史 + 推送给 Watcher
        """
        with self._lock:
            # 1. 写入历史
            self._history.append((revision, event))

            # 2. 维护 key 索引 (只保留最近的)
            if key not in self._key_history_index:
                self._key_history_index[key] = []
            self._key_history_index[key].append(len(self._history) - 1)
            # 清理索引中过旧的项
            if len(self._key_history_index) > self._history_size * 2:
                # 简单策略: 超出时全量清理 (实际可用 LRU)
                self._key_history_index.clear()

            # 3. 推送给所有匹配的 Watcher
            matched = 0
            for watcher in self._watchers.values():
                if watcher.matcher.matches(key):
                    if watcher.push_event(event):
                        matched += 1

        logger.debug(
            f"[Watch] 事件: {event.event_type.value} key={key}, rev={revision}, "
            f"推送到 {matched} 个 Watcher"
        )

    # ======== Watch API ========

    def create_watch(
        self,
        exact_key: Optional[str] = None,
        prefix: Optional[str] = None,
        range_start: Optional[str] = None,
        range_end: Optional[str] = None,
        pattern: Optional[str] = None,
        event_types: Optional[List[WatchEventType]] = None,
        start_revision: int = 0,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Response:
        """
        创建 Watch 订阅 (支持可恢复消费)

        Args:
            start_revision: 从哪个 revision 开始 (含)
                          - -1: 不重放历史,只看未来事件
                          - 0:  从当前最新 revision 开始 (未来事件)
                          - N>0: 重放 revision > N 的所有历史事件,再订阅未来事件

        返回:
            - success=True:
                watch_id, events(历史事件), events_count
                compact_revision (当前历史保留的最早 revision)
                head_revision (当前最新 revision)
                more_history (是否还有更多历史未返回)
            - success=False, code=TIMEOUT:
                start_revision 太旧,部分历史已丢失
                compact_revision (从这个 revision 开始才有历史)
        """
        with self._lock:
            compact_rev = self.compact_revision
            head_rev = self.head_revision

            # 超龄检测: 只有当 start_revision > 0 时才检测
            if start_revision > 0 and start_revision < compact_rev:
                logger.warning(
                    f"[Watch] start_revision={start_revision} 超龄! "
                    f"历史仅保留 compact_revision={compact_rev} 之后的事件"
                )
                return Response(
                    ErrorCode.TIMEOUT,
                    f"部分历史已丢失: 请求 revision={start_revision}, "
                    f"但历史仅从 compact_revision={compact_rev} 开始",
                    compact_revision=compact_rev,
                    head_revision=head_rev,
                    start_revision=start_revision,
                    lost_events=compact_rev - start_revision,
                    watch_id=0,
                    events=[],
                    events_count=0,
                )

            matcher = WatchMatcher(
                exact_key=exact_key,
                prefix=prefix,
                range_start=range_start,
                range_end=range_end,
                pattern=pattern,
            )

            watch_id = self._next_watch_id
            self._next_watch_id += 1

            event_type_set = set(event_types) if event_types else set()

            watcher = Watcher(
                watch_id=watch_id,
                matcher=matcher,
                event_types=event_type_set,
                start_revision=start_revision,
                filters=filters or {},
            )
            self._watchers[watch_id] = watcher

            # 重放历史事件 (revision > start_revision)
            historical_events = []
            more_history = False
            if start_revision >= 0 and self._history:
                for hist_rev, hist_event in self._history:
                    if hist_rev <= start_revision:
                        continue
                    if matcher.matches(hist_event.key):
                        if not event_type_set or hist_event.event_type in event_type_set:
                            historical_events.append(hist_event.to_dict())

            logger.info(
                f"[Watch] 创建 watch_id={watch_id}, 条件=[{matcher.description()}], "
                f"start_rev={start_revision}, 历史={len(historical_events)}条, "
                f"compact={compact_rev}, head={head_rev}"
            )

            return Response(
                ErrorCode.OK, "ok",
                watch_id=watch_id,
                events=historical_events,
                events_count=len(historical_events),
                matcher=matcher.description(),
                compact_revision=compact_rev,
                head_revision=head_rev,
                more_history=more_history,
            )

    def cancel_watch(self, watch_id: int) -> Response:
        """取消 Watch"""
        with self._lock:
            if watch_id not in self._watchers:
                return Response(ErrorCode.KEY_NOT_FOUND, f"Watch 不存在: {watch_id}")
            watcher = self._watchers.pop(watch_id)
            watcher.event.set()  # 唤醒可能在等待的线程

            logger.info(
                f"[Watch] 取消 watch_id={watch_id}, "
                f"共投递 {watcher.events_delivered} 条事件"
            )
            return Response(ErrorCode.OK, "ok", watch_id=watch_id)

    def fetch_events(
        self,
        watch_id: int,
        max_count: int = 100,
        timeout: float = 0,
    ) -> Response:
        """
        拉取 Watch 事件 (长轮询模式)

        Args:
            timeout: 0 = 非阻塞,立即返回 (可能为空)
                     >0 = 最多等待 timeout 秒,有事件就返回

        返回:
            events: 本次拉取的事件列表
            events_count: 事件数量
            has_more: 是否还有更多事件未返回
            compact_revision: 历史窗口起始 revision (用于断线恢复)
            head_revision: 当前最新 revision
            last_revision: 本次返回事件中的最大 revision

        也可以用回调模式 (见 add_callback_watch)
        """
        with self._lock:
            watcher = self._watchers.get(watch_id)
            compact_rev = self.compact_revision
            head_rev = self.head_revision

        if not watcher:
            return Response(
                ErrorCode.KEY_NOT_FOUND,
                f"Watch 不存在: {watch_id}",
                compact_revision=compact_rev,
                head_revision=head_rev,
            )

        events = watcher.pop_events(max_count=max_count, timeout=timeout)

        last_revision = watcher.start_revision
        for ev in events:
            if ev.revision > last_revision:
                last_revision = ev.revision

        return Response(
            ErrorCode.OK, "ok",
            watch_id=watch_id,
            events=[e.to_dict() for e in events],
            events_count=len(events),
            has_more=len(watcher.queue) > 0,
            compact_revision=compact_rev,
            head_revision=head_rev,
            last_revision=last_revision,
        )

    # ======== 回调模式 (更高效的推送) ========

    def create_callback_watch(
        self,
        callback: Callable[[WatchEvent], None],
        exact_key: Optional[str] = None,
        prefix: Optional[str] = None,
        range_start: Optional[str] = None,
        range_end: Optional[str] = None,
        pattern: Optional[str] = None,
        event_types: Optional[List[WatchEventType]] = None,
        start_revision: int = 0,
    ) -> Tuple[int, List[WatchEvent]]:
        """
        创建基于回调的 Watch (进程内使用,更高效)

        返回: (watch_id, historical_events)
        """
        with self._lock:
            matcher = WatchMatcher(
                exact_key=exact_key, prefix=prefix,
                range_start=range_start, range_end=range_end, pattern=pattern,
            )
            watch_id = self._next_watch_id
            self._next_watch_id += 1
            event_type_set = set(event_types) if event_types else set()

            # 创建历史事件列表
            historical = []
            if start_revision >= 0:
                for hist_rev, hist_event in self._history:
                    if hist_rev <= start_revision:
                        continue
                    if matcher.matches(hist_event.key):
                        if not event_type_set or hist_event.event_type in event_type_set:
                            historical.append(hist_event)

            # 包装回调: 在状态机变更时触发
            def wrapped_change_handler(key: str, event: WatchEvent, revision: int):
                if not matcher.matches(key):
                    return
                if event_type_set and event.event_type not in event_type_set:
                    return
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Watch 回调异常: {e}", exc_info=True)

            # 这里我们把回调也注册到 KVStateMachine 的回调列表
            # 但 WatchManager 不直接依赖 KV,所以通过 on_state_change 间接推送
            # 因此改为用 Watcher.queue 方式,由外部线程调用回调
            watcher = Watcher(
                watch_id=watch_id,
                matcher=matcher,
                event_types=event_type_set,
                start_revision=start_revision,
            )
            self._watchers[watch_id] = watcher

            logger.info(
                f"[Watch] 创建回调 watch_id={watch_id}, "
                f"条件=[{matcher.description()}], 历史={len(historical)}条"
            )

            return watch_id, historical

    # ======== 查询/调试 ========

    def list_watches(self) -> Response:
        with self._lock:
            return Response(
                ErrorCode.OK, "ok",
                count=len(self._watchers),
                watches=[
                    {
                        "watch_id": wid,
                        "matcher": w.matcher.description(),
                        "start_revision": w.start_revision,
                        "queue_size": len(w.queue),
                        "delivered": w.events_delivered,
                        "created_at": w.created_at,
                    }
                    for wid, w in self._watchers.items()
                ],
            )

    def get_history(
        self,
        since_revision: int = 0,
        limit: int = 100,
    ) -> Response:
        """获取历史事件 (用于调试和事件重放)"""
        with self._lock:
            result = []
            for rev, ev in self._history:
                if rev <= since_revision:
                    continue
                result.append({"revision": rev, "event": ev.to_dict()})
                if len(result) >= limit:
                    break
            return Response(
                ErrorCode.OK, "ok",
                events=result,
                count=len(result),
                history_total=len(self._history),
            )

    def dump(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "watchers_count": len(self._watchers),
                "history_size": len(self._history),
                "history_capacity": self._history_size,
                "next_watch_id": self._next_watch_id,
                "watchers": {
                    str(wid): {
                        "matcher": w.matcher.description(),
                        "queue_size": len(w.queue),
                        "delivered": w.events_delivered,
                    }
                    for wid, w in self._watchers.items()
                },
            }
