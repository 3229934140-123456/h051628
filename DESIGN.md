# 分布式锁与协调服务 - 系统设计说明

## 一、总体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        客户端 (Client)                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────────┐  │
│  │ KV SDK   │ │ Lock SDK │ │ Watch SDK│ │ 会话/租约管理       │  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────────┬──────────┘  │
└───────┼────────────┼────────────┼───────────────────┼─────────────┘
        │            │            │                   │
        ▼            ▼            ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    节点 (Node)  × N                               │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                  API 路由层 (Node)                           │  │
│  │   Leader 处理写 / Follower 自动转发                          │  │
│  └──────────────────────────┬──────────────────────────────────┘  │
│                             │                                      │
│  ┌───────────────┐  ┌───────▼────────┐  ┌─────────────────────┐  │
│  │  租约管理     │  │  锁服务        │  │  KV 状态机           │  │
│  │  LeaseManager│  │  LockService   │  │  KVStateMachine      │  │
│  └───────┬───────┘  └───────┬────────┘  └──────────┬──────────┘  │
│          │                  │                       │              │
│          └──────────────────┼───────────────────────┘              │
│                             ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    Watch 通知                                │  │
│  │                    WatchManager                              │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                             │                                      │
│                     ▲───────▼───────▲                              │
│                     │   日志应用    │  (确定性顺序)                │
│                ┌────┴───────────────┴────┐                         │
│                │  一致性复制 (Raft)       │                         │
│                │  ConsensusModule         │                         │
│                │  - Leader 选举           │                         │
│                │  - 日志复制              │                         │
│                │  - Quorum 提交           │                         │
│                └───────────┬──────────────┘                         │
└────────────────────────────┼──────────────────────────────────────┘
                             │
                     节点间 RPC (AppendEntries / RequestVote)
```

---

## 二、模块代码索引

| 模块 | 文件 | 核心类 |
|------|------|--------|
| 公共类型 | [common/__init__.py](file:///d:/trae-bz/TraeProjects/28/common/__init__.py) | `LogEntry`, `Lease`, `Session`, `LockInfo`, `WatchEvent` |
| 一致性复制 | [consensus/__init__.py](file:///d:/trae-bz/TraeProjects/28/consensus/__init__.py) | `ConsensusModule` |
| KV 状态机 | [kvstore/__init__.py](file:///d:/trae-bz/TraeProjects/28/kvstore/__init__.py) | `KVStateMachine` |
| 租约管理 | [lease/__init__.py](file:///d:/trae-bz/TraeProjects/28/lease/__init__.py) | `LeaseManager` |
| Watch 通知 | [watch/__init__.py](file:///d:/trae-bz/TraeProjects/28/watch/__init__.py) | `WatchManager` |
| 锁服务 | [lock/__init__.py](file:///d:/trae-bz/TraeProjects/28/lock/__init__.py) | `LockService` |
| 节点核心 | [node/__init__.py](file:///d:/trae-bz/TraeProjects/28/node/__init__.py) | `Node`, `ClusterSimulator` |
| 客户端 SDK | [client/__init__.py](file:///d:/trae-bz/TraeProjects/28/client/__init__.py) | `DistributedClient`, `DistributedLock`, `Watcher` |
| 完整示例 | [examples/demo.py](file:///d:/trae-bz/TraeProjects/28/examples/demo.py) | `main()` 演示所有功能 |

---

## 三、关键技术问题详解

### 3.1 写操作如何复制到多数节点才提交 — Quorum 机制

**核心代码位置**：[ConsensusModule.submit_write](file:///d:/trae-bz/TraeProjects/28/consensus/__init__.py#L230-L293) 和 [ConsensusModule._advance_commit_index](file:///d:/trae-bz/TraeProjects/28/consensus/__init__.py#L295-L330)

```
写操作提交流程:

客户端
   │  (1) PUT("key", "value")
   ▼
┌───────────────────────────────────────────────────────────┐
│  Leader 节点                                               │
│                                                           │
│  [1] 创建 LogEntry(index=5, term=3, type=KV_PUT, ...)    │
│  [2] 追加到本地 log[]                                     │
│  [3] 并发发送 AppendEntries RPC                           │
└──────────────┬───────────────────────┬────────────────────┘
               │                       │
       ┌───────▼───────┐        ┌──────▼────────┐
       │   Follower A  │        │   Follower B  │
       │  [4] 写入log  │        │  [4] 写入log  │
       │  [5] 返回成功 │        │  [5] 返回成功 │
       └───────┬───────┘        └──────┬────────┘
               │                       │
               └───────────┬───────────┘
                           │  [6] Leader 统计: 2/3 = Quorum ✓
                           ▼
                commit_index 推进到 5
                           │
                           ▼
                [7] 应用到 KVStateMachine
                           │
                           ▼
                [8] 返回成功给客户端
```

**关键点**：

1. **Quorum 计算**：`quorum_size = N // 2 + 1`，3 节点集群需要 2 票，5 节点需要 3 票
2. **安全检查**：只推进当前 term 的日志（`log[N].term == current_term`），防止提交旧 Leader 的未提交日志
3. **通知 Follower**：在下一次心跳中携带新的 `leader_commit`，Follower 也推进自己的 `commit_index`
4. **线性一致**：只有被多数节点持久化的日志才会被应用到状态机，保证即使 Leader 切换也不会丢失

---

### 3.2 键值如何作为复制状态机 — State Machine Replication

**核心代码位置**：[KVStateMachine.apply_entries](file:///d:/trae-bz/TraeProjects/28/kvstore/__init__.py#L106-L130) 和 [Node._apply_committed_entries](file:///d:/trae-bz/TraeProjects/28/node/__init__.py#L126-L146)

**原理**：复制状态机 = 确定性的输入（日志）+ 确定性的转换函数 → 所有节点产出相同状态

```
                    ┌──────────────────────┐
                    │   相同日志序列       │
                    │   log[1], log[2],... │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
   │  apply()     │    │  apply()     │    │  apply()     │
   │  Node A      │    │  Node B      │    │  Node C      │
   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
          │                   │                   │
          ▼                   ▼                   ▼
   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
   │ State A      │    │ State B      │    │ State C      │
   │ {            │    │ {            │    │ {            │
   │  "k1":"v1"   │  = │  "k1":"v1"   │  = │  "k1":"v1"   │
   │  "k2":"v2"   │    │  "k2":"v2"   │    │  "k2":"v2"   │
   │ }            │    │ }            │    │ }            │
   └──────────────┘    └──────────────┘    └──────────────┘
```

**确定性保证**：

1. **相同输入**：Raft 保证所有节点的 log[] 最终完全相同
2. **相同顺序**：按 log index 升序应用，绝不乱序
3. **相同操作**：`KV_PUT("k","v")` 在任何节点执行结果相同
4. **单调 revision**：每次应用全局 `_revision++`，保证版本号一致

**日志类型到状态机操作的映射**：

| LogEntryType | KVStateMachine 操作 |
|---|---|
| `KV_PUT` | `_do_put(key, value, lease_id, rev, idx)` |
| `KV_DELETE` | `_do_delete(key, rev)` |
| `LEASE_REVOKED` | 遍历删除所有 `lease_id` 匹配的键 → `_do_delete` |

---

### 3.3 租约如何让键在持有者停止心跳后自动过期

**核心代码位置**：[LeaseManager._expiry_check_loop](file:///d:/trae-bz/TraeProjects/28/lease/__init__.py#L346-L385) 和 [LeaseManager._apply_revoke](file:///d:/trae-bz/TraeProjects/28/lease/__init__.py#L187-L231)

**租约生命周期**：

```
客户端崩溃 / 断网
   │
   ▼  心跳停止
┌─────────┐   Grant    ┌─────────┐  KeepAlive   ┌─────────┐  过期检测  ┌─────────┐
│ Created │ ────────►  │  Active │ ───────────► │ Expiring│ ────────► │ Revoked │
│  (TTL)  │            │ (续期中) │              │(即将过期)│           │(级联删除)│
└─────────┘            └─────────┘              └─────────┘            └────┬────┘
                                                                             │
                                                    ┌────────────────────────┤
                                                    ▼                        ▼
                                            删除绑定的锁键     触发 Watch EXPIRE 事件
```

**具体实现步骤**：

1. **创建租约**：`LEASE_GRANT` 走 Raft，所有节点创建 `Lease{expire_time = now + TTL}`
2. **心跳续期**：
   - Leader 内存中立即续期（低延迟）：`lease.expire_time = now + TTL`
   - 批处理同步：每 500ms 合并为 `LEASE_KEEPALIVE` 日志走 Raft，保证 Follower 也续期
3. **过期检测（仅 Leader）**：`_expiry_check_loop` 每 200ms 遍历：
   ```python
   if now >= lease.expire_time:
       submit_write(LEASE_REVOKE, lease_id=X)
   ```
4. **级联删除**：`LEASE_REVOKE` 日志应用时，`KVStateMachine.expire_lease_keys()` 删除所有绑定键
5. **事件通知**：删除时触发 `WatchEventType.EXPIRE`，通知所有 Watcher

**为什么只有 Leader 检测过期？**
- 避免多节点重复提交 `LEASE_REVOKE`
- Leader 拥有最新的提交日志，判断最准确
- 即使 Leader 切换，新 Leader 上的 `expire_time` 也已通过 KeepAlive 日志同步

---

### 3.4 Watch 如何在键变化时通知所有关注者且不丢事件

**核心代码位置**：[WatchManager.on_state_change](file:///d:/trae-bz/TraeProjects/28/watch/__init__.py#L161-L197) 和 [WatchManager.create_watch](file:///d:/trae-bz/TraeProjects/28/watch/__init__.py#L200-L258)

**双重保证机制**：实时推送 + 历史重放

```
                              ┌─────────────────────────────────────┐
                              │        KVStateMachine               │
                              │  每次变更 → on_state_change() 回调  │
                              └───────────────┬─────────────────────┘
                                              │
                                  ┌───────────▼───────────┐
                                  │   写入历史缓冲区      │
                                  │  (10万条环形缓冲)     │
                                  └───────────┬───────────┘
                                              │
                     ┌────────────────────────┼────────────────────────┐
                     ▼                        ▼                        ▼
              ┌────────────┐          ┌────────────┐           ┌────────────┐
              │ Watcher 1  │          │ Watcher 2  │           │ Watcher N  │
              │ (精确匹配)  │          │ (前缀匹配) │           │ (范围匹配) │
              └─────┬──────┘          └─────┬──────┘           └─────┬──────┘
                    │                       │                        │
                    ▼                       ▼                        ▼
            独立事件队列             独立事件队列              独立事件队列
            (maxlen=10000)          (maxlen=10000)           (maxlen=10000)
```

**事件不丢失的保证**：

| 场景 | 处理方式 |
|------|----------|
| 实时事件 | 状态机回调 → 推送到所有匹配 Watcher 的队列 |
| Watcher 慢消费 | 队列满时覆盖旧的，但历史缓冲区仍保留完整历史 |
| 断线重连 | 指定 `start_revision` → 从历史缓冲区重放 ≥ start_revision 的所有事件 |
| 创建 Watch 时 | 先重放历史事件，再注册实时推送 → 不丢中间事件 |

**匹配器支持**：精确键 / 前缀 / 范围 / 通配符

---

### 3.5 分布式锁如何用带租约的键实现 — 崩溃自动释放

**核心代码位置**：[LockService.try_acquire](file:///d:/trae-bz/TraeProjects/28/lock/__init__.py#L261-L337) 和 [LockService._apply_acquire](file:///d:/trae-bz/TraeProjects/28/lock/__init__.py#L154-L212)

**锁的数据模型**：锁 = 带租约的 KV 键

```
锁键: "/locks/my_resource"
值:   {holder_session: "abc123", lease_id: 42}
TTL:  10秒 (通过 lease_id 绑定)
```

**获取锁的原子性**：通过 Raft 日志应用的顺序性保证

```
时间线:
  T1: Client A → submit(LOCK_ACQUIRE, key="/locks/X")
  T2: Client B → submit(LOCK_ACQUIRE, key="/locks/X")

Raft 提交顺序 (假设):
  log[100] = {A 的 ACQUIRE}  ← 先提交
  log[101] = {B 的 ACQUIRE}  ← 后提交

所有节点 apply:
  apply(log[100]): "/locks/X" 不存在 → 创建成功 ✓ (A 获得锁)
  apply(log[101]): "/locks/X" 已存在 → 失败 ✗ (B 没拿到)
```

**崩溃自动释放机制**：

```
正常流程:
  Client A: 持有锁 → 每 3s 发 KeepAlive → 租约永不过期 → 锁一直持有

崩溃流程:
  Client A: 崩溃 ← 心跳停止
                │
                ▼  10 秒后
        Leader 检测到 lease.expire_time < now
                │
                ▼
        submit(LEASE_REVOKE, lease_id=X)
                │
                ▼  Raft 提交
        所有节点删除 "/locks/X" + 其所有绑定键
                │
                ▼
        Client B 现在可以成功获取该锁
```

**锁与会话的绑定**：
- 会话过期 → 撤销其所有租约 → 释放其所有锁
- 避免客户端崩溃后遗留多个未清理的租约

---

### 3.6 Leader 处理写 / Follower 转发机制

**核心代码位置**：[Node._submit_write](file:///d:/trae-bz/TraeProjects/28/node/__init__.py#L164-L190) 和 [DistributedClient._call_leader](file:///d:/trae-bz/TraeProjects/28/client/__init__.py#L285-L314)

```
                客户端请求
                     │
         ┌───────────┴────────────┐
         ▼                        ▼
   ┌─────────┐  NOT_LEADER   ┌─────────┐
   │ Follower│ ────────────► │ Leader  │
   │  节点A   │   (重定向)    │  节点B   │
   └────┬────┘               └────┬────┘
        │                         │
        │ 内部直接转发             │ submit_write() → Raft
        ▼                         ▼
   调用 Leader 的方法         处理写操作 → Quorum → 应用 → 返回

自动重定向过程:
  [1] Follower 返回 {code=NOT_LEADER, leader_id="nodeB"}
  [2] 客户端/节点自动将请求发送到 nodeB
  [3] 如果 Leader 正在切换,重试最多 5 次
```

**读操作优化**：读操作可以在任意节点直接执行（不需要 Raft）
- 缺点：可能读到稍旧的数据（Follower 日志落后一点）
- 如需线性一致读：必须走 Leader，且 Leader 需确认自己仍是 Leader

---

### 3.7 网络分区时如何避免脑裂 — 绝不发出两把锁

**核心机制**：Raft + 租约 的双重防护

```
场景: 网络分区 (3 节点):
  ┌──────────┐        │        ┌──────────┐
  │ Leader A │        │        │ Follower B│
  │ Follower C│        │        └──────────┘
  └──────────┘        │         (分区 B)
   (分区 A: 2节点)     │
```

**分区 B (旧 Leader) 的视角**：
- 收不到 Follower C 的心跳响应（`match_index` 不推进）
- 但自己也收不到多数节点心跳 → 选举超时
- 变为 Candidate,发起选举,但分区 B 只有 1 节点 < Quorum(2)
- **无法成为 Leader，无法提交任何写操作**

**分区 A 的视角**：
- 2 节点 = Quorum(2) ✓
- 选举出新 Leader（如果 A 在分区 A）
- 新 Leader 可以提交写操作

**结论**：任何时刻最多只有一个分区能提交写操作

**具体到"两把锁"问题**：
```
假设分区发生在 Client A 刚获取锁之后:
  分区 A (多数): 知道 Client A 持有锁 → 新请求会被拒绝
  分区 B (少数): Client B 请求获取锁 → 无法提交 Raft → 超时失败
  → 任何时刻都只有 0 或 1 个客户端"成功持有"锁

如果 Client A 在分区 B 且崩溃:
  分区 A 经过 TTL 秒后 → 检测到租约过期 → 提交 LEASE_REVOKE → 锁释放
  → Client B 最终可以在分区 A 成功获取锁
  → 不会死锁
```

---

### 3.8 客户端会话如何与租约绑定

**核心代码位置**：[LeaseManager.session_heartbeat](file:///d:/trae-bz/TraeProjects/28/lease/__init__.py#L445-L466) 和 [LeaseManager.bind_lock_to_session](file:///d:/trae-bz/TraeProjects/28/lease/__init__.py#L472-L479)

**会话模型**：

```
┌──────────────────────────────────────────────────┐
│  Session (session_id = "abc123")                 │
│                                                  │
│  ├── lease_ids = {L1, L2, L3}     (绑定的租约)    │
│  ├── lock_keys = {"/locks/X", "/locks/Y"}        │
│  ├── last_heartbeat = 1718...                    │
│  └── timeout = 10s                               │
│                                                  │
│  L1: TTL=10s → 绑定 "/locks/X" + 临时键         │
│  L2: TTL=30s → 绑定 "/locks/Y"                   │
│  L3: TTL=60s → 绑定会话级临时数据                │
└──────────────────────────────────────────────────┘
              │
              │  会话心跳 (每 3s)
              ▼
    keepalive_lease(L1), keepalive_lease(L2), keepalive_lease(L3)
              │
              ▼  心跳停止超过 10s
    session.is_alive() = False
              │
              ▼
    遍历 lease_ids → revoke(L1), revoke(L2), revoke(L3)
              │
              ▼
    所有锁和临时键被自动清理
```

**绑定关系的作用**：

1. **简化心跳**：客户端只需要发送 `session_heartbeat()`，不需要单独给每个租约发心跳
2. **原子清理**：会话过期 → 一次性释放所有锁和临时键，不留垃圾
3. **崩溃安全**：客户端崩溃 = 会话心跳停止 = 所有资源自动释放
4. **多租约支持**：一个会话可以有多个不同 TTL 的租约，对应不同用途的资源

---

## 四、接口 API 总结

### KV 操作

| API | 语义 | 是否需要 Leader |
|-----|------|----------------|
| `kv_put(key, value, lease_id=0)` | 写入键值对 | 是 |
| `kv_delete(key)` | 删除键 | 是 |
| `kv_get(key)` | 读取单个键 | 否 (可在任意节点) |
| `kv_get_prefix(prefix)` | 前缀查询 | 否 |
| `kv_get_range(start, end)` | 范围查询 | 否 |

### 租约操作

| API | 语义 | 是否需要 Leader |
|-----|------|----------------|
| `lease_grant(ttl, session_id)` | 创建租约 | 是 |
| `lease_keepalive(lease_id)` | 续期租约 | 是 (Leader 本地+批量同步) |
| `lease_revoke(lease_id)` | 撤销租约 | 是 |
| `lease_ttl(lease_id)` | 查询剩余 TTL | 否 |

### 锁操作

| API | 语义 | 是否需要 Leader |
|-----|------|----------------|
| `lock_try_acquire(name, session, ttl)` | 非阻塞获取锁 | 是 |
| `lock_acquire(name, session, ttl, timeout)` | 阻塞获取锁 | 是 |
| `lock_release(name, session)` | 释放锁 | 是 |
| `lock_refresh(name, session)` | 续期锁 | 是 |
| `lock_info(name)` | 查询锁状态 | 否 |

### Watch 操作

| API | 语义 | 是否需要 Leader |
|-----|------|----------------|
| `watch_create(prefix=..., start_revision=0)` | 创建订阅 | 否 |
| `watch_fetch(watch_id, timeout)` | 拉取事件 | 否 |
| `watch_cancel(watch_id)` | 取消订阅 | 否 |

---

## 五、运行示例

```bash
cd d:\trae-bz\TraeProjects\28
python -m examples.demo
```

示例包含 7 个演示：
1. 集群启动与 Leader 选举
2. KV 存储 + Follower 写转发
3. Watch 实时通知
4. 分布式锁互斥验证（多线程计数器）
5. 租约过期自动清理（模拟崩溃）
6. Follower 写请求自动转发到 Leader
7. 所有节点状态一致性验证

---

## 六、与 etcd/ZooKeeper 对比

| 特性 | 本实现 | etcd | ZooKeeper |
|------|--------|------|-----------|
| 一致性协议 | 简化 Raft | Raft | ZAB |
| 数据模型 | KV + revision | KV + revision | ZNode 树 |
| 临时节点 | 租约绑定键 | 租约绑定键 | Ephemeral ZNode |
| Watch | 多匹配器 + 历史重放 | 事件通知 + 历史 | 一次性 Watch + 重连 |
| 分布式锁 | 租约键 + Raft | 相同原理 | ZNode 序号 + Watch |
| 会话管理 | 租约绑定 | 相同原理 | 会话级 Ephemeral |
| 线性一致读 | Leader 读 | 支持 | 支持 |
| 序列化写 | Raft 日志顺序 | 相同 | ZAB 顺序 |

本实现是这些系统核心机制的教学简化版，去掉了持久化、快照、成员变更、性能优化等生产级功能，但保留了最核心的算法思想。
