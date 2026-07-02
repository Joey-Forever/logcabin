# C++ Multi-Raft 强一致分布式内存 KV 数据库设计总结

## 1. 项目定位

本项目定位为：

> **C++ Multi-Raft 强一致分布式内存 NoSQL 数据库**

它不是 TiKV 这类磁盘型分布式事务 KV，也不是 Redis Cluster 这类偏缓存/弱一致的系统，而是一个面向强一致、低延迟、内存状态机的分布式有序 KV 数据库原型。

核心能力包括：

```text
内存状态机
+ Multi-Raft 分片复制
+ 连续 key range 分片
+ 全局 TSO
+ MVCC 一致 Scan
+ Lease Read 快速点查
+ LSM-like Snapshot Store
+ Raft log compaction
+ 崩溃恢复
```

可以将其理解为：

```text
Redis-like 内存读写性能
+ etcd-like 强一致语义
+ TiKV-like Multi-Raft 分片思想
+ 自研 MVCC / Snapshot / Recovery 机制
```

---

## 2. 总体架构

系统按连续 key range 分片，每个 range 对应一个 Raft Group。

```text
Client / Coordinator
  |
  |-- key/range 路由
  |-- NotLeader 重试
  |-- StaleRange 处理
  |-- 跨 Group Scan 拆分与结果合并
  |
MultiRaftNode
  |
  |-- RaftGroup 1: ["", "g")
  |-- RaftGroup 2: ["g", "n")
  |-- RaftGroup 3: ["n", "")
  |
每个 RaftGroup:
  |-- Raft Consensus
  |-- Raft WAL / HardState
  |-- MVCC Ordered Memory KV StateMachine
  |-- LSM-like Snapshot Store
  |-- Apply Worker
  |-- Lease Read / ReadIndex
```

每个 Raft Group 独立维护：

```text
Raft log
currentTerm / votedFor / HardState
commitIndex
appliedIndex
leader state
snapshot
MVCC 内存状态机
```

这使得系统不是单 Raft KV，而是标准的 **Multi-Raft 分片架构**。

---

## 3. 数据分片设计：连续 Range

系统使用连续 key range 分片，而不是 hash 分片。

示例：

```text
Group 1: ["", "g")
Group 2: ["g", "n")
Group 3: ["n", "")
```

选择连续 range 的原因：

```text
1. 支持有序 KV
2. 支持 Range Scan / Prefix Scan
3. 方便后续做 range split / merge
4. 和跨 Group Scan 的范围拆分天然匹配
```

Hash 分片虽然能让点查负载更均匀，但会破坏 key 的自然顺序，不适合有序 KV 和一致 Range Scan 的目标。

---

## 4. 写入路径：TSO + Raft Entry

写入在 append Raft log 之前先获取全局 TSO timestamp，并将该 timestamp 写入 Raft log entry。

写入流程：

```text
Client Put(k, v)
  -> 路由到 key 所属 Group Leader
  -> Leader 向 TSO 获取 tso_ts
  -> 构造 Raft entry: {PUT, key, value, tso_ts}
  -> append 到本 Group Raft log
  -> 复制到 majority
  -> commitIndex 推进
  -> apply 到 MVCC 内存状态机
  -> 返回成功
```

每条写入都有全局版本号：

```text
entry.tso_ts
```

这个设计的好处：

```text
1. 全局版本号被 Raft 复制，leader crash 后不会丢
2. follower replay log 时版本稳定
3. snapshot / recovery 语义稳定
4. 跨 group scan 可以用同一个 read_ts 做一致快照
5. scan 可以判断 pending log entry 是否早于 read_ts
```

TSO 可以由一个特殊 Meta Raft Group 提供：

```text
MetaGroup:
  global_ts += 1
  return global_ts
```

后续可优化为批量分配：

```text
Leader 从 TSO 申请 [1000, 2000)
本地给多条写入分配 ts
```

这样可以减少每条写入都远程访问 TSO 的开销。

---

## 5. 点查读路径：Lease Read

点查只要求读最新值，不需要完整 scan 快照语义。

理想路径：

```text
Client Get(k)
  -> 路由到 Group Leader
  -> Leader lease 有效
  -> 直接读内存 MVCC 最新版本
  -> 返回
```

性能上接近：

```text
一次 RPC + 一次内存查找
```

Lease Read 的安全要求：

```text
1. lease 必须基于 quorum heartbeat/ack
2. 使用 monotonic clock
3. lease duration 小于 election timeout
4. leader 刚当选后提交 no-op 或确认 commit 状态
5. 读前确保 appliedIndex 不落后
```

这是项目的低延迟读路径亮点。

---

## 6. MVCC：支持一致 Range Scan

状态机不是简单的：

```text
key -> value
```

而是：

```text
key -> [(tso_ts, value/delete), ...]
```

示例：

```text
user:1:
  ts=100 -> Alice
  ts=130 -> Bob

user:2:
  ts=120 -> Tom
  ts=150 -> DELETE
```

点查最新值：

```text
读取版本链最后一个 visible version
```

Range Scan 读取某个 read_ts 的快照：

```text
Scan(read_ts=140):
  user:1 -> Bob
  user:2 -> Tom
```

`DELETE` 作为 tombstone 参与 MVCC 和 snapshot compaction。

MVCC GC 需要维护活跃 scan 的 read_ts：

```text
active_read_ts
safe_gc_ts = min(active_read_ts)
```

GC 时不能删除仍可能被 scan 读取的旧版本。

---

## 7. 跨 Group 一致 Range Scan

跨 Group Scan 是项目的重要数据库语义亮点。

Scan 流程：

```text
1. Coordinator 向 TSO 获取 read_ts
2. 根据 range metadata 把 scan range 拆到多个 group
3. 对每个 group leader 发送 Scan(part_range, read_ts)
4. group leader 检查本地 log 中是否存在 tso_ts <= read_ts 但尚未 apply 的 entry
5. 如果存在，等待这些 entry commit/apply，或者确认它们被 Raft 覆盖/不会提交
6. group 在 MVCC 状态机中读取 <= read_ts 的最新版本
7. Coordinator 合并结果返回
```

语义：

```text
所有 tso_ts <= read_ts 且最终成功提交的写入，scan 必须看到；
所有 tso_ts > read_ts 的写入，scan 不能看到。
```

这使得跨多个 Raft Group 的 scan 仍然是一个一致视图。

系统不支持跨 region 原子写事务，因此不需要 TiKV/Percolator 的：

```text
2PC
primary lock
secondary lock
lock resolve
rollback record
跨 key 事务冲突检测
```

能力边界是：

```text
支持：单 key 写入 + 全局版本化 + 跨 group 一致 scan
不支持：跨 group 多 key 原子事务
```

---

## 8. NotLeader / StaleRange 处理

如果某个 group leader 在执行 scan 时失去 leader 身份：

```text
返回 NotLeader
```

Coordinator 可以保留同一个 `read_ts`，只重试失败的 group/range part：

```text
read_ts 固定
已成功的 part 保留
失败的 part 找新 leader 后重试
```

如果发生 range split / merge：

```text
返回 StaleRange
Coordinator 重新查 metadata
用同一个 read_ts 重新拆分该 part
```

因此 scan 不必整体重来。只要所有 part 最终都按同一个 read_ts 读取，整个 scan 仍然是一致视图。

---

## 9. Snapshot 设计：LSM-like Snapshot Store

最终 snapshot 设计采用 **LSM-like Snapshot Store**，不是简单 full snapshot。

核心层次：

```text
Raft Log:
  最细粒度复制日志，用于一致性和增量恢复

Active Delta Log:
  snapshot 层的 append-only 增量文件

Compacted Delta SST:
  对 active delta log 做排序、去重、tombstone 合并后的 immutable 文件

Segmented Base Snapshot:
  按 key range 切分的完整 checkpoint segment 文件
```

文件结构示例：

```text
snapshot/
  MANIFEST

  base/
    seg_000_10000.sst
    seg_001_10000.sst
    seg_002_13000.sst

  delta/
    delta_active.log
    delta_10000_12000.sst
    delta_12000_13000.sst
```

这个设计借鉴了 LSM Tree 的核心思想：

```text
顺序追加
immutable file
后台 compaction
新版本覆盖旧版本
tombstone 清理
```

但它不承担在线查询压力，主要用于：

```text
节点重启恢复
InstallSnapshot
Raft log compaction
恢复耗时控制
```

---

## 10. Snapshot 生成流程

### 10.1 增量写入 Active Delta Log

每次增量 snapshot，不立即生成 SST，而是把 dirty kv 的最新状态追加到 active delta log：

```text
delta_active.log:
  index=11000, a -> v2
  index=11000, b -> DELETE
  index=12000, a -> v3
  index=12000, c -> v1
```

这是 append-only，前台成本低。

每条 record 需要包含：

```text
record_len
index / snapshot_index
op_type
key
value
crc
```

崩溃恢复时顺序读，遇到半条或 CRC 错误就 truncate 到上一条有效 record。

---

### 10.2 Active Delta Log Compact 成 Delta SST

当 active delta log 达到大小或 record 数阈值：

```text
delta_active.log >= 64MB
```

后台将其 compact 为有序、去重、immutable 的 delta SST：

```text
delta_10000_12000.sst:
  a -> v3
  b -> DELETE
  c -> v1
```

同一个 key 多次出现，只保留最新状态。

DELETE tombstone 必须保留，直到它被合并进 base segment 后才能清掉。

---

### 10.3 Delta SST 及时回灌 Base Snapshot

系统采用 **eager compaction**：

```text
delta SST 一生成，就进入后台 compaction queue
```

后台线程尽快把它合入 segmented base snapshot，而不是长期堆积。

流程：

```text
1. 解析 delta SST 涉及哪些 segment
2. 按 segment 拆分成 compaction task
3. old base segment + delta fragment -> new segment tmp
4. fsync new segment
5. rename 成正式 segment
6. 更新 MANIFEST
7. 删除或标记 obsolete delta fragment / delta SST
```

只重写 dirty segments，不重写整个 base snapshot。

示例：

```text
base snapshot:
  seg_000: ["", "g")
  seg_001: ["g", "n")
  seg_002: ["n", "")

delta 只影响 seg_001

那么只生成：
  seg_001_new.sst

seg_000 / seg_002 继续复用旧文件
```

这就是 **Segmented Copy-on-Write Snapshot**。

---

## 11. Snapshot Compaction 策略

Delta SST 生成后及时回 base 的理由：

```text
1. 利用访问局部性
2. 单个 delta SST 通常只影响少量 segment
3. 避免 delta SST 堆积后一次性涉及大量 segment
4. 平滑 IO，避免集中 compaction 尖刺
5. 保持恢复路径短
```

但后台 compaction 需要控制：

```text
1. 后台线程执行
2. rate limit
3. 每轮只处理少量 dirty segments
4. 同一 segment 可做短窗口 debounce，避免热点 segment 频繁重写
5. WAL fsync 延迟升高时暂停或降速 compaction
```

由于系统是内存数据库，在线读写主要走内存，磁盘关键路径主要是 Raft WAL fsync。  
因此 snapshot compaction 的长期小 IO 是可接受的，但必须保证 Raft WAL 优先级高。

优先级可以定义为：

```text
最高优先级：
  Raft WAL append/fsync
  Raft replication
  leader heartbeat
  apply committed log

中等优先级：
  active delta log append
  snapshot manifest 原子更新

低优先级：
  delta SST -> base segment compaction
  obsolete file cleanup
```

Delta 积压主要是 **recovery debt**，不是在线读写瓶颈。

---

## 12. Snapshot 恢复流程

节点重启恢复：

```text
1. 读取 MANIFEST
2. 加载 base segments，重建内存 MVCC 状态
3. apply sealed delta SST
4. apply active delta log
5. replay snapshot_index 之后的 Raft log
6. 恢复到最新 applied state
```

如果 delta 回 base 不及时，主要影响：

```text
1. 单节点恢复耗时
2. 落后副本 install snapshot 成本
3. 磁盘空间
```

在 Raft 多副本架构下，单节点恢复慢不会直接导致集群不可用。只要该 Raft Group 仍有多数派存活，集群可以继续对外服务。

但恢复越慢，系统少一个副本运行的降级窗口越长，因此后台 compaction 的目标是控制 recovery debt，而不是优化在线读写路径。

---

## 13. Snapshot 崩溃安全

核心原则：

```text
所有 base segment / delta SST 都 immutable
不原地修改旧文件
通过 MANIFEST 原子切换当前 snapshot 视图
```

更新流程：

```text
1. 写 new_segment.tmp
2. fsync new_segment.tmp
3. rename -> new_segment.sst
4. 写 MANIFEST.tmp
5. fsync MANIFEST.tmp
6. rename MANIFEST.tmp -> MANIFEST
7. fsync snapshot directory
8. 旧文件延迟删除
9. snapshot 安全后再推进 Raft log GC
```

恢复时只信任 MANIFEST，不扫描目录猜文件状态。

这样 crash 后要么看到旧 manifest，要么看到新 manifest，不会出现半新半旧 snapshot。

---

## 14. Raft Log Compaction

只有当 snapshot store 确认能恢复到某个 `snapshot_index` 后，才能删除对应 Raft log：

```text
snapshot 持久化成功
  -> MANIFEST 原子切换成功
  -> 才能 GC raft log <= snapshot_index
```

不能先删 log 再写 snapshot。

这保证了节点崩溃后一定可以通过：

```text
snapshot + remaining raft log
```

恢复状态机。

---

## 15. 核心技术亮点

### 15.1 Multi-Raft 架构

一个节点管理多个 Raft Group，每个 group 负责一个连续 key range。  
相比单 Raft KV demo，复杂度和含金量明显更高。

### 15.2 强一致内存数据库定位

在线状态机是内存 MVCC 有序 KV，读写路径轻。  
它不是磁盘型 TiKV，也不是普通缓存。

### 15.3 全局 TSO + Raft Entry 携带 ts

写入 append log 前拿 TSO，并把 ts 复制进 Raft log。  
这让全局版本、恢复、scan 语义都更清楚。

### 15.4 Lease Read 快速点查

点查读最新值，leader lease 有效时直接读内存，不走 quorum read。

### 15.5 跨 Group 一致 Range Scan

scan 拿全局 read_ts，所有 group 基于 MVCC 返回同一快照视图。  
不支持跨 region 写事务，但支持跨 region 一致 scan，边界非常清晰。

### 15.6 MVCC 版本链和 GC

每个 key 保存多版本，scan pin read_ts，后台 GC 清理旧版本。  
这是数据库内核味很强的设计。

### 15.7 LSM-like Snapshot Store

Active delta log、delta SST、segmented base snapshot、后台 compaction。  
把 LSM 的 append-only、immutable file、compaction 思想用到了 Raft snapshot 场景。

### 15.8 Segmented Copy-on-Write Snapshot

delta 回 base 时只重写 dirty segments，而不是全量重写 snapshot。  
生成成本接近 dirty segment，恢复成本接近 full snapshot。

### 15.9 Eager Background Compaction

delta SST 生成后尽快回 base，利用访问局部性，避免 compaction debt 堆积和集中 IO 尖刺。

### 15.10 Crash Safety + Recovery Debt 控制

MANIFEST 原子切换、旧文件延迟删除、Raft log GC 边界、恢复耗时控制。  
这是非常真实的存储工程问题。

---

## 16. 完整落地后的含金量评估

如果最终代码完整实现以下能力：

```text
1. Multi-Raft 架构
2. 连续 key range 分片
3. Raft WAL / HardState / commitIndex / appliedIndex
4. 内存 MVCC 有序 KV 状态机
5. TSO timestamp 分配
6. 写入 entry 携带 tso_ts
7. Lease Read 点查
8. 跨 Group 一致 Range Scan
9. NotLeader / StaleRange retry
10. LSM-like Snapshot Store
11. Active Delta Log
12. Delta SST compaction
13. Segmented Base Snapshot
14. MANIFEST 原子切换
15. Raft log compaction
16. 节点重启恢复
17. leader failover
18. snapshot 过程中崩溃恢复
19. 基础 benchmark 与故障注入测试
```

那么这个项目的含金量可以评为：

> **很高，足以作为核心作品集项目。**

原因是它覆盖了分布式数据库/存储系统中非常核心的一组问题：

```text
一致性复制：Raft / Multi-Raft / majority commit
分片扩展：连续 range sharding
读路径优化：Lease Read
快照语义：TSO + MVCC + 跨 group scan
持久化恢复：Raft WAL + snapshot + log replay
工程存储：LSM-like snapshot + compaction + manifest
崩溃安全：fsync / rename / manifest 原子切换
可用性：leader failover / NotLeader retry / 节点恢复
```

相比普通后端项目，它明显更偏底层基础设施；相比普通 Raft toy KV，它多了 Multi-Raft、TSO、MVCC、跨 group scan 和工程化 snapshot；相比直接“读过 RocksDB/TiKV 源码”，它更能证明你能把分布式系统和存储设计落成可运行系统。

这个项目非常适合投递方向：

```text
C++ 后端
分布式系统
数据库内核
KV 存储
云计算基础设施
中间件
存储系统
控制面元数据系统
```

如果配套测试和文档做得扎实，它可以支撑面试中深入讨论：

```text
为什么 range 不用 hash？
Lease Read 如何保证安全？
跨 group scan 如何保证一致视图？
写入为什么先拿 TSO 再写 Raft log？
如果 leader 在 scan 等待期间 step down 怎么处理？
snapshot manifest 如何保证 crash safety？
为什么 delta 积压主要是 recovery debt？
Raft log GC 和 snapshot 持久化的顺序是什么？
```

最终评价：

> **这是一个小型但设计完整的强一致分布式内存数据库内核。完整落地后，含金量很高，明显强于绝大多数普通后端作品集，也比单 Raft KV demo 更有区分度。**

---

## 17. 简历描述参考

```text
C++ Multi-Raft 强一致分布式内存 KV 数据库

- 实现 Multi-Raft 架构，一个节点内管理多个 Raft Group，每个 Group 负责连续 key range，并独立维护 Raft log、HardState、Snapshot 与 apply 状态；
- 使用内存有序 MVCC KV 作为状态机，写入在 append Raft log 前获取全局 TSO timestamp，并随 log entry 复制到多数派，commit/apply 后成为可见版本；
- 支持 leader lease read，点查在 lease 有效期内直接读取内存最新版本，避免每次读走 quorum；
- 支持跨 Raft Group 一致 Range Scan：scan 获取全局 read_ts，各 Group 等待本地 tso_ts <= read_ts 的 pending log entry apply 后，读取 MVCC 中不超过 read_ts 的版本；
- 设计 LSM-like Snapshot Store：增量先追加到 active delta log，阈值触发 compact 成去重有序 delta SST，再由后台 eager compaction 合并进 segmented base snapshot；
- 实现 segmented copy-on-write snapshot，仅重写 dirty segments，并通过 MANIFEST 原子切换保证崩溃安全；
- 支持 snapshot 成功持久化后的 Raft log compaction，并通过故障注入测试覆盖 leader crash、NotLeader retry、节点重启、snapshot 过程中崩溃等场景。
```

---

## 18. 跨 Raft Group 原子写入与 TiKV GC 处理参考

单条 SQL / 原子命令如果要同时写多个 Raft Group，不应该让一个 Raft 共识实例覆盖多个 Group，而是在每个 Group 内部继续使用独立 Raft，跨 Group 的原子性由事务层负责。

例如一条原子命令：

```text
txn_id / start_ts = 100
primary key = 10

groupA 写入:
  10 -> "我是爸爸"

groupB 写入:
  11 -> "我是爷爷"
```

如果原始状态机只是：

```cpp
std::map<int, std::string> data;
```

则 prewrite 阶段不能直接写入 `data`，否则 groupA 成功、groupB 失败时，新值已经对外可见，跨 Group 原子性会被破坏。

状态机需要扩展为 MVCC / 事务状态：

```cpp
struct Lock {
    uint64_t txn_id;
    int primary_key;
    enum Type { Put, Delete, Lock } type;
};

struct WriteRecord {
    uint64_t start_ts;
    uint64_t commit_ts;
    enum Type { Put, Delete, Rollback } type;
};

struct DataState {
    std::map<int, std::string> committed;
    std::map<int, Lock> locks;
    std::map<std::pair<int, uint64_t>, std::string> pending;
    std::set<std::pair<int, uint64_t>> rollback_records;

    // GC safe point。GC 负责清理 safe point 之前的 rollback record / 历史版本。
    uint64_t gc_safe_point = 0;
};
```

### 18.1 2PC 写入流程

prewrite groupA：

```cpp
locks[10] = Lock{
    .txn_id = 100,
    .primary_key = 10,
    .type = Lock::Put,
};

pending[{10, 100}] = "我是爸爸";
```

prewrite groupB：

```cpp
locks[11] = Lock{
    .txn_id = 100,
    .primary_key = 10,
    .type = Lock::Put,
};

pending[{11, 100}] = "我是爷爷";
```

prewrite 成功后，新值还不能进入 `committed`，因此外部读不到新值，只能看到锁。

所有 Group 的 prewrite 都确认成功之后，协调者才能进入 commit 阶段：

```text
1. commit primary key 10
2. commit secondary key 11
```

commit groupA primary：

```cpp
committed[10] = pending[{10, 100}];
pending.erase({10, 100});
locks.erase(10);
```

commit groupB secondary：

```cpp
committed[11] = pending[{11, 100}];
pending.erase({11, 100});
locks.erase(11);
```

如果 groupB prewrite 超时，协调者不能把它当成明确失败，因为请求可能已经在 groupB 成功 apply，只是响应丢失。正确做法是重试同一个 prewrite。prewrite 必须幂等：

```text
same txn_id + same key + same mutation
  -> 如果已经写入 lock/pending，则返回成功
  -> 如果已经 rollback，则返回 AlreadyRollback / TxnTooOld
```

如果最终决定放弃事务，需要对所有相关 Group 发送 rollback。rollback 也必须幂等：

```cpp
if (locks.contains(key) && locks[key].txn_id == txn_id) {
    locks.erase(key);
    pending.erase({key, txn_id});
}

rollback_records.insert({key, txn_id});
```

rollback marker 不能马上删除。它至少要保留到系统的 GC safe point 超过该事务的 `start_ts`。prewrite 路径需要优先检查同 `start_ts` 的 rollback record，防止 cleanup / rollback 之后迟到的 prewrite 重新成功：

```cpp
Status Prewrite(int key, std::string value, uint64_t start_ts, int primary_key) {
    if (rollback_records.contains({key, start_ts})) {
        return AlreadyRollback;
    }

    if (locks.contains(key)) {
        if (locks[key].txn_id == start_ts) {
            return OK; // 幂等重试
        }
        return Locked;
    }

    locks[key] = Lock{
        .txn_id = start_ts,
        .primary_key = primary_key,
        .type = Lock::Put,
    };
    pending[{key, start_ts}] = value;
    return OK;
}
```

### 18.2 primary 状态与 secondary lock 恢复

如果 primary 已经 commit，但 secondary commit 请求丢失，secondary Group 可能长期残留：

```text
locks[11] = Lock{txn_id=100, primary_key=10}
pending[{11, 100}] = "我是爷爷"
```

后续访问 `11` 或后台 GC 扫到这个锁时，需要查 primary 状态：

```text
CheckTxnStatus(primary=10, start_ts=100)
```

如果 primary 已提交，则补提交 secondary：

```cpp
committed[11] = pending[{11, 100}];
pending.erase({11, 100});
locks.erase(11);
```

如果 primary 已回滚，则清理 secondary：

```cpp
pending.erase({11, 100});
locks.erase(11);
rollback_records.insert({11, 100});
```

因此 primary key 的状态是事务最终结局的锚点：

```text
primary committed -> secondary should commit
primary rollback   -> secondary should rollback
```

普通 2PC 下，primary commit record 不需要保存所有 secondary keys。secondary lock 自己保存 primary 指针：

```text
secondary lock:
  key = 11
  start_ts = 100
  primary = 10
```

所以恢复路径是从 secondary lock 反查 primary 状态，而不是从 primary commit record 主动找到所有 secondaries。

### 18.3 GC 的关键不变量

不能出现这种状态：

```text
primary 的 commit / rollback 证据已经被 GC 删除
secondary 还残留需要依赖 primary 判断结局的 lock
```

否则 secondary 访问者看到 pending lock 后，去 primary 查询却发现没有记录，无法区分：

```text
1. primary 从未提交，事务应 rollback
2. primary 曾经提交，但 commit 记录已经被 GC 删除
```

因此 GC 必须满足：

```text
删除 primary 事务状态之前，所有 start_ts <= safe_point 的残留 lock 必须已经被 resolve。
```

贴近 TiKV 的流程：

```text
1. meta raft leader 选择 target_safe_point
2. GC worker 扫描所有 data group 的 lock.ts <= target_safe_point
3. 对扫到的 old lock 查 primary，并 commit / rollback secondary
4. old locks resolve 完成后，推进 gc_safe_point
5. data group 根据 gc_safe_point 物理删除旧版本和过老 rollback marker
```

下面这个时序在标准 2PC 里本身不成立：

```text
1. groupB 的旧 prewrite(start_ts=100) 在网络里卡住
2. GC 扫 groupB，没有扫到这个 lock
3. GC 推进 gc point = 200
4. groupA 删除 primary commit 证据
5. groupB 还没收到 gc point
6. 迟到 prewrite 到达 groupB
7. 如果 groupB 只看本地 gc point，就可能错误接受旧 prewrite
```

原因是：如果 groupB 是这个事务的 secondary，且 groupB 的 prewrite 从未确认成功，则协调者不能 commit groupA primary。TiKV 靠标准 2PC 的提交前置条件消除该场景。

### 18.4 meta raft group 的职责边界

可以用一个 meta raft group 管理全局元信息：

```cpp
struct MetaState {
    uint64_t next_tso;
    uint64_t gc_safe_point;
    uint64_t gc_epoch;

    // shard / range 路由、节点 membership、全局配置等。
};
```

meta raft group 可以负责：

```text
1. 分配 TSO / start_ts / commit_ts
2. 维护 GC safe point
3. 记录 Region / Shard 路由
4. 选出或直接承载 GC worker
5. 维护全局配置与 membership
```

meta raft group 的 leader 可以直接作为 GC worker，但 GC 任务必须跑在后台线程中，不能阻塞 meta raft apply / heartbeat / TSO 请求。

GC RPC 建议携带 epoch 信息：

```text
meta_term
gc_epoch
target_safe_point
```

data group 只接受当前有效 epoch 的 GC 请求，避免旧 meta leader 失去 leadership 后继续执行过期 GC。

同时，meta raft group 不应该参与每条跨 Group 事务的提交决策，否则会变成全局事务瓶颈。它只提供时间戳和全局元信息；数据写入仍然由各 data raft group 通过 MVCC + 2PC 完成。

### 18.5 TiKV 的实际处理方式

TiKV / TiDB 的核心不是在每个 primary commit record 里记录所有 secondary keys，而是：

```text
1. 普通 2PC 中，TiDB 必须确认所有 secondary prewrite 成功后，才允许 commit primary。
2. secondary lock 里保存 primary key 指针。
3. 后续访问或 GC 扫到 secondary lock 时，通过 primary key 查询事务状态。
4. primary committed 则补 commit secondary。
5. primary rollback / expired 则 rollback secondary。
```

所以这种时序在正确 TiKV 2PC 协议里不成立：

```text
groupB 第一次 prewrite 还没成功
groupA primary 已经 commit
```

如果 groupB prewrite 超时，TiDB 会重试确认；确认不了不能 commit primary，只能走 cleanup / rollback。

TiKV 的 GC 也不是由 primary commit record 驱动去找 secondaries，而是由全局 GC safe point 驱动：

```text
1. TiDB 集群中选出的 GC owner / GC worker 计算 safe point。
2. GC worker 在 Do GC 之前执行 Resolve Locks。
3. Resolve Locks 逻辑上覆盖全局 keyspace，扫描 safe point 之前的 old locks。
4. 实际扫描下推到各 TiKV store / Region，本地扫描 Lock CF。
5. 扫到 old secondary lock 后，lock resolver 查 primary 状态并 resolve。
6. old locks 处理完成后，再进入物理 GC，删除旧 MVCC 版本。
```

因此 TiKV 避免了：

```text
primary commit 证据先被删
secondary lock 后发现但无法判断事务结局
```

GC 的网络和扫描压力客观存在，但它不在事务提交关键路径上，并且通过后台周期执行、批量扫描、只扫 Lock CF、分布式下推到 TiKV、限速和重试来控制成本。

对自研 Multi-Raft KV 来说，最保守且容易证明正确的设计是：

```text
1. commit primary 之前，必须确认所有 prewrite 成功。
2. prewrite / rollback / commit 都必须幂等。
3. rollback marker 防止 cleanup / rollback 之后迟到 prewrite 复活事务。
4. physical GC 前必须先 resolve old locks。
5. 删除 primary 状态前，必须保证不会再出现依赖该 primary 状态的 old secondary lock。
6. meta raft 负责 TSO、safe point、GC epoch 和路由；data raft group 负责 MVCC 状态机和事务 apply。
```
