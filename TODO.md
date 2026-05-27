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

MVCC GC 需要维护活跃长事务的最小 start_ts：

```text
coordinator 本地维护 active_txns
coordinator 周期性向 meta raft group 上报 min_active_start_ts
safe_gc_ts <= min(所有有效 coordinator lease 的 min_active_start_ts)
```

本项目不再区分纯读事务和写事务。只要用户执行 `BEGIN`，就认为该长事务后续可能读也可能写，
其 `start_ts` 在 coordinator lease 有效期间必须保护 GC safe point，防止事务后续 snapshot read
读不到历史版本。coordinator 断线或超过 max wait / max txn ttl 后，meta 可以丢弃它上报的
`min_active_start_ts`，GC safe point 可以继续推进；迟到事务只能返回 TxnExpired / SnapshotTooOld。

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

早期简化边界只包含单 key 写入和跨 group 一致 scan。
当前第 18 节已经把事务层升级为 TiDB 风格的 pessimistic 长事务，
通过 2PC、primary/secondary lock、lock resolve 和 rollback record 支持跨 Raft Group 原子写。

能力边界更新为：

```text
支持：单 key 写入 + 全局版本化 + 跨 group 一致 scan + 跨 group 多 key pessimistic 长事务
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
跨 region 一致 scan 通过全局 read_ts 完成；跨 region 原子写由第 18 节的 pessimistic 长事务和 2PC 负责。

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

## 18. 跨 Raft Group 悲观长事务与 TiKV GC 处理参考

如果一个用户事务要同时写多个 Raft Group，不应该让一个 Raft 共识实例覆盖多个 Group，
而是在每个 Group 内部继续使用独立 Raft，跨 Group 的原子性由事务层负责。

本项目事务模型采用 TiDB 风格的 pessimistic 长事务：

本节采用以下最小行为定义和能力边界：

```text
乐观事务：执行阶段不加悲观锁，冲突主要在 prewrite / commit 暴露；SELECT FOR UPDATE 仍读 start_ts 快照，只把结果 key 加入提交冲突检测。
悲观事务：写操作 / SELECT FOR UPDATE 使用新的 for_update_ts 做当前读并立即加锁，冲突主要在语句执行阶段暴露。

RR / SI：普通 SELECT 在整个事务中固定读取 start_ts 快照。
RC：普通 SELECT 每条语句读取新的 statement_read_ts 快照。

TiDB 乐观事务不支持 RC 的行为本质：乐观事务统一按 start_ts 做写冲突检测；如果只让 SELECT 改用新 ts，
事务可能先读到 start_ts 之后提交的版本，却在 prewrite 时又因为该版本晚于 start_ts 而冲突。
支持 optimistic RC 需要为不同 key / 语句维护独立的读取与冲突验证时间戳，而不只是给 SELECT 换一个 ts。

本项目第一版只支持悲观事务 + RR / SI：普通 SELECT 读 start_ts 快照，写操作 / SELECT FOR UPDATE
使用 for_update_ts 当前读并加锁；暂不支持乐观事务和 RC。

本项目和 TiDB 一样不显式支持 SERIALIZABLE。业务可以通过 SELECT FOR UPDATE 锁定共同的 guard key，
并在持锁后当前读相关状态，对特定临界区实现按需串行化；这不等于数据库提供通用 SERIALIZABLE 隔离级别。
```

典型的 `write skew` / `serialization anomaly`：

```text
初始 A、B 两名医生都值班，约束是至少一人值班。
T1、T2 同时读到 A、B 都在值班；T1 只写 A=下班，T2 只写 B=下班。
两者写不同 key，在 SI 下可能都提交，但结果不等价于任何串行顺序；SERIALIZABLE 必须中止其中一个事务。
```

```text
BEGIN:
  coordinator 向 meta raft group / TSO 获取 start_ts，并加入本地 active_txns。
  coordinator 周期性向 meta leader 上报本机 min_active_start_ts，类似 TiDB server 上报 /tidb/server/minstartts/<server_uuid>。

事务执行期间:
  snapshot read 使用 start_ts。
  写 SQL / SELECT FOR UPDATE 使用新的 for_update_ts 做 current read，并在 data group 状态机中写入 pessimistic lock。

COMMIT:
  coordinator 获取 commit_ts。
  对整个事务的 write set 执行一次 2PC：prewrite -> commit primary -> commit secondaries。

ROLLBACK:
  清理本事务已经写入的 pessimistic lock / prewrite lock / pending value，并写 rollback record。
```

它和 optimistic 长事务的核心区别是：

```text
optimistic 长事务:
  执行阶段写入通常先缓存在 coordinator 内存里，不提前占锁。
  COMMIT 时才 prewrite，冲突也主要在 COMMIT/prewrite 暴露。

pessimistic 长事务:
  每条写 SQL 执行时就对目标 key 做 current read，并在 data group 中写入 pessimistic lock。
  后续 COMMIT 时把已有 pessimistic lock 转成真正的 prewrite lock，再提交。
  因此热点写冲突更早暴露，后续事务会等待/重试锁，而不是都堆到 COMMIT 才失败。
```

例如一个跨 Group 长事务：

```text
BEGIN start_ts = 100

UPDATE key=10  // for_update_ts = 120, groupA 写入 pessimistic lock
UPDATE key=11  // for_update_ts = 140, groupB 写入 pessimistic lock

COMMIT commit_ts = 180
primary key = 10
```

如果原始状态机只是：

```cpp
std::map<int, std::string> data;
```

则事务执行阶段和 prewrite 阶段都不能直接写入 `data`，否则事务还没有 COMMIT，
新值已经对外可见，跨 Group 原子性会被破坏。

状态机需要扩展为 MVCC / 事务状态：

```cpp
struct Lock {
    uint64_t start_ts;       // BEGIN 时的事务 start_ts。
    int primary_key;         // 整个事务的 primary key，所有 secondary lock 都指向它。
    uint64_t ttl_ms;         // lock TTL。TiKV 的 Lock 都有 ttl；本项目只把 primary lock ttl 作为事务存活性的权威依据。
    uint64_t for_update_ts;  // 当前 key 被悲观锁锁住时的 current-read 时间点。

    enum Type {
        Put,          // prewrite 后的写锁。
        Delete,       // prewrite 后的删除锁。
        Lock,         // prewrite 后的纯锁。
        Pessimistic,  // 执行阶段的悲观锁，还不是最终 prewrite lock。
    } type;
};

struct Version {
    uint64_t start_ts;
    uint64_t commit_ts;
    enum Type { Put, Delete } type;
    std::optional<std::string> value;
};

struct DataState {
    // 已提交 MVCC 版本链。每个 key 的版本按 commit_ts 排序。
    std::map<int, std::vector<Version>> committed;

    // committed 版本链的 GC 候选索引。
    // 用于避免 Do GC 时全量遍历 committed map。
    //
    // 一个 key 只有在 committed[key].size() >= 2 时才需要进入该队列；
    // 队列按该 key 第二老版本的 commit_ts 排序。
    // 如果 second_oldest_commit_ts < gc_safe_point，说明该 key 至少有一个
    // safe point 前的旧版本可以删除。
    struct GcCandidate {
        int key;
        uint64_t second_oldest_commit_ts;
    };
    MinHeap<GcCandidate> committed_gc_heap;

    // 未提交锁，对应 TiKV Lock CF，包括 pessimistic lock 和 prewrite lock。
    std::map<int, Lock> locks;

    // 未提交 value，对应 TiKV Default CF: (key, start_ts) -> value。
    // 执行阶段可以先缓存在 coordinator；进入 prewrite 后必须落到 data group 状态机。
    std::map<std::pair<int, uint64_t>, std::string> pending;

    // 回滚标记独立保存，不混入 committed MVCC 链。
    std::set<std::pair<int, uint64_t>> rollback_records;

    // GC safe point。GC 负责清理 safe point 之前的 rollback record / 历史版本。
    uint64_t gc_safe_point = 0;
};
```

TiKV 的事务状态在 RocksDB 中主要拆到三个 Column Family。本项目是内存状态机，不需要照搬
RocksDB CF，但可以借用它的职责划分：

TiKV 对上层暴露的是一个全局有序 KV keyspace，可以近似理解成一个巨大的 `map<byte[], byte[]>`。
这个 map 不按 SQL 表、主键类型或业务语义排序，而是按 key 的原始字节做 unsigned lexicographic
order，也就是逐字节比较：

```text
[0x01, 0x02] < [0x01, 0x03]
[0x01]       < [0x01, 0x00]
"user:1"     < "user:2"
```

TiKV 本身不理解“表”“行”“索引”这些 SQL 概念。TiDB 在 SQL 层负责把关系模型编码成这个统一
keyspace 里的字节 key。简化后可以理解为：

如果先不考虑 MVCC、2PC、悲观锁、Raft 复制和 GC，只从作用在业务 KV 数据上的基础能力看，TiKV
主要提供四类有序 KV 操作：

```text
Get(key)                       // 点读
Scan(start_key, end_key)       // 按 byte key 顺序范围读
Put(key, value)                // 写入一个 KV
Delete(key)                    // 删除一个 KV
```

实际工程中还会有 batch get / batch put / delete range / raw kv atomic ops 等变体；事务 KV 写入也
不会直接裸 `Put/Delete`，而是经过 prewrite / commit / rollback、lock check 和 MVCC 可见性判断。
但数据模型抽象仍然是有序 KV，SQL 层的表、列、索引、类型和约束都由 TiDB 转成这些 KV 读写。

```text
row key:
  table_id + row_id -> encoded row value

unique index key:
  table_id + index_id + indexed_columns -> row_id

non-unique index key:
  table_id + index_id + indexed_columns + row_id -> empty / handle
```

例如一个普通 SQL 表：

```sql
CREATE TABLE user (
  id BIGINT PRIMARY KEY,
  name VARCHAR(32),
  age INT,
  city VARCHAR(32),
  UNIQUE KEY uk_name(name),
  KEY idx_city(city)
);
```

在 TiKV 看来并不存在 `user` 表这个对象。TiDB 会把主表行和索引分别编码成 KV：

```text
row key:
  table_id + row_id(id)

row value:
  encode(name, age, city)

example:
  t123_r1001 -> encoded_row{name="Alice", age=20, city="Shanghai"}

unique index key:
  table_id + index_id(uk_name) + name

unique index value:
  row_id

example:
  t123_i5_Alice -> 1001

non-unique index key:
  table_id + index_id(idx_city) + city + row_id

non-unique index value:
  empty / handle / extra info

example:
  t123_i6_Shanghai_1001 -> empty
```

也就是说，普通列通常被统一编码进主表 row value；主键列通常参与 row key / handle 编码；索引列
除了存在于 row value，也会额外出现在索引 key 中，用来支持按索引定位 row。TiKV 不知道这些 bytes
分别代表 `name`、`age` 还是 `city`，列类型、NULL、列 ID、索引维护和二级索引回表都由 TiDB 处理。

因此不同业务表在 TiKV 看来并不是不同的物理数据库或不同 map，而是同一个全局 KV map 中不同
`table_id` 前缀下的 key range。TiKV 只负责按 byte key 排序、切分 Region、复制和存储：

```text
global keyspace:
  ... table_41 rows ...
  ... table_41 index_1 ...
  ... table_42 rows ...
  ... table_42 index_1 ...
```

Region 也是按这个全局 keyspace 的连续 key range 切分的。一个 Region 本质上负责某段连续 byte key
范围，并由一个 Raft group 复制；TiDB 根据 key range 找到对应 Region leader，再把 KV 请求发过去。

MVCC 之后，TiKV 会在 user key 后面追加时间戳后缀：

```text
Write CF:
  user_key + commit_ts -> Write{type, start_ts, ...}

Default CF:
  user_key + start_ts  -> value
```

时间戳后缀的编码会让同一个 `user_key` 的多个版本在 RocksDB 物理顺序上相邻，并使较新的版本在
扫描该 key 的版本链时更容易先被访问到。因此一次 point read / scan 通常先在 Write CF 中找到
`commit_ts <= read_ts` 的最新可见版本，再根据 Write 里的 `start_ts` 到 Default CF 取 value。
这也是为什么同一个 key 堆积大量历史版本、rollback 或 lock-only 记录时，会带来 MVCC read
amplification。

```text
+------------+--------------------------------------+--------------------------------------+----------------------------------------------+
| TiKV CF    | 存储内容                             | 典型 key / value 形态                | 本项目内存结构对应                           |
+------------+--------------------------------------+--------------------------------------+----------------------------------------------+
| Write CF   | 已提交写记录，以及 rollback / lock   | user_key + commit_ts -> Write        | committed[key] 的 Version 链                 |
|            | 这类事务结局记录                     | Write 内含 start_ts / type / short   | rollback_records 单独保存 rollback 证据      |
|            |                                      | value / gc_fence 等                  |                                              |
+------------+--------------------------------------+--------------------------------------+----------------------------------------------+
| Default CF | 实际 value，尤其是大 value            | user_key + start_ts -> raw value     | pending[{key, start_ts}]；提交后可内联进     |
|            | prewrite 后、commit 前已经写入        |                                      | Version.value                                |
+------------+--------------------------------------+--------------------------------------+----------------------------------------------+
| Lock CF    | 当前未提交锁                         | user_key -> Lock                     | locks[key]                                   |
|            | 包括 pessimistic lock 和 prewrite lock| Lock 内含 primary / start_ts / ttl / |                                              |
|            |                                      | for_update_ts / min_commit_ts 等     |                                              |
+------------+--------------------------------------+--------------------------------------+----------------------------------------------+
```

更直观地说：

```text
TiKV Write CF:
  key@commit_ts -> Write{type=Put/Delete/Lock/Rollback, start_ts, ...}

TiKV Default CF:
  key@start_ts -> value

TiKV Lock CF:
  key -> Lock{type, primary, start_ts, ttl, for_update_ts, ...}

本项目:
  committed[key]        -> 已提交 Put/Delete 版本链
  pending[{key, ts}]    -> prewrite 后、commit 前的未提交 value
  locks[key]            -> 当前未提交 pessimistic/prewrite lock
  rollback_records      -> rollback 证据，独立于普通 scan 主路径
```

TiKV commit 时不会把完整 value 再写入 Write CF。prewrite 阶段已经把大 value 写入 Default CF：

```text
prewrite:
  Default CF: key@start_ts -> value
  Lock CF:    key          -> Lock{start_ts, short_value?}

commit:
  Write CF:   key@commit_ts -> Write{type=Put, start_ts, short_value?}
  Lock CF:    delete key
  Default CF: 不删除，也不改写
```

因此 TiKV 普通读路径是先查 Write CF 找到可见版本，再根据 `start_ts` 到 Default CF 取大 value。
如果 value 长度不超过 short value 阈值，value 会内联在 Lock / Write 记录中，读路径不需要再查
Default CF。这个设计让 Write CF 保持更小，更适合承载 MVCC 索引、快照判断和 scan 跳版本；代价是
大 value 点读可能多一次 Default CF 访问。本项目是内存状态机，可以在 commit 时直接把
`pending[{key, start_ts}]` 移入 `committed[key].Version.value`，不需要保留 TiKV 这种 Write CF /
Default CF 的二级索引形式。

TiKV Default CF 中的 value 不在 commit 时删除。commit 后它从“未提交 pending value”变成
“被 Write CF commit record 引用的已提交 value body”。它的删除时机是后台 GC：当某个旧版本已经
早于 GC safe point 且不再需要保留时，GC 删除对应的 Write CF 旧版本，并删除其引用的
`Default CF: key@start_ts -> value`。本项目如果采用内联 `Version.value`，则没有独立 Default CF
value 需要清理；GC 删除旧 `Version` 时自然同时释放 value。

TiKV 把 rollback record 也放在 Write CF 中，普通 MVCC 读遇到 `Rollback` / `Lock` 记录时需要继续
向旧版本查找。这在 RocksDB + GC + compaction 优化下是可接受的工程取舍，但对本项目
的内存状态机来说，没有必要让普通 scan 主路径为事务状态记录付额外成本。因此本项目把
`rollback_records` 单独保存，只在 prewrite 幂等性检查、rollback 幂等性检查、CheckTxnStatus /
resolve lock 这类事务状态路径中查询。

需要注意，TiKV 的这种混存并不是没有代价。如果同一个 key 上长期堆积大量 rollback write，
快照读需要跳过这些记录才能找到最近的 `Put/Delete`，会造成 MVCC read amplification。实际 TiKV
依赖 rollback 不是常态主路径、GC safe point 持续推进、compaction 后台清理、热点冲突监控和业务侧
降热点来把成本控制在可接受范围内。对本项目而言，`rollback_records` 独立存储可以直接避免普通
point read / scan 路径被大量 rollback 拖慢。若线上真的出现同 key 大量 rollback，根因通常是业务
热点冲突或事务重试过多，主要依赖业务层拆热点、降低冲突概率、缩短事务和限流来治理。

只支持悲观事务也有助于降低 rollback 堆积。乐观事务通常到 prewrite / commit 阶段才发现写冲突，
冲突判断基于事务较早的 `start_ts`，热点 key 上容易产生提交失败和 rollback record。悲观事务不只是
提前发现冲突：每次写语句加锁时都会使用新的 `for_update_ts`，acquire pessimistic lock 阶段就基于
这个更靠后的时间戳做主要写冲突判断，而不是等到提交阶段再用事务最早的 `start_ts` 判断。只要某个
key 成功拿到悲观锁且锁未丢失，后续 prewrite 对这个 key 正常不应再发生普通写冲突；prewrite 阶段
保留检查主要是为了处理锁丢失、请求重试、`for_update_ts` 不匹配、懒约束检查等边界情况。因此，
悲观锁既减少了长事务期间被中间提交事务判为冲突的概率，也把真实热点冲突提前转化为锁等待、锁超时
或语句失败，提交阶段 rollback 压力会小很多。不过悲观事务仍可能因为锁超时、TTL 过期、客户端断连、
deadlock 或主动 rollback 产生 rollback record，所以 `rollback_records` 仍然需要保留用于事务幂等和
resolve lock。

其中 `Version` 同时保存 `start_ts` 和 `commit_ts`：

```text
commit_ts:
  用于 MVCC 快照读。读请求 read_ts 要找 commit_ts <= read_ts 的最新版本。

start_ts:
  用于事务状态查询，当前项目主要是 secondary resolve lock 场景的 primary 状态查询。
  secondary lock 只知道 primary_key + start_ts，
  回查 primary 时必须通过 start_ts 找到该事务是否已提交，并拿到 commit_ts。
```

`for_update_ts` 是悲观事务写 SQL 的关键字段：

```text
1. start_ts 是事务级别，BEGIN 时确定，整个事务共用。
2. primary key 是事务级别，一个事务只有一个 primary，不是每条 SQL 一个 primary。
3. for_update_ts 是 key / statement 级别，不同 key 可以在不同 SQL 中被锁住，因此可能不同。
4. commit_ts 是事务级别，最终所有 mutation 共用同一个 commit_ts。
```

执行写 SQL / SELECT FOR UPDATE 时，coordinator 先向 TSO 获取新的 `for_update_ts`，
然后向目标 data group 发送 `AcquirePessimisticLock`。这个 Raft command 在状态机中串行 apply：

```cpp
Status AcquirePessimisticLock(int key,
                              uint64_t start_ts,
                              int primary_key,
                              uint64_t for_update_ts,
                              uint64_t ttl_ms) {
    if (rollback_records.contains({key, start_ts})) {
        return AlreadyRollback;
    }

    if (LatestCommitTs(key) > for_update_ts) {
        return WriteConflict;
    }

    if (locks.contains(key)) {
        if (locks[key].start_ts == start_ts) {
            locks[key].for_update_ts = std::max(locks[key].for_update_ts, for_update_ts);
            return OK; // 同事务重复加锁 / 推进 for_update_ts。
        }
        return Locked;
    }

    locks[key] = Lock{
        .start_ts = start_ts,
        .primary_key = primary_key,
        .ttl_ms = ttl_ms,
        .for_update_ts = for_update_ts,
        .type = Lock::Pessimistic,
    };
    return OK;
}
```

`for_update_ts` 到 pessimistic lock 真正落到 data group 状态机之间不是物理原子操作。
因此 AcquirePessimisticLock apply 时必须重新检查 `LatestCommitTs(key) <= for_update_ts`：

```text
T1: BEGIN start_ts=100
T1: 获取 for_update_ts=150
T2: commit key=10 at commit_ts=160
T1: AcquirePessimisticLock(key=10, for_update_ts=150) apply

此时 LatestCommitTs(10)=160 > 150，T1 必须 WriteConflict / 重新取更大的 for_update_ts 后重试。
```

如果 T1 的 pessimistic lock 先 apply，则后续 T2 看到 lock，只能等待、重试或 resolve lock。
所以悲观事务的 current read 不是让物理世界暂停，而是在 data group apply 时用 `for_update_ts`
做一次可证明的版本校验和加锁。

Prewrite 时需要把每个 key 自己的 `for_update_ts` 保留到 prewrite lock 中：

```cpp
locks[key] = Lock{
    .start_ts = start_ts,
    .primary_key = primary_key,
    .ttl_ms = is_primary ? primary_lock_ttl_ms : 0,
    .for_update_ts = key_specific_for_update_ts,
    .type = Lock::Put,
};
```

原因是不同 key 的悲观锁可能来自不同 SQL，`for_update_ts` 不一定相同。它用于标记该 prewrite lock
来自 pessimistic transaction，并记录这个 key 的 current-read / lock 校验边界；后续冲突返回、
min_commit_ts / commit_ts 推进和 lock resolve 都需要能看到这个字段。提交后的 `Version` 不需要保存
`for_update_ts`，MVCC 可见性仍然只依赖 `commit_ts`。

### 18.1 悲观长事务的 2PC 提交流程

COMMIT 时，coordinator 对完整 write set 执行 prewrite。prewrite groupA：

```cpp
locks[10] = Lock{
    .start_ts = 100,
    .primary_key = 10,
    .ttl_ms = primary_lock_ttl_ms,
    .for_update_ts = 120,
    .type = Lock::Put,
};

pending[{10, 100}] = "我是爸爸";
```

prewrite groupB：

```cpp
locks[11] = Lock{
    .start_ts = 100,
    .primary_key = 10,
    .ttl_ms = 0, // secondary lock 暂不单独使用 TTL；事务是否存活以后续回查 primary lock TTL 为准。
    .for_update_ts = 140,
    .type = Lock::Put,
};

pending[{11, 100}] = "我是爷爷";
```

prewrite 会把执行阶段的 `Pessimistic` lock 转成 `Put/Delete/Lock` prewrite lock，并把 mutation value 写入 `pending`。prewrite 成功后，新值还不能进入 `committed`，因此外部读不到新值，只能看到锁。

快照读必须检查目标 key 上是否存在会影响本次 read_ts 的 lock：

```text
如果 lock.start_ts <= read_ts：
  该 lock 对本次快照读有潜在影响，不能直接跳过。

如果 lock 属于本事务：
  不阻塞，不走 resolve lock。
  但也不能把 lock / pending value 当成 commit_ts <= start_ts 的 committed version。
  如果 coordinator 本地 write buffer 中有该 key 的 mutation，返回本事务未提交写，实现 read-your-own-writes。
  如果只是 SELECT FOR UPDATE / UPDATE 前置阶段留下的 Pessimistic lock，仍然按 start_ts 读取 committed 旧版本。

如果 lock 属于其他事务：
  需要返回 Locked，或由上层 lock resolver 查询 primary 状态。
```

读路径不能只看 `committed[key]` 中 `commit_ts <= read_ts` 的最新版本。否则会破坏快照隔离：

```text
T1: start_ts=100，prewrite key=10，留下 lock，但还没 commit primary。
T2: start_ts=150，读取 key=10。

如果 T2 无视 lock，直接读 committed 中旧版本：
  后续 T1 可能以 commit_ts=120 提交成功。
  那么 T2 的 read_ts=150 本应看到 T1 的写，却已经读到了旧值。
```

因此普通 2PC 下，读遇到其他事务的有效 lock 时，必须先 resolve：

```text
1. 根据 secondary lock 中的 primary_key + start_ts 回查 primary。
2. primary committed：拿到 commit_ts，补提交当前 secondary，然后重试读。
3. primary rollback / expired：清理当前 lock / pending，写 rollback record，然后重试读。
4. primary still locked：读请求等待、backoff 或返回 Locked。
```

读协助 resolve lock 在工程上是可接受的，但必须把它理解成冲突/遗留锁路径，而不是普通读的常态路径：

```text
少量 resolve lock：
  可接受。它负责清理 primary commit 后残留的 secondary lock，或清理 TTL 过期的 dead transaction。

频繁 resolve lock：
  不可接受。它会增加额外 RPC、Raft 写入、backoff 和读尾延迟，通常说明存在热点 key、长事务、锁等待或事务冲突。
```

如果根因是热点 key 同时承载高频读和高频写，系统层的 backoff、singleflight、resolved lock cache、
限流或 Async Commit 只能缓解，不能从根本上消除冲突。更有效的优化通常在业务建模层：

```text
1. counter / 统计类热点 key：拆成多个 shard key，写入分散，读时聚合。
2. 可延迟一致的总量：用异步聚合 / 缓存 / stale read 降低强一致读压力。
3. 库存 / 余额这类强约束热点：用预分配额度、token bucket、escrow 或显式排队串行化。
4. 事务内部慢操作：先在事务外计算，缩短持锁时间。
```

数据库层负责正确处理热点锁和控制退避重试成本，但不应该承诺自动消除所有业务热点。

当前项目先采用这条保守路径，不引入 Async Commit 的 `min_commit_ts` 优化。
这样协议更简单：只要读遇到影响 read_ts 的其他事务 lock，就必须等待或 resolve 到明确状态后再读。

所有 Group 的 prewrite 都确认成功之后，协调者才能进入 commit 阶段：

```text
1. commit primary key 10
2. commit secondary key 11
```

commit groupA primary：

```cpp
committed[10].push_back(Version{
    .start_ts = 100,
    .commit_ts = 150,
    .type = Version::Put,
    .value = pending[{10, 100}],
});

pending.erase({10, 100});
locks.erase(10);
```

commit groupB secondary：

```cpp
committed[11].push_back(Version{
    .start_ts = 100,
    .commit_ts = 150,
    .type = Version::Put,
    .value = pending[{11, 100}],
});

pending.erase({11, 100});
locks.erase(11);
```

如果 groupB prewrite 超时，协调者不能把它当成明确失败，因为请求可能已经在 groupB 成功 apply，只是响应丢失。正确做法是重试同一个 prewrite。prewrite 必须幂等：

```text
same start_ts + same key + same mutation
  -> 如果已经写入 lock/pending，则返回成功
  -> 如果已经 rollback，则返回 AlreadyRollback / TxnTooOld
```

如果最终决定放弃事务，需要对所有相关 Group 发送 rollback。rollback 也必须幂等：

```cpp
if (locks.contains(key) && locks[key].start_ts == start_ts) {
    locks.erase(key);
    pending.erase({key, start_ts});
}

rollback_records.insert({key, start_ts});
```

rollback marker 不能马上删除。它至少要保留到系统的 GC safe point 超过该事务的 `start_ts`。prewrite 路径需要优先检查同 `start_ts` 的 rollback record，防止 cleanup / rollback 之后迟到的 prewrite 重新成功：

```cpp
Status Prewrite(int key, std::string value, uint64_t start_ts, int primary_key, uint64_t for_update_ts) {
    if (rollback_records.contains({key, start_ts})) {
        return AlreadyRollback;
    }

    if (locks.contains(key)) {
        if (locks[key].start_ts == start_ts) {
            // 悲观事务正常路径下，应当已经持有本事务的 Pessimistic lock；
            // prewrite 将其转换成真正的 Put/Delete/Lock。
            locks[key].type = Lock::Put;
            locks[key].for_update_ts = std::max(locks[key].for_update_ts, for_update_ts);
            pending[{key, start_ts}] = value;
            return OK; // 幂等重试 / 锁转换
        }
        return Locked;
    }

    // 如果允许未提前加悲观锁的写，这里必须重新按 for_update_ts 检查 LatestCommitTs。
    // 本项目更保守的设计是：悲观事务 prewrite 前必须已经持有本事务的 Pessimistic lock；
    // 下面这个分支只用于说明兜底校验。
    if (LatestCommitTs(key) > for_update_ts) {
        return WriteConflict;
    }

    locks[key] = Lock{
        .start_ts = start_ts,
        .primary_key = primary_key,
        .ttl_ms = (key == primary_key ? primary_lock_ttl_ms : 0),
        .for_update_ts = for_update_ts,
        .type = Lock::Put,
    };
    pending[{key, start_ts}] = value;
    return OK;
}
```

### 18.2 primary 状态与 secondary lock 恢复

如果 primary 已经 commit，但 secondary commit 请求丢失，secondary Group 可能长期残留：

```text
locks[11] = Lock{start_ts=100, primary_key=10, ttl_ms=0, for_update_ts=140, type=Put}
pending[{11, 100}] = "我是爷爷"
```

后续访问 `11` 或后台 GC 扫到这个锁时，需要查 primary 状态：

```text
CheckTxnStatus(primary=10, start_ts=100)
```

如果 primary 已提交，则补提交 secondary：

```cpp
committed[11].push_back(Version{
    .start_ts = 100,
    .commit_ts = 150,
    .type = Version::Put,
    .value = pending[{11, 100}],
});

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
  ttl_ms = 0  // 本项目暂不使用 secondary TTL
  for_update_ts = 140
  type = Put
```

所以恢复路径是从 secondary lock 反查 primary 状态，而不是从 primary commit record 主动找到所有 secondaries。

事务提交只有在明确收到 commit success 或 rollback success 后，才能确定最终结果。
如果 coordinator 在 prewrite 后、commit primary 后或 commit secondary 过程中断线，
恢复后必须通过 `CheckTxnStatus(primary_key, start_ts)` 查询 primary 状态，不能自己假设事务已经提交或回滚。
业务层如果要重试整个事务请求，需要提供幂等语义，例如 request id / 唯一约束 / CAS 条件。

`CheckTxnStatus(primary_key, start_ts)` 可以按这个顺序判断：

```cpp
TxnStatus CheckTxnStatus(int primary_key, uint64_t start_ts) {
    if (rollback_records.contains({primary_key, start_ts})) {
        return RolledBack{};
    }

    for (const auto& version : committed[primary_key]) {
        if (version.start_ts == start_ts) {
            return Committed{version.commit_ts};
        }
    }

    auto lock_it = locks.find(primary_key);
    if (lock_it != locks.end() && lock_it->second.start_ts == start_ts) {
        if (LockTtlExpired(lock_it->second)) {
            // primary lock TTL 过期后，可以把该事务判定为 dead transaction，
            // 通过 Raft 状态机写入 rollback record，并清理 primary lock/pending。
            return RolledBack{};
        }
        return Locked{lock_it->second};
    }

    if (start_ts <= gc_safe_point) {
        return TxnTooOldOrUnknown{};
    }

    return NotFound{};
}
```

secondary 回查 primary 时直接线性扫描 `committed[primary_key]`，匹配 `version.start_ts == start_ts`。
如果没有 primary lock、没有 commit record、没有 rollback record，并且 `start_ts <= gc_safe_point`，
说明事务状态证据可能已经被 GC 删除，此时只能返回 `TxnTooOldOrUnknown`，不能误判成 rollback。

不建议让 secondary 回查 primary 时直接对 `committed[key]` 按 `start_ts` 二分：

```text
1. committed[key] 的主排序键应该是 commit_ts，因为快照读依赖 commit_ts 可见性。
2. TiKV 的 Write CF 也是按 key + commit_ts 组织，CheckTxnStatus 是在 write records 中找 matching start_ts。
3. TiKV 不把 start_ts 与 commit_ts 同序作为协议前提；悲观事务下可能出现 start_ts 较小但 commit_ts 较大的记录。
4. 本项目支持 pessimistic 长事务，for_update_ts、rollback record 和 GC 特殊记录都会打破对 start_ts/commit_ts 隐含同序的依赖。
```

因此更稳的做法是：

```text
普通 MVCC 读:
  使用 committed[key]，按 commit_ts 查 <= read_ts 的最新版本。

事务状态查询（当前项目主要是 secondary resolve lock）:
  先查 rollback_records，再线性扫描 committed[key] 匹配 start_ts。
```

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

本项目不再区分 read 事务和 write 事务：

```text
active transaction:
  BEGIN 时获取 start_ts，并加入 coordinator 本地 active_txns。
  后续该事务既可能 snapshot read，也可能写入，因此只要 coordinator lease 仍有效，
  meta 计算 target_safe_point 时就不能超过全局最小 min_active_start_ts。
  coordinator 长时间断线或事务超过 max_txn_ttl / gc_max_wait_time 后，其 min_active_start_ts 失效，
  GC 可以继续推进；迟到事务只能返回 TxnExpired / SnapshotTooOld。

old locks:
  GC 推过 target_safe_point 前，仍然必须 resolve lock.ts <= target_safe_point 的 old locks。
  如果 primary 已 committed，就补 commit secondary；如果 primary lock TTL 过期且未提交，就 rollback。
```

贴近 TiDB 的 min start ts 上报流程：

```text
1. 每个 coordinator 本地维护 active_txns，计算 min_active_start_ts。
2. coordinator 周期性向 meta leader 上报 {coordinator_id, min_active_start_ts, lease_epoch}。
3. meta raft group 只保存每个 coordinator 的最小 start_ts，不保存每个事务。
4. meta raft leader 选择 target_safe_point，不能超过所有有效 coordinator lease 中的最小 min_active_start_ts。
5. GC worker 中心调度 Resolve Locks，扫描所有 data group 的 lock.ts <= target_safe_point。
6. 对扫到的 old lock 查 primary，并 commit / rollback secondary；过期 primary lock 可以 rollback。
7. old locks resolve 完成后，推进 gc_safe_point。
8. distributed Do GC：各 store / data group 后台观察 gc_safe_point，自行分批物理删除旧版本和过老 rollback marker。
```

primary lock TTL 是必须的正确性/可用性边界：

```text
1. prewrite primary 时写入 TTL。
2. 活着的长事务需要 heartbeat 延长 primary lock TTL。
3. TiKV 的 Lock 结构中每个 lock 都有 ttl 字段，但事务状态裁决实际通过 CheckTxnStatus 回查 primary。
4. 本项目 secondary lock 的 ttl_ms 暂时没有实际用途，可以置 0 或仅作为调试信息；不能用 secondary TTL 单独 rollback。
5. coordinator 断线后，primary lock TTL 到期，lock resolver / GC 才能安全 rollback dead transaction。
6. 没有 primary TTL 时，dead coordinator 留下的 primary lock 可能永久阻塞冲突写入和 GC safe point 推进。
```

这里的关键边界是：

```text
Resolve Locks:
  正确性屏障，必须确认 target_safe_point 之前的 old locks 都已经处理完。

Do GC:
  空间回收任务，gc_safe_point 推进后可以由各 store / data group 本地异步执行。
```

本项目状态机是内存 MVCC，不像 TiKV/RocksDB 可以依赖底层有序 keyspace scan 或 compaction filter
顺手清理旧版本。因此 data group 本地需要维护一个 committed 版本链的 GC 候选优先队列，
避免每次 `Do GC` 都全量遍历 `committed map`。

优先队列元素是：

```cpp
struct GcCandidate {
    int key;
    uint64_t second_oldest_commit_ts;
};
```

队列按 `second_oldest_commit_ts` 小顶堆排序，而不是按最老版本的 `commit_ts` 排序。
原因是 MVCC GC 不能删除 safe point 前的最后一个可见版本：

```text
key=10 versions:
  commit_ts = 10
  commit_ts = 20
  commit_ts = 30
  commit_ts = 40

gc_safe_point = 35

可以删除:
  10, 20

必须保留:
  30  // safe point 前最后一个可见版本
  40
```

因此一个 key 真正出现可删除版本的条件不是：

```text
oldest_commit_ts < gc_safe_point
```

而是：

```text
second_oldest_commit_ts < gc_safe_point
```

`committed` 的所有新版本插入必须收口到统一方法中维护这个队列：

```cpp
void InsertCommitted(int key, Version version) {
    auto& versions = committed[key];

    bool should_enqueue = (versions.size() == 1);

    versions.push_back(std::move(version));

    if (should_enqueue) {
        committed_gc_heap.push(GcCandidate{
            .key = key,
            .second_oldest_commit_ts = versions[1].commit_ts,
        });
    }
}
```

这里不需要额外的 `generation` 或 `in_heap` 字段，只要保证这个不变量：

```text
committed[key].size() >= 2  <=>  key 在 committed_gc_heap 中
committed[key].size() < 2   <=>  key 不需要进入 committed_gc_heap
```

`Do GC` 时弹出堆顶 key，沿着该 key 的整条版本链一次性删除所有可删除旧版本，
但保留 safe point 前最后一个可见版本。清理完成后，如果该 key 仍然有两个及以上版本，
则用新的第二老版本 `commit_ts` 重新入堆：

```text
1. while heap.top.second_oldest_commit_ts < gc_safe_point:
2.   pop key
3.   清理 committed[key] 中所有 safe point 前、但不是 safe point 前最后一个可见版本的旧版本
4.   如果 committed[key].size() >= 2:
5.       push {key, committed[key][1].commit_ts}
```

这样 `Do GC` 的成本接近实际被清理的 key/version 数量，而不是每次都扫描全量
`committed map`。

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

    struct CoordinatorTxnState {
        uint64_t min_active_start_ts;
        uint64_t lease_epoch;
        uint64_t lease_deadline_ms;
    };
    std::map<int, CoordinatorTxnState> coordinator_txn_states;

    // shard / range 路由、节点 membership、全局配置等。
};
```

meta raft group 可以负责：

```text
1. 分配 TSO / start_ts / for_update_ts / commit_ts
2. 维护 coordinator 级 min_active_start_ts 上报和租约
3. 维护 GC safe point
4. 记录 Region / Shard 路由
5. 选出或直接承载 GC worker
6. 维护全局配置与 membership
```

meta raft group 的 leader 可以直接作为 GC worker，但 GC 任务必须跑在后台线程中，不能阻塞 meta raft apply / heartbeat / TSO 请求。

coordinator 上报 min_active_start_ts 的粒度应该是 coordinator 级别，而不是事务级别：

```text
coordinator 本地可能同时运行多个事务。
每轮上报只需要取这些事务 start_ts 的最小值。
meta 只需要保存每个 coordinator 的 min_active_start_ts 和租约状态。
coordinator 租约过期后，该 coordinator 的 min_active_start_ts 不再保护 GC。
```

GC RPC 建议携带 epoch 信息：

```text
meta_term
gc_epoch
target_safe_point
```

data group 只接受当前有效 epoch 的 GC 请求，避免旧 meta leader 失去 leadership 后继续执行过期 GC。

同时，meta raft group 不应该参与每条跨 Group 事务的提交决策，否则会变成全局事务瓶颈。它只提供时间戳和全局元信息；数据写入仍然由各 data raft group 通过 MVCC + 2PC 完成。

### 18.5 Async Commit 暂不实现，但可以作为后续优化

TiDB / TiKV 中存在 Async Commit / 1PC 优化，但它不应该作为本项目第一版事务协议的基础。

当前本地 TiDB 源码中的默认值是：

```text
tidb_enable_async_commit = OFF
tidb_enable_1pc = OFF
```

因此更贴近工程实践的第一版设计是：

```text
1. 普通 2PC 是基础路径。
2. primary lock + CheckTxnStatus + Resolve Lock 是正确性核心。
3. Async Commit / min_commit_ts 是提交延迟优化，不是必须能力。
```

Async Commit 的核心思想是：prewrite lock 中保存 `min_commit_ts`，表示该事务未来合法 commit_ts
必须大于等于这个下界。读事务遇到未提交 lock 时，如果发现 `reader_start_ts >= lock.min_commit_ts`，
可以通过 CheckTxnStatus 把 primary lock 的 `min_commit_ts` 推到 `reader_start_ts + 1` 或更大。
之后当前读就可以安全绕过这批 lock 读取旧版本，因为该写事务已经不可能再用小于等于本次 read_ts
的 commit_ts 提交成功。

这能减少读被未完成 prewrite lock 阻塞的时间，但代价很明确：

```text
1. 读路径可能触发写操作：更新 primary lock 的 min_commit_ts，需要走 Raft apply。
2. lock resolve 路径更复杂：读可能既要查 primary，又要 push min_commit_ts，再携带 bypass_locks 重试。
3. commit 路径更复杂：commit_ts 必须满足所有 lock 上被推进后的 min_commit_ts，否则要换更大的 commit_ts 或 fallback。
4. DDL / schema change / GC 需要考虑 async commit safe window。
5. 热点冲突下会增加额外写放大和尾延迟。
```

所以本项目暂不实现 Async Commit：

```text
快照读遇到会影响 read_ts 的其他事务 lock：
  必须 wait / backoff / resolve lock，不能直接跳过。

未来要优化提交延迟或减少读等待时：
  再引入 min_commit_ts、bypass_locks、async secondary commit 和对应恢复逻辑。
```

### 18.6 TiKV 的实际处理方式

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
6. old locks 处理完成后，推进 gc_safe_point。
7. distributed Do GC：TiKV 节点观察 safe point 变化，本地扫描并删除旧 MVCC 版本。
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

---

## 19. 双 Manifest LSM Snapshot 与 Field-Level Delta MVCC

本节记录一个更进一步的 snapshot / 持久化状态机设计：在线数据仍然全部维护在内存中，磁盘侧使用
LSM/SST 作为辅助持久化状态机和 snapshot 传输载体。它不是传统 RocksDB checkpoint，而是一个
Raft-aware 的双 manifest snapshot backend。

原始问题描述如下：

```text
搞两个manifest，一个是snapshot manifest，记录最新的snapshot对应到哪些sst，一个是active L0 manifest，记录着自上次打snapshot以来的新增L0 sst，compaction线程根据manifest整理各自manifest中的sst，打新snapshot得时候，先将active L0 manifest中的sst加到snapshott manifest中，然后acitive L0 manifest请空即可。然后如果Leader有installSnapshot需求的，在发送snapshot前先获取snapshot manifest，将其中的所有sst做引用+1，然后compaction线程在compact的时候对于引用不为0的sst文件先不删除，等到installsnapshot结束后解引用为0后自己删除。

因为我的数据全在内存，读数据时不需要到磁盘LSM tree中查找，所以可以支持kv到部分字段修改，也就是sst中只记录此次version的update字段（key=raw_key+version，value={字段1id+字段1新值+字段2id+字段2新值...}），然后compaction的时候对于不需要的version，将其value直接合并到上一个version中的value即可。代价是compaction不是像rocksdb一样简单设置个compaction filter，然后直接丢弃不需要的kv，而是要解析kv然后合并，可能会消耗些cpu，内存结构也是mvcc链的元素只记录update字段，读的时候聚合从旧的version到新的version的update字段，读的性能也会打点折扣，但是还是读内存还是非常快，最重要是内存和磁盘的存储空间都会大幅减少，发送snapshot时的量也会小得多，崩溃恢复时snapshot恢复到内存也会快得多。
```

### 19.1 双 Manifest Snapshot Backend

核心结构分成两套 manifest：

```cpp
struct SstMeta {
    uint64_t file_id;
    uint64_t size_bytes;
    uint64_t smallest_key;
    uint64_t largest_key;
    uint32_t level;
    uint64_t generation;
    uint64_t min_commit_ts;
    uint64_t max_commit_ts;
    uint32_t checksum;
    uint32_t ref_count;
};

struct SnapshotManifest {
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    uint64_t manifest_generation;
    std::vector<SstMeta> files;
};

struct ActiveL0Manifest {
    uint64_t start_index;       // snapshot_index + 1
    uint64_t max_applied_index; // active manifest 已持久化到的最大 apply index
    uint64_t manifest_generation;
    std::vector<SstMeta> files;
};
```

语义如下：

```text
snapshot manifest:
  最新一个可用于 InstallSnapshot / Raft log GC 的稳定基线。
  其中的 SST 只包含 snapshot_index 及之前的状态。

active L0 manifest:
  自上次 snapshot 之后新增的 L0 SST。
  这部分已经持久化，但还没有进入稳定 snapshot baseline。
```

打新 snapshot 的流程：

```text
1. 暂停 data group apply。
2. Flush 当前 memtable，确保 <= lastApplied 的变更都落成 SST，并进入 active L0 manifest。
3. 加 manifest_mutex，阻止 active manifest compaction 与 seal 并发修改 manifest。
4. snapshot_manifest.files += active_l0_manifest.files。
5. snapshot_manifest.snapshot_index = lastApplied。
6. snapshot_manifest.snapshot_term = term(lastApplied)。
7. snapshot_manifest.manifest_generation++。
8. active_l0_manifest.clear()，并设置新的 start_index = lastApplied + 1。
9. 将两套 manifest 写入 MANIFEST.tmp，fsync，rename 为 MANIFEST，fsync 目录。
10. 恢复 apply。
11. Raft log GC 只允许推进到 snapshot_manifest.snapshot_index。
```

compaction 线程按 manifest 分组工作：

```text
1. snapshot manifest 内部 SST 可以互相 compact。
2. active L0 manifest 内部 SST 可以互相 compact。
3. snapshot manifest 和 active L0 manifest 不能混合 compact。
4. compaction 输出 SST 必须回写到对应 manifest 中。
5. manifest 更新必须原子：先生成 output SST 并 fsync，再 manifest remove input / add output。
6. input SST 如果 ref_count == 0 且不再被任何 manifest 引用，才能物理删除。
```

这样可以保证：

```text
snapshot manifest 对应的视图不会混入 snapshot_index 之后的数据。
active L0 manifest 中的新数据不会破坏正在发送或已经持久化的 snapshot 视图。
snapshot 内部仍然可以 compaction，旧 SST 不需要永久 pin 住。
```

InstallSnapshot 时获取 pinned snapshot handle：

```cpp
struct SnapshotHandle {
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    uint64_t manifest_generation;
    std::vector<SstMeta> files;
};
```

流程：

```text
AcquireSnapshotHandle:
  1. 加 manifest_mutex。
  2. 复制当前 snapshot_manifest。
  3. 对其中每个 SST ref_count++。
  4. 释放 manifest_mutex。

InstallSnapshot send:
  按 handle.files 发送 SST 文件和 snapshot metadata。

ReleaseSnapshotHandle:
  1. 对 handle.files 中每个 SST ref_count--。
  2. 如果某个 SST ref_count == 0 且不在当前 manifest 中，加入 obsolete delete queue。
```

发送期间 snapshot manifest 可以继续被 compaction 更新。发送线程使用的是 acquire 时 pin 住的旧文件集合；旧 SST
即使已经被新的 compacted SST 取代，也会因为 ref_count > 0 而暂时不能删除。

恢复流程：

```text
1. 读取 MANIFEST。
2. 加载 snapshot_manifest 对应的稳定基线 SST。
3. 加载 active_l0_manifest 中 snapshot 之后的增量 SST。
4. 根据磁盘 SST 重建内存 MVCC 状态。
5. 从 Raft log replay active_l0_manifest.max_applied_index + 1 之后的 entry。
```

因此 `snapshot_manifest` 是对外可安装、可 GC Raft log 的稳定基线；`active_l0_manifest` 是本机已经持久化但尚未
seal 进 snapshot baseline 的增量状态。

### 19.2 Field-Level Delta MVCC

由于在线读写全部走内存 MVCC，磁盘 LSM 不承担在线读查询，因此磁盘和内存中的 MVCC version 都可以只保存字段级 delta，
而不是完整 value。

当前设计不支持复杂 SQL / 在线 DDL。表的字段集合和 `field_id` 映射在 `CREATE TABLE` 时固定，后续不支持
`ALTER TABLE DROP COLUMN / ADD COLUMN`。如果需要变更字段集合，创建新 table 并迁移数据。

当前设计要求所有字段都是 non-null，并且每一行的每个字段都必须有值：

```text
1. INSERT / FullBase 必须包含完整字段集合；如果业务没有传某个字段，只能由 schema default 补齐。
2. UPDATE / FieldDelta 只记录被修改字段，但前提是该 row 已经存在。
3. UPDATE 不允许隐式创建不存在的 row。
4. DELETE 只在行级发生，写 RowDelete tombstone。
```

SST 编码：

```text
key   = table_id + raw_key + commit_ts/version
value = version_type + field_delta_list

version_type:
  FullBase    // 完整行，用于 INSERT / compact 后的 base version。
  FieldDelta  // UPDATE 的字段级增量。
  RowDelete   // DELETE 整行 tombstone。

field_delta_list:
  field_id_1 + encoded_value_1
  field_id_2 + encoded_value_2
  ...
```

内存结构也保持同样语义：

```cpp
struct FieldDelta {
    uint32_t field_id;
    std::string value;
};

struct VersionDelta {
    uint64_t start_ts;
    uint64_t commit_ts;
    enum Type { FullBase, FieldDelta, RowDelete } type;
    std::vector<FieldDelta> fields;
};

std::map<int, std::vector<VersionDelta>> committed;
```

读流程：

```text
1. 找到 commit_ts <= read_ts 的最新 VersionDelta。
2. 如果该 version 是 RowDelete，返回该行不存在。
3. 如果是 FieldDelta，从目标 version 沿 MVCC 链向旧版本收集字段 delta。
4. 对同一个 field_id，越新的 delta 优先级越高。
5. 遇到 FullBase 后，用该 full/base version 补齐未出现字段并停止。
6. 由于所有字段都要求有值，最终必须能组装出完整 row；否则说明数据损坏或 INSERT 违反约束。
```

对于 latest read，可以额外维护 `current_materialized_value[key]`，避免每次都沿 delta 链聚合；历史 snapshot read
才需要按 MVCC 链聚合字段 delta。

compaction / GC 规则：

```text
1. MVCC GC 仍然必须保留 safe point 前最后一个可见版本。
2. 被删除的旧 delta 不能简单丢弃；需要合并到保留边界 version。
3. 合并后，该边界 version 可以变成 FullBase 完整 base version。
4. 当前设计不支持字段级删除；删除只发生在行级，通过 RowDelete tombstone 表达。
5. INSERT / compact 后生成的 FullBase 必须包含完整字段集合。
6. 如果 safe point 前最后一个可见版本是 RowDelete，则可以只保留 RowDelete，删除更早的 FullBase / FieldDelta。
```

例子：

```text
v10 FullBase:   {a=1, b=1, c=1}
v20 FieldDelta: {a=2}
v30 FieldDelta: {b=3}
v40 FieldDelta: {c=4}

gc_safe_point = 35
需要保留 safe point 前最后一个可见版本 v30。

可以 compact 为：
v30 FullBase:   {a=2, b=3, c=1}
v40 FieldDelta: {c=4}

然后删除 v10 / v20 的独立记录。
```

这和普通 RocksDB compaction filter 不同。普通 filter 可以在判断某个 KV 已过期后直接丢弃；field-level delta MVCC
必须解析 value 并做字段级合并，否则会丢失仍然需要被新保留版本继承的字段。

收益：

```text
1. 单字段更新不需要重写完整 value。
2. 内存 MVCC version 链占用更小。
3. SST 写入量更小。
4. snapshot manifest 发送的数据量更小。
5. 崩溃恢复从 SST 重建内存时读取的数据更少。
6. 对宽行 / 多字段对象 / 高频局部字段更新尤其有利。
```

代价：

```text
1. compaction CPU 更高，需要解析和合并 field_delta_list。
2. 历史读需要沿 delta 链聚合字段，读放大会增加。
3. 必须周期性生成 full/base version，限制最坏读链长度。
4. field_id 映射必须在 CREATE TABLE 时写入状态机共识内容，恢复和 InstallSnapshot 时必须能解析旧 delta。
5. 当前不支持在线 schema 演进；字段集合变化通过新建 table 并迁移数据完成。
```

因此该设计的核心取舍是：用后台 compaction CPU 和一部分历史读放大，换取内存空间、磁盘空间、snapshot 网络传输量和恢复速度的大幅优化。

### 19.3 与 Fork Snapshot 的差异

```text
fork snapshot:
  依赖 OS COW 获取一致内存视图。
  每次 snapshot 全量 dump 状态机。
  热写期间可能产生大量 COW 内存放大。
  多线程 fork 后子进程运行复杂 C++ 逻辑存在稳定性风险。

双 manifest + field-level delta MVCC:
  snapshot 成本主要是 flush + manifest seal。
  数据持续以 SST 增量形式落盘。
  snapshot 发送以 SST 文件集合为单位，可显著减少传输量。
  compaction 可以持续压缩 snapshot manifest 内的数据。
  不需要 fork，不承担 COW 内存放大。
  复杂度集中在 manifest 原子更新、SST 引用计数、compaction crash safety 和 field delta 合并。
```

该方案技术深度高，但实现复杂度也明显高于普通 full snapshot / RocksDB checkpoint。第一版可以先保证：

```text
1. manifest seal 正确。
2. active 与 snapshot manifest 不混合 compaction。
3. InstallSnapshot pin / unpin 正确。
4. field-level delta 读写和 GC 合并语义正确。
5. 崩溃恢复不会出现 manifest 指向不存在 SST，或 Raft log 已 GC 但 snapshot 不可恢复。
```
