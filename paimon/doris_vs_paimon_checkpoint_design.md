# Paimon vs Doris：Checkpoint 延迟问题的根本差异

## 核心答案

**Doris 不会出现 Paimon 的 Checkpoint 延迟问题，因为两个系统的架构完全不同！**

虽然都基于 LSM Tree，但：
- **Paimon**：Checkpoint 时**同步等待**压缩
- **Doris**：Checkpoint 与压缩**完全异步解耦**

---

## 1. 架构差异对比

### Paimon 的设计：Checkpoint 同步等待压缩

```
┌─────────────────────────────────────────────────────────────┐
│                    Paimon 写入 & Checkpoint                  │
│                                                              │
│  数据流向                                                     │
│  ┌──────────┐     ┌──────────┐     ┌─────────┐  ┌────────┐ │
│  │ Flink   │────▶│WriteBuffer│───▶│Level-0  │─▶│Manifest│ │
│  │ Source  │     │(内存)     │    │Files    │  │(元数据)│ │
│  └──────────┘     └──────────┘     └─────────┘  └────────┘ │
│                                         △                   │
│  Checkpoint 流程                         │                   │
│  ┌──────────────────────────────────────┼──────────────────┐ │
│  │  1. flushWriteBuffer()               │                  │ │
│  │     - 将内存数据写到 Level-0        │                  │ │
│  │                                      │                  │ │
│  │  2. shouldWaitForPreparingCheckpoint()  ⚠️ 关键！        │ │
│  │     if (numberOfSortedRuns > 4)     │                  │ │
│  │        waitCompaction = true         │                  │ │
│  │                                      │                  │ │
│  │  3. ⏸️ 同步等待压缩完成              │                  │ │
│  │     trySyncLatestCompaction(true)   │                  │ │
│  │     ├─ compactManager.getCompactionResult(blocking=true)  │
│  │     └─ Future.get() 阻塞             │                  │ │
│  │                                      ▼                  │ │
│  │  4. 返回 CommitMessage                                  │ │
│  │     (包含压缩结果)                   │                  │ │
│  └──────────────────────────────────────┼──────────────────┘ │
│                                         │                   │
│  后台压缩                               │                   │
│  ┌──────────────────────────────────────┘                   │
│  │ CompactManager (异步后台线程)                           │
│  │ ├─ k-way merge                                          │
│  │ ├─ 生成 Level-1+ 文件                                   │
│  │ └─ 更新 Levels 结构                                    │
│  └──────────────────────────────────────────────────────────┘
│
│  特点：Checkpoint 必须等待压缩完成！
└─────────────────────────────────────────────────────────────┘
```

### Doris 的设计：Checkpoint 与压缩完全异步

```
┌──────────────────────────────────────────────────────────────┐
│                    Doris 写入 & Checkpoint                    │
│                                                               │
│  数据流向                                                      │
│  ┌──────────┐  RPC  ┌──────────┐  ┌──────────┐  ┌────────┐  │
│  │ Flink   │──────▶│ Doris BE │──▶│Memory    │─▶│RowSet  │  │
│  │ Connector     │  │ Tablet   │  │(Memtable)│  │        │  │
│  └──────────┘     └──────────┘  └──────────┘  └────────┘  │
│                                                               │
│  Checkpoint 流程（仅确认写入）                                │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  1. Flink Checkpoint 触发                              │ │
│  │     - 通知 Doris：记录当前的 WAL 位置                 │ │
│  │     - 返回 CommitMessage（包含 tablet_id, version）    │ │
│  │                                                        │ │
│  │  2. ⚡ 立即返回（无等待！）                             │ │
│  │     - 并不关心 Level-0 的状态                         │ │
│  │     - 不等待任何压缩                                  │ │
│  │                                                        │ │
│  │  3. Checkpoint 完成（1-2 秒）                         │ │
│  │                                                        │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  后台压缩（独立进行，完全不影响 Checkpoint）                  │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ BE 后台压缩线程（独立于 Checkpoint）                    │ │
│  │ ├─ 定时检查 Level-0 大小                              │ │
│  │ ├─ 定时检查 Level-0 文件数                            │ │
│  │ ├─ 条件满足时异步 Compact                             │ │
│  │ │  (不阻塞任何写入或 Checkpoint)                      │ │
│  │ ├─ k-way merge                                        │ │
│  │ └─ 生成 Level-1+ 文件                                 │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  特点：Checkpoint 不关心压缩状态！
└──────────────────────────────────────────────────────────────┘
```

---

## 2. 关键机制差异

### 差异 1：Checkpoint 时是否同步等待压缩

| 项目 | Paimon | Doris |
|------|--------|-------|
| **Checkpoint 时等待压缩** | ✅ **YES** (默认) | ❌ NO |
| **代码位置** | MergeTreeWriter.prepareCommit() 第 639 行 | 无对应实现 |
| **等待方式** | `Future.get()`（同步阻塞） | 异步独立进行 |
| **触发条件** | SortedRun > 4 或 commitForceCompact | 时间或大小阈值 |
| **影响** | ⏸️ Checkpoint 延迟到分钟级 | ✨ Checkpoint 仍为秒级 |

### 差异 2：Level-0 文件堆积的处理

#### Paimon 的处理

```java
// MergeTreeWriter.java:634-639
if (compactManager.shouldWaitForPreparingCheckpoint()) {
    // Level-0 文件太多了，必须等待压缩！
    waitCompaction = true;
    trySyncLatestCompaction(waitCompaction);  // ⏸️ 阻塞等待
}
```

**结果**：
- 强制等待压缩完成
- Checkpoint 变慢（可达 5-10 分钟）
- 但保证 Level-0 不会无限堆积

#### Doris 的处理

```
Checkpoint 时：
  └─ 直接返回，不管 Level-0 状态

后台压缩时：
  ├─ 独立判断 Level-0 大小
  ├─ 如果超过阈值（例如 1GB），触发压缩
  └─ 压缩完全异步进行，不影响写入和 Checkpoint
```

**结果**：
- Checkpoint 总是快速返回
- Level-0 可能临时堆积，但不阻塞 Checkpoint
- 写入性能更好，Checkpoint 延迟更低

### 差异 3：提交机制

#### Paimon 的三阶段提交

```
Stage 1: PrepareCommit
  ├─ 刷新缓冲区
  ├─ 等待压缩（如需要）
  └─ 返回 CommitMessage
          ↓
Stage 2: Commit (全局提交)
  ├─ 收集所有 CommitMessage
  └─ 提交到 Manifest
          ↓
Stage 3: NotifyCheckpointComplete
  ├─ 通知所有算子
  └─ 清理中间状态
```

**关键**：PrepareCommit 时已经**同步处理压缩**

#### Doris 的简化提交

```
Checkpoint Barrier
  ├─ Flink Connector 记录状态
  └─ 返回 CommitMessage（轻量化）
          ↓
Checkpoint Complete
  ├─ 通知 Doris 提交
  └─ Doris 记录版本号
          ↓
后台压缩（独立进行）
  └─ 不与提交交互
```

**关键**：提交不需要**等待压缩**

---

## 3. 为什么 Paimon 要同步等待压缩？

### Paimon 的设计选择（可追溯原因）

```
需求：
  1. 单机多分区/Bucket 的 LSM Tree 管理
  2. 强一致性保证（表级别的快照隔离）
  3. 避免运行时 Level-0 爆炸

解决方案：
  ├─ 在 Checkpoint 时检查 Level-0 堆积
  ├─ 如果堆积太多，强制等待压缩
  └─ 确保下一个 Checkpoint 时 Level-0 不会更多

代码体现：
  └─ shouldWaitForPreparingCheckpoint()
       return levels.numberOfSortedRuns() > numSortedRunStopTrigger + 1
```

**优点**：
- ✅ 读取性能稳定（Level-0 文件数有上界）
- ✅ 避免运行时故障（Level-0 爆炸）
- ✅ 保证写入性能逐步下降而不是突然故障

**代价**：
- ❌ Checkpoint 时间波动大（1s ~ 10min）
- ❌ 任务吞吐量不稳定

---

## 4. 为什么 Doris 不需要同步等待？

### Doris 的架构优势

#### 1. **分布式架构 vs 单机架构**

```
Paimon：
  └─ 单机上管理多个分区/Bucket
     ├─ Level-0 堆积全部发生在一台机器
     └─ 单个 Checkpoint 必须处理全部堆积

Doris：
  └─ 分布式架构，每个 BE 节点独立
     ├─ Level-0 堆积分散到多个 BE
     ├─ 单个 BE 的堆积影响相对较小
     └─ 一个 BE 的压缩不影响其他 BE 的 Checkpoint
```

#### 2. **写入模型差异**

```
Paimon：
  写入 → 内存 WriteBuffer → Level-0 文件 → (压缩) → Levels
                                  △
                            Checkpoint 时必须处理

Doris：
  写入 → RPC → BE 内存 → Memtable → (后台异步 flush) → RowSet
                                            ↓
                                    与 Checkpoint 解耦
```

#### 3. **压缩触发时机**

```
Paimon：
  ├─ Checkpoint 时：shouldWait...检查 → 同步等待
  └─ 普通写入时：CompactManager.triggerCompaction(false) → 异步

Doris：
  ├─ 时间触发：每 X 秒检查一次
  ├─ 大小触发：Level-0 大小超过 Y MB
  └─ 数量触发：Level-0 文件数超过 Z 个
  （全部异步，与 Checkpoint 无关）
```

---

## 5. 详细对比表格

| 维度 | Paimon | Doris | 影响 |
|------|--------|-------|------|
| **Checkpoint 的职责范围** | 包括压缩同步 | 仅记录位置 | Paimon Checkpoint 变慢 |
| **Level-0 堆积处理** | Checkpoint 时处理 | 后台异步处理 | Paimon 一次处理多个堆积 |
| **压缩与写入的关系** | 耦合（通过阈值） | 解耦（独立线程） | Paimon 写入性能波动 |
| **分布式特性** | 单机 LSM 树 | 分布式 Tablet | Doris 压力分散 |
| **同步点数量** | 3+ 个（Prepare + Commit + Compact） | 1 个（仅 Commit） | Paimon 同步成本高 |
| **Checkpoint 延迟范围** | 1-600s（取决于压缩） | 1-3s（稳定） | Paimon 波动大 |

---

## 6. 具体代码对比

### Paimon 的 Checkpoint 处理

```java
// paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:621-642
@Override
public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
    // 第 1 步：刷新缓冲区
    flushWriteBuffer(waitCompaction, false);

    // 第 2 步：检查是否需要强制压缩
    if (commitForceCompact) {
        waitCompaction = true;
    }

    // 第 3 步：检查 Level-0 堆积 ⚠️ 关键决策！
    if (compactManager.shouldWaitForPreparingCheckpoint()) {
        waitCompaction = true;
    }

    // 第 4 步：同步等待压缩 ⏸️ 阻塞点！
    trySyncLatestCompaction(waitCompaction);

    // 第 5 步：收集增量
    return drainIncrement();
}
```

### Doris 的对应处理

```
(Flink Connector 侧)
在 Checkpoint 时：
  1. 记录当前已发送的数据位置
  2. 返回 CommitMessage（轻量级）
  3. 立即返回，不等待任何后台操作

(Doris BE 侧)
后台独立进行：
  1. 监控 Memtable 大小
  2. 定期 flush 到 Level-0
  3. 监控 Level-0 大小/数量
  4. 异步触发 Compaction
```

**代码体现**：Doris Connector 中没有对应的 "shouldWaitForPreparingCheckpoint" 逻辑

---

## 7. 性能对比模拟

### 场景：恢复后的第一个 Checkpoint

```
Paimon：
  ├─ 检查 Level-0：10 个文件
  ├─ shouldWaitForPreparingCheckpoint()：10 > 4 + 1 = true
  ├─ 进入 trySyncLatestCompaction(true)
  ├─ ⏸️ 等待压缩（k-way merge 10 个文件）
  ├─   ├─ 读取 I/O：1-2 分钟
  ├─   ├─ 排序合并：1-2 分钟
  ├─   └─ 写入结果：1-2 分钟
  ├─ 压缩完成，返回
  └─ Checkpoint 完成：5-10 分钟 ⏳

Doris：
  ├─ 记录当前 WAL 位置
  ├─ 返回 CommitMessage（几毫秒）
  ├─ Checkpoint 完成：1-2 秒 ✨
  │
  └─ 同时，后台 BE 继续：
     ├─ 监控 Level-0 大小：可能 > 100MB
     ├─ 异步启动 Compaction（不影响上面）
     └─ 后续写入继续，不受 Level-0 堆积影响
```

**结果**：Doris 的 Checkpoint 延迟低 10-100 倍！

---

## 8. 为什么 Paimon 采用这种设计？

### 背景分析

Paimon 的设计目标：
1. **单机 LSM Tree 完整实现**
   - 需要对 Level-0 有严格控制
   - 无法依赖分布式协调

2. **强一致性保证**
   - Checkpoint 是全局同步点
   - 在此时点强制处理堆积较为安全

3. **读写平衡**
   - 避免运行时 Level-0 爆炸导致读取故障
   - 宁可 Checkpoint 慢一点，也要保证稳定

### 设计权衡

```
Paimon 选择：Checkpoint 同步等待压缩
  优点：
    ✅ Level-0 数量有界（不会无限堆积）
    ✅ 读取性能上界可保证
    ✅ 避免"突然故障"（Level-0 爆炸）

  代价：
    ❌ Checkpoint 延迟到分钟级
    ❌ 写入吞吐量波动大
    ❌ 任务可能超时

Doris 选择：Checkpoint 异步处理压缩
  优点：
    ✅ Checkpoint 永远快速返回
    ✅ 写入吞吐量稳定
    ✅ 任务延迟稳定

  代价：
    ❌ Level-0 可能临时堆积
    ❌ 需要依赖分布式协调
    ❌ 需要更复杂的故障恢复
```

---

## 9. 结论

### 这不是技术优劣，而是架构哲学不同

| 哲学 | Paimon | Doris |
|------|--------|-------|
| **设计目标** | 强一致的本地 LSM Tree | 分布式高性能数据库 |
| **压缩策略** | "即时"处理 | "延迟"处理 |
| **故障方式** | 清晰（会延迟，但稳定） | 隐式（正常情况下不会感知） |
| **适用场景** | 数据湖导出、长期存储 | 实时 OLAP、频繁更新 |

### 能否改进？

**Paimon 可以考虑的优化**：

1. **参数调优**
   ```properties
   # 增大触发阈值，减少强制等待
   core.write.sort-spill.file-num.stop-trigger=8   # 从 4 改为 8

   # 减少一次压缩的文件数
   core.write.sort-spill.max-size-amp=300          # 从 200 增加到 300
   ```

2. **并行化压缩**
   ```properties
   # 增加压缩线程数
   table.compaction-threads=16                     # 增加并发
   ```

3. **异步提交**
   ```
   考虑在 Checkpoint 时不等待压缩，
   而是将压缩任务作为独立的后台任务
   （需要重新设计 Manifest 提交机制）
   ```

**Doris 已经做对的事**：
- ✅ Checkpoint 与压缩完全异步
- ✅ 分布式负载分散
- ✅ 压力自动均衡
