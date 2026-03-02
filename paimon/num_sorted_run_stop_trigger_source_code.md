# Paimon `num-sorted-run.stop-trigger` 参数源码追踪

## 1. 参数定义位置

### 📍 位置 1：CoreOptions.java - 参数声明

**文件**：`paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`

**行号**：1004-1013

```java
/**
 * 排序运行停止触发配置选项。指定触发停止写入的排序运行数量（默认为压缩触发数+3）。
 */
public static final ConfigOption<Integer> NUM_SORTED_RUNS_STOP_TRIGGER =
        key("num-sorted-run.stop-trigger")
                .intType()
                .noDefaultValue()
                .withDescription(
                        "The number of sorted runs that trigger the stopping of writes,"
                                + " the default value is 'num-sorted-run.compaction-trigger' + 3.");
```

**关键信息**：
- 配置键名：`num-sorted-run.stop-trigger`
- 类型：Integer
- 默认值：**noDefaultValue()** → 如果不设置，使用 `num-sorted-run.compaction-trigger + 3`
  - `num-sorted-run.compaction-trigger` 默认为 5
  - 所以 `num-sorted-run.stop-trigger` 默认为 8

---

## 2. 参数的 getter 方法

### 📍 位置 2：CoreOptions.java - 获取参数值

**文件**：`paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`

**行号**：3536-3542

```java
public int numSortedRunStopTrigger() {
    Integer stopTrigger = options.get(NUM_SORTED_RUNS_STOP_TRIGGER);
    if (stopTrigger == null) {
        // 如果用户没有设置，自动计算为：压缩触发阈值 + 3
        stopTrigger = MathUtils.addSafely(numSortedRunCompactionTrigger(), 3);
    }
    return Math.max(numSortedRunCompactionTrigger(), stopTrigger);
}
```

**执行逻辑**：
1. 尝试从配置中获取 `NUM_SORTED_RUNS_STOP_TRIGGER`
2. 如果为 null（用户未设置），计算为：`numSortedRunCompactionTrigger() + 3`
3. 返回 `max(numSortedRunCompactionTrigger(), stopTrigger)`

---

## 3. 参数的使用位置

### 📍 位置 3：MergeTreeCompactManager.java - 构造函数接收参数

**文件**：`paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManager.java`

**行号**：130-145

```java
public MergeTreeCompactManager(
        ExecutorService executor,
        Levels levels,
        CompactStrategy strategy,
        Comparator<InternalRow> keyComparator,
        long compactionFileSize,
        int numSortedRunStopTrigger,  // ⚠️ 参数传入
        CompactRewriter rewriter,
        // ... 其他参数
        ) {
    // ...
    this.numSortedRunStopTrigger = numSortedRunStopTrigger;  // 保存为成员变量
    // ...
}
```

### 📍 位置 4：MergeTreeCompactManager.java - 在阈值检查中使用

**文件**：`paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManager.java`

**行号**：181-183

```java
@Override
public boolean shouldWaitForPreparingCheckpoint() {
    // cast to long to avoid Numeric overflow
    return levels.numberOfSortedRuns() > (long) numSortedRunStopTrigger + 1;
}
```

**关键逻辑**：
- 如果 `SortedRun数 > (numSortedRunStopTrigger + 1)`，则 Checkpoint 时强制等待压缩
- 默认值 8，所以当 `SortedRun数 > 9` 时会触发强制等待

---

## 4. 参数传递链路

完整的参数流向：

```
┌─────────────────────────────────────────────────────────────┐
│  用户配置文件 (flink-conf.yaml 或 Catalog 配置)            │
│                                                             │
│  num-sorted-run.stop-trigger: 10                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │   CoreOptions.java          │
        │   NUM_SORTED_RUNS_STOP_...  │
        │   numSortedRunStopTrigger() │
        └─────────────┬───────────────┘
                      │ 读取配置
                      ▼
        ┌─────────────────────────────────────────┐
        │   KeyValueFileStore.newWrite()          │
        │   (或其他 FileStore 实现)                │
        │   获取 coreOptions.numSortedRunStopTrigger() │
        └─────────────┬───────────────────────────┘
                      │ 作为参数传递
                      ▼
        ┌─────────────────────────────────────────┐
        │   MergeTreeCompactManager 构造函数      │
        │   MergeTreeCompactManager(               │
        │     ...,                                │
        │     numSortedRunStopTrigger,   ◀─────────────┐
        │     ...                                │
        │   )                                    │
        └─────────────┬───────────────────────────┘
                      │ 保存为成员变量
                      ▼
        ┌──────────────────────────────────────────┐
        │   shouldWaitForPreparingCheckpoint()    │
        │   在 MergeTreeWriter.prepareCommit() 中 │
        │   使用该参数进行判断                    │
        │                                         │
        │   if (numberOfSortedRuns >              │
        │       numSortedRunStopTrigger + 1)      │
        │   {                                     │
        │       waitCompaction = true;     ◀─────────┐ 强制等待压缩
        │   }                                     │
        └──────────────────────────────────────────┘
```

---

## 5. 相关参数的完整列表

| 参数名称 | 配置键 | 默认值 | 位置 | 作用 |
|---------|--------|--------|------|------|
| **NUM_SORTED_RUNS_COMPACTION_TRIGGER** | `num-sorted-run.compaction-trigger` | 5 | CoreOptions:996 | 触发压缩的阈值 |
| **NUM_SORTED_RUNS_STOP_TRIGGER** | `num-sorted-run.stop-trigger` | 无（5+3=8） | CoreOptions:1007 | 触发强制等待的阈值 |
| **NUM_LEVELS** | `num-levels` | 无（自动计算） | CoreOptions:1018 | LSM Tree 层级数 |
| **COMMIT_FORCE_COMPACT** | `commit.force-compact` | false | CoreOptions:1028 | 提交时是否强制压缩 |

---

## 6. 配置方式举例

### 在 Flink 中配置

#### 方式 1：flink-conf.yaml

```yaml
# 触发压缩的文件数
num-sorted-run.compaction-trigger: 5

# 触发停止写入的文件数（强制等待压缩）
num-sorted-run.stop-trigger: 10

# 提交时是否强制压缩
commit.force-compact: false
```

#### 方式 2：WITH 子句（SQL）

```sql
CREATE TABLE my_paimon_table (
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'path' = '/data/my_table',
    'bucket' = '4',
    'num-sorted-run.compaction-trigger' = '5',
    'num-sorted-run.stop-trigger' = '10',
    'commit.force-compact' = 'false'
);
```

#### 方式 3：Java API

```java
Map<String, String> options = new HashMap<>();
options.put("num-sorted-run.compaction-trigger", "5");
options.put("num-sorted-run.stop-trigger", "10");
options.put("commit.force-compact", "false");

Table table = tEnv.fromPath("my_paimon_table");
```

---

## 7. 参数工作流程详解

```
Checkpoint 发生时的检查流程：
                      │
                      ▼
        ┌─────────────────────────────┐
        │  MergeTreeWriter            │
        │  .prepareCommit(W)          │
        │  (第 621 行)                │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  flushWriteBuffer(W, false) │
        │  (第 623 行)                │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌──────────────────────────────────────────┐
        │  if (commitForceCompact)                 │
        │      waitCompaction = true;              │
        │  (第 626-627 行)                        │
        └─────────────┬────────────────────────────┘
                      │
                      ▼
        ┌──────────────────────────────────────────┐
        │  if (shouldWaitForPreparingCheckpoint()) │
        │  (第 634 行)                            │
        │                                          │
        │  ◀─────────────────────────────────────────────┐
        │  ┌──────────────────────────────────┐         │
        │  │ MergeTreeCompactManager          │         │
        │  │ .shouldWaitForPreparingCheckpoint│         │
        │  │ (第 181-183 行)                  │         │
        │  │                                  │         │
        │  │ return numberOfSortedRuns >     │         │
        │  │        numSortedRunStopTrigger  │         │
        │  │        + 1                       │         │ 使用参数！
        │  └──────────────────────────────────┘         │
        │  ◀─────────────────────────────────────────────┘
        │      YES ─→ waitCompaction = true;  │
        │  (第 635 行)                        │
        └─────────────┬──────────────────────┘
                      │
                      ▼
        ┌────────────────────────────────────┐
        │  trySyncLatestCompaction(W)        │
        │  (第 639 行)                      │
        │                                   │
        │  ⏸️ 如果 W=true 则阻塞等待          │
        │     getCompactionResult(true)     │
        │     → Future.get()                │
        └────────────────────────────────────┘
```

---

## 8. 源码调用链总结

| 步骤 | 文件 | 行号 | 方法 | 说明 |
|------|------|------|------|------|
| 1 | CoreOptions.java | 1007 | 参数定义 | 声明 `NUM_SORTED_RUNS_STOP_TRIGGER` 配置 |
| 2 | CoreOptions.java | 3536 | numSortedRunStopTrigger() | 读取配置或计算默认值 |
| 3 | KeyValueFileStoreWrite.java | ? | newWrite() | 从 coreOptions 读取参数 |
| 4 | MergeTreeCompactManager.java | 145 | 构造函数 | 接收参数并保存 |
| 5 | MergeTreeCompactManager.java | 181 | shouldWaitForPreparingCheckpoint() | 使用参数进行判断 |
| 6 | MergeTreeWriter.java | 634 | prepareCommit() | 在 Checkpoint 时调用上述方法 |

---

## 9. 参数影响的具体行为

```
numSortedRunStopTrigger 的值对应的行为：

┌─────────┬─────────────────────────────┬──────────────────┐
│ 参数值  │ 触发强制等待的条件          │ Checkpoint 影响  │
├─────────┼─────────────────────────────┼──────────────────┤
│ 4       │ SortedRun > 5               │ 非常频繁触发等待 │
│ 8       │ SortedRun > 9               │ 频繁触发等待     │
│ 10      │ SortedRun > 11              │ 中等频率等待     │
│ 20      │ SortedRun > 21              │ 很少触发等待     │
│ 999     │ SortedRun > 1000            │ 基本不触发等待   │
└─────────┴─────────────────────────────┴──────────────────┘

默认行为：
  num-sorted-run.compaction-trigger = 5
  num-sorted-run.stop-trigger = 8 (auto = 5 + 3)
  触发强制等待：SortedRun > 9
```

---

## 10. 完整的配置调优建议

```properties
# ===== 默认配置 =====
num-sorted-run.compaction-trigger=5          # 触发压缩的阈值
num-sorted-run.stop-trigger=8                # 触发强制等待的阈值（5+3）

# ===== 减少 Checkpoint 延迟的调优 =====
# 增大 stop-trigger，减少强制等待的频率
num-sorted-run.stop-trigger=12               # 改为 12（5+7）
# 或者增大 compaction-trigger
num-sorted-run.compaction-trigger=8          # 改为 8
num-sorted-run.stop-trigger=11               # 自动变为 8+3=11

# ===== 更激进的压缩 =====
num-sorted-run.compaction-trigger=3          # 降低触发压缩的阈值
num-sorted-run.stop-trigger=6                # 3+3=6
commit.force-compact=true                    # 每次提交都强制压缩

# ===== 几乎无强制等待 =====
num-sorted-run.stop-trigger=100              # 非常大的值
# 缺点：Level-0 可能临时堆积，影响读性能
```

---

## 11. 常见问题

### Q1：参数没有设置会怎样？
**A**：会自动计算为 `num-sorted-run.compaction-trigger + 3`，即 `5 + 3 = 8`

### Q2：参数值设置得越大越好？
**A**：不是。越大意味着：
- ✅ Checkpoint 延迟越少
- ❌ Level-0 可能堆积更多
- ❌ 读取性能可能下降

### Q3：和 num-sorted-run.compaction-trigger 什么关系？
**A**：
- `compaction-trigger=5`：当文件数 > 5 时，触发**异步**压缩
- `stop-trigger=8`：当文件数 > 9 时，Checkpoint **同步等待**压缩完成

### Q4：如何在不修改全局配置的情况下调整这个参数？
**A**：在创建表时通过 WITH 子句指定：
```sql
CREATE TABLE t WITH (
    'num-sorted-run.stop-trigger' = '20'
)
```
