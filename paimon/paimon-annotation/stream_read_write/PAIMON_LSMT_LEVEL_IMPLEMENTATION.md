# Paimon LSM Tree 层级控制（Level）实现详细总结

> **📌 文档说明**
> 本文档详细介绍 Apache Paimon 中 LSM Tree 的层级控制实现。通过此文档，您可以快速了解 Paimon 的数据组织结构和压缩机制。

---

## 📑 目录（快速导航）

- [一、核心源码文件位置](#一核心源码文件位置)
  - [1. 层级管理核心模块](#1-层级管理核心模块)
  - [2. 压缩策略模块](#2-压缩策略模块)
  - [3. 压缩执行模块](#3-压缩执行模块)
  - [4. 文件元数据模块](#4-文件元数据模块)
  - [5. 写入和存储模块](#5-写入和存储模块)

- [二、LSM Tree 层级结构详解](#二lsm-tree-层级结构详解)
  - [2.1 层级组织方式](#21-层级组织方式)
  - [2.2 Level-0 文件排序规则](#22-level-0-文件排序规则)
  - [2.3 Levels 类核心方法](#23-levels-类核心方法)

- [三、通用压缩策略（UniversalCompaction）详解](#三通用压缩策略universalcompaction详解)
  - [3.1 三种压缩触发条件](#31-三种压缩触发条件)
  - [3.2 触发条件详解](#32-触发条件详解)
    - [（1）空间放大](#1空间放大pickforsizeamp)
    - [（2）大小比例](#2大小比例pickforsiseratio)
    - [（3）文件数量](#3文件数量numruncompactiontrigger)
    - [（4）非峰值时段调整](#4非峰值时段调整)

- [四、提前全量压缩（EarlyFullCompaction）](#四提前全量压缩earlyfullcompaction)
  - [4.1 三种触发条件](#41-三种触发条件)
  - [4.2 配置示例](#42-配置示例)

- [五、压缩任务执行（MergeTreeCompactTask）](#五压缩任务执行mergetreecompacttask)
  - [5.1 智能文件升级策略](#51-智能文件升级策略)
  - [5.2 区间分区算法（IntervalPartition）](#52-区间分区算法intervalpartition)

- [六、文件元数据中的层级信息](#六文件元数据中的层级信息)
  - [6.1 DataFileMeta 中的层级字段](#61-datafilemeta-中的层级字段)
  - [6.2 DataFileMeta 的核心字段](#62-datafilemeta-的核心字段)

- [七、写入流程中的层级控制](#七写入流程中的层级控制)
  - [7.1 MergeTreeWriter 的文件跟踪](#71-mergetreewriter-的文件跟踪)
  - [7.2 文件生命周期](#72-文件生命周期)

- [八、核心配置参数](#八核心配置参数)

- [九、数据流向总结](#九数据流向总结)

- [十、关键设计决策](#十关键设计决策)

- [十一、重要类关系图](#十一重要类关系图)

- [十二、参考资源](#十二参考资源)

---

## 一、核心源码文件位置

### 1. 层级管理核心模块

| 源码文件 | 关键类/接口 | 主要职责 | 源码位置 |
|---------|----------|--------|--------|
| `Levels.java` | `Levels` | LSM Tree 所有层级的管理中枢，管理 Level-0 和 Level-1~N 的文件组织 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/Levels.java) |
| `LevelSortedRun.java` | `LevelSortedRun` | 为 SortedRun 添加层级标号，用于压缩策略的决策 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/LevelSortedRun.java) |
| `SortedRun.java` | `SortedRun` | 代表一个有序的文件运行集合，键范围不重叠 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/SortedRun.java) |

### 2. 压缩策略模块

| 源码文件 | 关键类/接口 | 主要职责 | 源码位置 |
|---------|----------|--------|--------|
| `CompactStrategy.java` | `CompactStrategy` | 压缩策略接口，定义选择压缩单元的核心逻辑 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/CompactStrategy.java) |
| `UniversalCompaction.java` | `UniversalCompaction` | 通用压缩策略实现，支持 3 种触发条件（空间放大、大小比例、文件数量） | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/UniversalCompaction.java) |
| `EarlyFullCompaction.java` | `EarlyFullCompaction` | 提前全量压缩策略，基于时间、总大小或增量大小触发 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/EarlyFullCompaction.java) |

### 3. 压缩执行模块

| 源码文件 | 关键类 | 主要职责 | 源码位置 |
|---------|-------|--------|--------|
| `MergeTreeCompactTask.java` | `MergeTreeCompactTask` | 执行具体的压缩操作，包括区间分区和文件升级 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactTask.java) |
| `MergeTreeCompactManager.java` | `MergeTreeCompactManager` | 管理压缩任务的生命周期 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManager.java) |
| `IntervalPartition.java` | `IntervalPartition` | 区间分区算法，将文件分割为键范围不重叠的分区 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/IntervalPartition.java) |

### 4. 文件元数据模块

| 源码文件 | 关键类/接口 | 主要职责 | 源码位置 |
|---------|----------|--------|--------|
| `DataFileMeta.java` | `DataFileMeta` | 数据文件元数据接口，包含文件的层级号和所有统计信息 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java) |
| `ManifestEntry.java` | `ManifestEntry` | Manifest 条目，记录文件的元数据变更并包含层级信息 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/manifest/ManifestEntry.java) |

### 5. 写入和存储模块

| 源码文件 | 关键类 | 主要职责 | 源码位置 |
|---------|-------|--------|--------|
| `MergeTreeWriter.java` | `MergeTreeWriter` | MergeTree 写入器，管理缓冲区、Level-0 文件生成 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java) |
| `FileStore.java` | `FileStore` | 文件存储接口 | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/FileStore.java) |
| `KeyValueFileStore.java` | `KeyValueFileStore` | KeyValue 表存储实现，基于 MergeTree | [点击查看](../../paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java) |

[⬆ 返回顶部](#目录快速导航)

---

## 二、LSM Tree 层级结构详解

### 2.1 层级组织方式

```
Levels（层级管理器）
├─ Level-0（特殊层级，大小为 TreeSet）
│  ├─ 特点：文件按序列号降序排列（最新的在前）
│  ├─ 键可能重叠
│  └─ 一个文件对应一个 SortedRun
│
├─ Level-1~N（常规层级，ArrayList<SortedRun>）
│  ├─ 特点：每层一个 SortedRun，键区间不重叠
│  ├─ 文件按键值排序
│  └─ 数据相对稳定
│
└─ LevelSortedRun（带层级的 SortedRun）
   ├─ level：层级号（0、1、2...）
   └─ run：SortedRun 对象
```

### 2.2 Level-0 文件排序规则（优先级从高到低）

1. **maxSequenceNumber**：序列号越大越靠前（最新数据优先）
2. **minSequenceNumber**：最小序列号升序
3. **creationTime**：创建时间升序
4. **fileName**：文件名升序（保证唯一性）

```java
// Levels.java 中的排序实现
TreeSet<DataFileMeta> level0 = new TreeSet<>((a, b) -> {
    if (a.maxSequenceNumber() != b.maxSequenceNumber()) {
        // 第一级：按最大序列号降序
        return Long.compare(b.maxSequenceNumber(), a.maxSequenceNumber());
    } else if (a.minSequenceNumber() != b.minSequenceNumber()) {
        // 第二级：按最小序列号升序
        return Long.compare(a.minSequenceNumber(), b.minSequenceNumber());
    } else if (timeCompare != 0) {
        // 第三级：按创建时间升序
        return a.creationTime().compareTo(b.creationTime());
    } else {
        // 第四级：按文件名升序
        return a.fileName().compareTo(b.fileName());
    }
});
```

### 2.3 Levels 类核心方法

| 方法 | 职责 | 关键用途 |
|------|------|--------|
| `addLevel0File(DataFileMeta)` | 添加文件到 Level-0 | 写入时新文件添加 |
| `runOfLevel(int level)` | 获取指定层级的 SortedRun | 获取特定层级的文件 |
| `numberOfSortedRuns()` | 获取 SortedRun 总数 | 判断是否需要压缩 |
| `nonEmptyHighestLevel()` | 获取最高非空层级 | 压缩策略决策 |
| `levelSortedRuns()` | 获取所有层级的 LevelSortedRun | 压缩、读取时的核心数据结构 |
| `update(before, after)` | 更新层级（压缩后调用） | 压缩完成后更新 LSM Tree 状态 |

**详细说明**: [查看 Levels.java 源码](../../paimon-core/src/main/java/org/apache/paimon/mergetree/Levels.java)

[⬆ 返回顶部](#目录快速导航)

---

## 三、通用压缩策略（UniversalCompaction）详解

### 3.1 三种压缩触发条件

```
UniversalCompaction 压缩策略

优先级顺序（按优先级依次检查）：
1️⃣ EarlyFullCompaction（提前全量压缩）
   ↓ [检查失败]
2️⃣ Space Amplification（空间放大检查）
   ↓ [检查失败]
3️⃣ Size Ratio（大小比例检查）
   ↓ [检查失败]
4️⃣ File Num（文件数量检查）
   ↓ [检查失败]
❌ 不需要压缩，等待下一个检查周期
```

### 3.2 触发条件详解

#### （1）空间放大（pickForSizeAmp）

**源码**: [UniversalCompaction.java - pickForSizeAmp() 方法](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/UniversalCompaction.java)

```
公式：candidateSize * 100 > maxSizeAmp * earliestRunSize

参数：
- maxSizeAmp：最大空间放大百分比（例如 200 表示 200%）
- earliestRunSize：最早文件的大小（字节）
- candidateSize：除最早文件外的所有文件总大小

示例：
  maxSizeAmp = 200（允许 200% 额外空间）
  earliestRunSize = 100MB
  candidateSize = 250MB
  → 250 * 100 > 200 * 100 = 25000 > 20000 ✓ 触发全量压缩

触发时的行为：
- 选择所有现有 Run（全量压缩）
- 输出到 maxLevel（最高层级）
- 目标：防止空间浪费
```

#### （2）大小比例（pickForSizeRatio）

**源码**: [UniversalCompaction.java - pickForSizeRatio() 方法](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/UniversalCompaction.java)

```
算法流程：
1. 初始候选文件数：candidateCount（通常为 1）
2. 累计候选大小：candidateSize
3. 遍历后续文件：
   if (candidateSize * (100 + sizeRatio + offPeakRatio) / 100 < nextFileSize)
       break  // 下一个文件太大，停止

示例（sizeRatio = 1%）：
  文件大小：[10MB, 11MB, 50MB, 100MB]

  步骤1：candidateSize = 10MB, count = 1
  步骤2：10 * 1.01 = 10.1 < 11 ✓ 加入 → candidateSize = 21MB
  步骤3：21 * 1.01 = 21.21 < 50 ✓ 加入 → candidateSize = 71MB
  步骤4：71 * 1.01 = 71.71 < 100 ✓ 加入 → candidateSize = 171MB
  结果：压缩所有 4 个文件

触发时的行为：
- 如果候选文件数 > 1，创建压缩单元
- 输出层级：下一个未压缩文件的层级 - 1
- 避免输出到 Level-0
```

#### （3）文件数量（numRunCompactionTrigger）

```
触发条件：
  if (runs.size() > numRunCompactionTrigger)
      → 强制压缩

示例：
  numRunCompactionTrigger = 4
  当前文件数 = 7
  → candidateCount = 7 - 4 + 1 = 4
  → 选择最新的 4 个文件进行压缩
```

#### （4）非峰值时段调整

**源码**: [OffPeakHours.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/OffPeakHours.java)

```
在非峰值时段（如夜间），可以增加 sizeRatio 以更激进地压缩：

if (isOffPeakHour()) {
    sizeRatio += offPeakRatio  // 例如增加 10%
}

公式变为：
  candidateSize * (100 + sizeRatio + offPeakRatio) / 100 < nextFileSize
```

**详细说明**: [查看 UniversalCompaction.java 源码](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/UniversalCompaction.java)

[⬆ 返回顶部](#目录快速导航)

---

## 四、提前全量压缩（EarlyFullCompaction）

### 4.1 三种触发条件

**源码**: [EarlyFullCompaction.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/EarlyFullCompaction.java)

```
触发条件（满足任意一个）：

1. 时间间隔
   ├─ 距离上次全量压缩超过 fullCompactionInterval
   ├─ 示例：1 小时进行一次全量压缩
   └─ 场景：定期保证 changelog 质量

2. 总大小阈值
   ├─ 所有文件总大小 < totalSizeThreshold
   ├─ 示例：100MB 以下立即合并
   └─ 场景：小数据集快速合并到最高层级

3. 增量大小阈值
   ├─ 非最高层级文件 > incrementalSizeThreshold
   ├─ 示例：增量数据超过 1GB 触发
   └─ 场景：防止低层级文件累积过多
```

### 4.2 配置示例

```properties
# 配置示例
compaction.optimization-interval = 1h        # 每小时全量压缩
compaction.total-size-threshold = 100MB      # 小于100MB立即合并
compaction.incremental-size-threshold = 1GB  # 增量超过1GB触发
```

[⬆ 返回顶部](#目录快速导航)

---

## 五、压缩任务执行（MergeTreeCompactTask）

### 5.1 智能文件升级策略

**源码**: [MergeTreeCompactTask.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactTask.java)

```
工作流程：
1. 区间分区：将文件划分为不重叠的键范围段
2. 遍历每个段（Section）：
   a. 多文件段：加入候选队列进行归并压缩
   b. 单文件段：
      - 大文件（≥minFileSize）：直接升级层级（避免重写）✓
      - 小文件（<minFileSize）：加入压缩队列 ✓

示例（minFileSize = 5MB）：
  输入文件：[A(10MB), B(1MB), C(2MB), D(15MB), E(1MB)]

  区间分割结果：
  Section1[A]         → 10MB ≥ 5MB → 直接升级 ✓
  Section2[B, C]      → 多文件 → 归并压缩 ✓
  Section3[D]         → 15MB ≥ 5MB → 直接升级 ✓
  Section4[E]         → 1MB < 5MB → 加入压缩队列 ✓

关键特性：
✓ 避免重写大文件，只改层级
✓ 合并小文件，减少文件数量
✓ 平衡 I/O 成本和性能
```

### 5.2 区间分区算法（IntervalPartition）

**源码**: [IntervalPartition.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/IntervalPartition.java)

```
核心思想：
1. 对文件按 minKey 和 maxKey 排序
2. 将键范围不重叠的文件分组为 Section
3. 在 Section 内部最小化 SortedRun 数量

使用场景：
- 减少同时处理的文件数
- 提高压缩和读取性能
- 减少文件句柄占用
```

[⬆ 返回顶部](#目录快速导航)

---

## 六、文件元数据中的层级信息

### 6.1 DataFileMeta 中的层级字段

**源码**: [DataFileMeta.java](../../paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java)

```java
new DataField(10, "_LEVEL", new IntType(false))  // LSM 层级【核心字段】
```

**说明**:
- 索引: 10
- 类型: int（不可为 null）
- 含义: 文件在 LSM Tree 中的层级号
  - 0 = Level-0（最新，键可能重叠）
  - 1+ = Level-1~N（较旧，键不重叠）

### 6.2 DataFileMeta 的核心字段

| 字段 | 类型 | 用途 | 来源 |
|------|------|------|------|
| `level()` | int | **LSM 层级号** | 写入或压缩时确定 |
| `fileName()` | String | 文件唯一标识 | 文件写入时生成 |
| `fileSize()` | long | 文件大小（字节） | 影响压缩策略 |
| `rowCount()` | long | 文件中的行数 | 数据量统计 |
| `minKey()` | BinaryRow | 最小键 | 范围查询优化 |
| `maxKey()` | BinaryRow | 最大键 | 范围查询优化 |
| `minSequenceNumber()` | long | 最小序列号 | 版本控制 |
| `maxSequenceNumber()` | long | 最大序列号 | Level-0 排序基准 |
| `creationTime()` | LocalDateTime | 创建时间 | 数据过期策略 |
| `keyStats()` | SimpleStats | 键统计信息 | 谓词下推优化 |
| `valueStats()` | SimpleStats | 值统计信息 | 查询规划 |
| `deleteRowCount()` | long | 删除行数（可选） | 评估是否需要重写 |

**详细字段说明**: [查看 DataFileMeta.java 源码](../../paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java)

[⬆ 返回顶部](#目录快速导航)

---

## 七、写入流程中的层级控制

### 7.1 MergeTreeWriter 的文件跟踪

**源码**: [MergeTreeWriter.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java)

```java
// 新生成的数据文件（刷新 WriteBuffer 产生的 Level-0 文件）
LinkedHashSet<DataFileMeta> newFiles;

// 压缩前后的文件跟踪
LinkedHashMap<String, DataFileMeta> compactBefore;   // 压缩前的文件
LinkedHashSet<DataFileMeta> compactAfter;            // 压缩后的文件
LinkedHashSet<DataFileMeta> compactChangelog;        // 压缩生成的 changelog 文件
```

### 7.2 文件生命周期

```
┌──────────────────────────────────────────┐
│ 写入阶段                                  │
├──────────────────────────────────────────┤
│ 1. 数据写入 WriteBuffer（内存）           │
│ 2. 缓冲区满时刷新                         │
│ 3. 生成 Level-0 文件（level = 0）        │
│ 4. 添加到 newFiles 集合                  │
│ 5. 添加到 Levels.level0()                │
├──────────────────────────────────────────┤
│ 压缩触发                                  │
├──────────────────────────────────────────┤
│ 1. CompactManager 定期检测压缩条件       │
│ 2. CompactStrategy 做出压缩决策           │
│ 3. 选择文件进行压缩（→ compactBefore）   │
├──────────────────────────────────────────┤
│ 压缩执行                                  │
├──────────────────────────────────────────┤
│ 1. MergeTreeCompactTask 执行压缩         │
│ 2. IntervalPartition 分割文件            │
│ 3. 大文件直接升级层级                    │
│ 4. 小文件进行归并压缩                    │
│ 5. 生成新的高层级文件                    │
│ 6. 添加到 compactAfter 集合              │
├──────────────────────────────────────────┤
│ 层级更新                                  │
├──────────────────────────────────────────┤
│ 1. 调用 Levels.update(before, after)    │
│ 2. 删除压缩前的文件                      │
│ 3. 添加压缩后的文件（更高层级）         │
│ 4. 更新 Manifest 记录                   │
│ 5. 清空状态，准备下一轮                 │
└──────────────────────────────────────────┘
```

[⬆ 返回顶部](#目录快速导航)

---

## 八、核心配置参数

**源码**: [CoreOptions.java](../../paimon-api/src/main/java/org/apache/paimon/CoreOptions.java)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `compaction.max-size-amplification-percent` | 200 | 最大空间放大百分比 |
| `compaction.size-ratio` | 1 | 大小比例阈值（%） |
| `compaction.num-run-compaction-trigger` | 4 | 触发压缩的文件数量 |
| `compaction.optimization-interval` | null | 提前全量压缩的时间间隔 |
| `compaction.total-size-threshold` | null | 全量压缩的总大小阈值 |
| `num-levels` | 2 | **层级总数** |
| `write-buffer-size` | 256MB | 写入缓冲区大小 |

[⬆ 返回顶部](#目录快速导航)

---

## 九、数据流向总结

```
                    🔄 完整的 LSM Tree 工作流程

┌──────────────────────────────────────────────────────────────┐
│ 📝 数据写入                                                   │
├──────────────────────────────────────────────────────────────┤
│ 1️⃣ 数据通过 MergeTreeWriter 写入                             │
│ 2️⃣ 数据先进入 WriteBuffer（内存排序）                        │
│ 3️⃣ WriteBuffer 满时刷新到磁盘                                │
│ 4️⃣ 生成 Level-0 文件（level = 0）                           │
│ 5️⃣ 添加到 Levels.level0()                                   │
└──────────────────────────────────────────────────────────────┘
                           ⬇️
┌──────────────────────────────────────────────────────────────┐
│ 🎯 压缩触发                                                   │
├──────────────────────────────────────────────────────────────┤
│ 1️⃣ CompactManager 定期检测压缩条件                           │
│ 2️⃣ UniversalCompaction 做出压缩决策                         │
│    └─ 空间放大检查 | 大小比例检查 | 文件数量检查             │
│    └─ 提前全量压缩检查                                       │
└──────────────────────────────────────────────────────────────┘
                           ⬇️
┌──────────────────────────────────────────────────────────────┐
│ ⚡ 压缩执行                                                   │
├──────────────────────────────────────────────────────────────┤
│ 1️⃣ MergeTreeCompactTask 执行压缩                            │
│ 2️⃣ IntervalPartition 分割文件                               │
│ 3️⃣ 大文件直接升级层级（避免重写）                           │
│ 4️⃣ 小文件进行 k-way merge 压缩                              │
│ 5️⃣ 生成新的高层级文件                                       │
└──────────────────────────────────────────────────────────────┘
                           ⬇️
┌──────────────────────────────────────────────────────────────┐
│ 🔄 层级更新                                                   │
├──────────────────────────────────────────────────────────────┤
│ 1️⃣ 调用 Levels.update()                                     │
│ 2️⃣ 按层级分组删除压缩前的文件                                │
│ 3️⃣ 按层级分组添加压缩后的文件（更高层级）                   │
│ 4️⃣ 更新 Manifest 记录（持久化）                             │
└──────────────────────────────────────────────────────────────┘
```

[⬆ 返回顶部](#目录快速导航)

---

## 十、关键设计决策

| 决策 | 说明 | 优势 |
|------|------|------|
| **Level-0 的特殊设计** | 使用 TreeSet 自动排序，按序列号降序保证最新数据优先读取 | 写入高效，读取时能优先获取最新数据 |
| **智能文件升级** | 避免小文件重写，大文件直接升级 | 平衡 I/O 成本和性能 |
| **区间分区** | 将文件按键范围分组 | 减少同时处理的文件数 |
| **多层压缩策略** | 空间放大、大小比例、文件数量多维度控制 | 灵活适应不同场景 |
| **非峰值时段优化** | 夜间可以更激进地压缩 | 提高整体性能，白天性能稳定 |

[⬆ 返回顶部](#目录快速导航)

---

## 十一、重要类关系图

```
┌────────────────────────────────────────────────────────┐
│  Levels（层级管理器）                                  │
│  ├─ TreeSet<DataFileMeta> level0                      │
│  ├─ List<SortedRun> levels                            │
│  └─ List<DropFileCallback> callbacks                  │
└────────────┬─────────────────────────────────────────┘
             │
             ├──→ LevelSortedRun
             │      ├─ int level
             │      └─ SortedRun run
             │
             └──→ SortedRun
                    └─ List<DataFileMeta> files

┌────────────────────────────────────────────────────────┐
│  CompactStrategy（压缩策略接口）                      │
│  ├─ pick() → Optional<CompactUnit>                    │
│  └─ pickFullCompaction() → Optional<CompactUnit>      │
└────────────┬─────────────────────────────────────────┘
             │
             └──→ UniversalCompaction
                    ├─ pickForSizeAmp()
                    ├─ pickForSizeRatio()
                    └─ EarlyFullCompaction（可选）

┌────────────────────────────────────────────────────────┐
│  CompactUnit（压缩单元）                              │
│  ├─ int outputLevel       ← 输出层级                  │
│  ├─ List<DataFileMeta> files                          │
│  └─ boolean rewriteAllFiles                           │
└────────────┬─────────────────────────────────────────┘
             │
             └──→ MergeTreeCompactTask（执行）
                    ├─ IntervalPartition
                    └─ 文件升级和归并逻辑

┌────────────────────────────────────────────────────────┐
│  DataFileMeta（文件元数据）                           │
│  ├─ int level()          ← 【核心字段】              │
│  ├─ String fileName()                                  │
│  ├─ long fileSize()                                    │
│  ├─ BinaryRow minKey()                                 │
│  ├─ BinaryRow maxKey()                                 │
│  ├─ long minSequenceNumber()                           │
│  └─ long maxSequenceNumber()                           │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│  MergeTreeWriter（写入器）                             │
│  ├─ LinkedHashSet<DataFileMeta> newFiles             │
│  ├─ LinkedHashMap<String, DataFileMeta> compactBefore│
│  ├─ LinkedHashSet<DataFileMeta> compactAfter         │
│  └─ WriteBuffer writeBuffer                           │
└────────────────────────────────────────────────────────┘
```

[⬆ 返回顶部](#目录快速导航)

---

## 十二、参考资源

### 官方文档和资源

- 📚 [RocksDB Universal Compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction)
- 📚 [Paimon 官方文档](https://paimon.apache.org/)
- 📚 [LSM Tree 论文参考](https://www.cs.umb.edu/~poneil/lsmtree.pdf)

### 源码浏览快速链接

| 功能 | 源码位置 |
|------|--------|
| 层级管理 | [Levels.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/Levels.java) |
| 压缩策略 | [CompactStrategy.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/CompactStrategy.java) |
| 通用压缩 | [UniversalCompaction.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/UniversalCompaction.java) |
| 提前全量压缩 | [EarlyFullCompaction.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/EarlyFullCompaction.java) |
| 压缩执行 | [MergeTreeCompactTask.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactTask.java) |
| 区间分区 | [IntervalPartition.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/compact/IntervalPartition.java) |
| 文件元数据 | [DataFileMeta.java](../../paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java) |
| 写入器 | [MergeTreeWriter.java](../../paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java) |
| 核心配置 | [CoreOptions.java](../../paimon-api/src/main/java/org/apache/paimon/CoreOptions.java) |

[⬆ 返回顶部](#目录快速导航)

---

## 📖 快速开始指南

### 如果您想...

| 目标 | 推荐阅读 |
|------|--------|
| ⚡ 快速了解 Paimon LSM Tree 结构 | [第二章: LSM Tree 层级结构详解](#二lsm-tree-层级结构详解) |
| 🎯 理解压缩策略如何选择文件 | [第三章: 通用压缩策略详解](#三通用压缩策略universalcompaction详解) |
| ⚙️ 学习压缩执行细节 | [第五章: 压缩任务执行](#五压缩任务执行mergetreecompacttask) |
| 📊 理解文件元数据 | [第六章: 文件元数据中的层级信息](#六文件元数据中的层级信息) + [DataFileMeta.java](../../paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java) |
| 🔄 看完整数据流向 | [第九章: 数据流向总结](#九数据流向总结) |
| 📈 查看类关系和架构 | [第十一章: 重要类关系图](#十一重要类关系图) |

---

**生成时间**: 2026-02-13
**Paimon 版本**: 主分支
**文档作者**: Claude Code

[⬆ 返回顶部](#目录快速导航)
