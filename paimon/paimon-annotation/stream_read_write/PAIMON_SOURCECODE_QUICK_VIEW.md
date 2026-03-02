# Paimon 流读流写 - 源码关键位置速览

> 本文档为主分析文档的补充，提供了各章节内容对应的源码位置，便于快速跳转查看。

## 📍 快速索引

### 流写部分

#### 核心类关系 → 源码位置

```
StreamWriteBuilder (接口)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilder.java

StreamWriteBuilderImpl (实现)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java
  ✓ 关键: 行 60-119
  ✓ commitUser 初始化: 行 80
  ✓ newWrite() 方法: 行 110
  ✓ newCommit() 方法: 行 115

StreamTableWrite (接口)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableWrite.java
  ✓ 关键: 行 73-108
  ✓ prepareCommit() 方法: 行 106

TableWriteImpl<T> (实现层)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java
  ✓ 关键: 行 83-144+
  ✓ 架构说明: 行 50-73
  ✓ write() 方法: 搜索 "public void write"
  ✓ prepareCommit() 方法: 搜索 "public List<CommitMessage>"

FileStoreWrite<T> (底层接口)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/operation/FileStoreWrite.java
  ✓ 关键: 行 44-100+
  ✓ 写入流程: 行 47-54
  ✓ 关键方法: 行 70-92
```

#### 初始化阶段 → 源码查看

**涉及的类和方法**:

1. **new StreamWriteBuilderImpl(table)**
   - 文件: `table/sink/StreamWriteBuilderImpl.java`
   - 方法: 构造函数 (行 78)
   - 关键: `this.commitUser = createCommitUser(...)` (行 80)

2. **builder.withCommitUser("job-123")**
   - 文件: `table/sink/StreamWriteBuilderImpl.java`
   - 方法: `withCommitUser()` (行 104)
   - 关键: `this.commitUser = commitUser` (行 105)

3. **write = builder.newWrite()**
   - 文件: `table/sink/StreamWriteBuilderImpl.java`
   - 方法: `newWrite()` (行 110)
   - 跳转: `table.newWrite(commitUser)` → 去 `InnerTable.java` 查看

4. **new TableWriteImpl(...)**
   - 文件: `table/sink/TableWriteImpl.java`
   - 方法: 构造函数 (行 122)
   - 初始化:
     - `keyAndBucketExtractor` (行 131)
     - `recordExtractor` (行 132)
     - `rowKindGenerator` (行 133)
     - `rowKindFilter` (行 134)

5. **FileStoreWrite 初始化**
   - 文件: `operation/FileStoreWrite.java`
   - 关键: 接口定义，具体实现在 `KeyValueFileStoreWrite` 或 `AppendOnlyFileStoreWrite`

#### 数据写入阶段 → 源码查看

1. **write.write(row)**
   - 文件: `table/sink/TableWriteImpl.java`
   - 方法: `public void write(InternalRow row)`
   - 搜索此方法查看实现

2. **keyAndBucketExtractor.extract(row)**
   - 文件: `table/sink/KeyAndBucketExtractor.java`
   - 方法: `KeyAndBucket extract(T row)` (接口)
   - 实现:
     - `FixedBucketRowKeyExtractor.java`
     - `DynamicBucketRowKeyExtractor.java`
     - `PostponeBucketRowKeyExtractor.java`

3. **recordExtractor.toRecord(row)**
   - 文件: `table/sink/RecordExtractor.java`
   - 方法: `T toRecord(SinkRecord record)` (接口)

4. **FileStoreWrite.write(partition, bucket, record)**
   - 文件: `operation/FileStoreWrite.java`
   - 接口方法（具体实现在子类）

5. **RecordWriter.write(record)**
   - 文件: `utils/RecordWriter.java`
   - 方法: `void write(T record)` (接口)

#### Checkpoint 提交阶段 → 源码查看

1. **write.prepareCommit(waitCompaction, checkpointId)**
   - 文件: `table/sink/StreamTableWrite.java`
   - 方法声明: 行 106
   - 文件: `table/sink/TableWriteImpl.java`
   - 实现方法: 搜索 `public List<CommitMessage> prepareCommit`

2. **FileStoreWrite.flush()**
   - 文件: `operation/FileStoreWrite.java`
   - 接口方法

3. **FileStoreWrite.waitForCompaction()**
   - 文件: `operation/FileStoreWrite.java`
   - 接口方法

4. **FileStoreWrite.getCommitIncrement()**
   - 文件: `operation/FileStoreWrite.java`
   - 接口方法
   - 相关: `utils/CommitIncrement.java`

5. **commit.commit(messages, checkpointId, commitUser)**
   - 文件: `table/sink/StreamTableCommit.java`
   - 方法: `void commit(List<CommitMessage> messages)` (接口)
   - 实现: `table/sink/StreamTableCommitImpl.java`

6. **Snapshot 创建**
   - 文件: `Snapshot.java`
   - 文件: `utils/SnapshotManager.java`
   - 方法: `void commitSnapshot(Snapshot snapshot)`

---

### 流读部分

#### 核心类关系 → 源码位置

```
StreamTableScan (接口)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java
  ✓ 关键: 行 92-160
  ✓ restore() 方法: 行 123
  ✓ checkpoint() 方法: 行 142
  ✓ notifyCheckpointComplete() 方法: 行 159

DataTableStreamScan (实现)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java
  ✓ 关键: 行 55-120+
  ✓ 核心组件: 行 80-86
  ✓ 流式扫描模式: 行 88-102
  ✓ plan() 方法: 搜索 "public Plan plan()"

StartingScanner (接口)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/StartingScanner.java
  ✓ 实现类:
    - FullStartingScanner.java
    - CompactedStartingScanner.java
    - ContinuousLatestStartingScanner.java
    - StaticFromSnapshotStartingScanner.java

FollowUpScanner (接口)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/FollowUpScanner.java
  ✓ 实现类:
    - DeltaFollowUpScanner.java
    - AllDeltaFollowUpScanner.java
    - ChangelogFollowUpScanner.java

AbstractDataTableRead (基类)
  ↓ 查看: paimon-core/src/main/java/org/apache/paimon/table/source/AbstractDataTableRead.java
  ✓ 关键: 行 98-100+
  ✓ 子类:
    - KeyValueTableRead.java
    - AppendTableRead.java
```

#### 初始化和恢复阶段 → 源码查看

1. **table.newStreamScan()**
   - 文件: 根据表类型，查看 `DataTableStreamScan.java`

2. **new DataTableStreamScan(...)**
   - 文件: `table/source/DataTableStreamScan.java`
   - 关键: 构造函数初始化各个扫描器组件

3. **scan.restore(savedNextSnapshotId)**
   - 文件: `table/source/StreamTableScan.java`
   - 方法声明: 行 123
   - 文件: `table/source/DataTableStreamScan.java`
   - 实现方法: 搜索 `public void restore`

#### 单次扫描循环 → 源码查看

1. **scan.plan() - 首次扫描**
   - 文件: `table/source/DataTableStreamScan.java`
   - 方法: `public Plan plan()`
   - 调用: `StartingScanner.scan()`
   - 文件: `table/source/snapshot/StartingScanner.java`

2. **StartingScanner 的选择**
   - 查看 `DataTableStreamScan.java` 中 `createStartingScanner()` 方法

3. **scan.plan() - 后续扫描**
   - 方法: `public Plan plan()`
   - 调用: `FollowUpScanner.scan()`
   - 文件: `table/source/snapshot/FollowUpScanner.java`

4. **NextSnapshotFetcher.getNextSnapshot()**
   - 文件: `utils/NextSnapshotFetcher.java`
   - 方法: `public Snapshot getNextSnapshot(long currentSnapshotId)`

#### 数据读取阶段 → 源码查看

1. **plan.splits()**
   - 文件: `table/source/PlanImpl.java`
   - 方法: `public List<Split> splits()`

2. **createReader(split)**
   - 文件: `table/source/TableRead.java` (接口)
   - 实现: `table/source/AbstractDataTableRead.java`
   - 方法: `public RecordReader<InternalRow> createReader(Split split)`

3. **new KeyValueTableRead(split)**
   - 文件: `table/source/KeyValueTableRead.java`
   - 方法: 构造函数

4. **reader.next()**
   - 文件: `table/source/AbstractDataTableRead.java`
   - 方法: `public InternalRow next()`

5. **RecordReader.next()**
   - 文件: `reader/RecordReader.java` (接口)
   - 实现: 具体的读取器实现

#### Checkpoint 保存和恢复 → 源码查看

1. **scan.checkpoint()**
   - 文件: `table/source/StreamTableScan.java`
   - 方法声明: 行 142
   - 文件: `table/source/DataTableStreamScan.java`
   - 实现方法: 搜索 `public Long checkpoint()`

2. **scan.notifyCheckpointComplete(nextSnapshotId)**
   - 文件: `table/source/StreamTableScan.java`
   - 方法声明: 行 159
   - 文件: `table/source/DataTableStreamScan.java`
   - 实现方法: 搜索 `public void notifyCheckpointComplete`

---

## 🔗 关键交互点源码查看

### Exactly-Once 实现

1. **CommitUser 的使用**
   - 设置: `StreamWriteBuilderImpl.java` (行 80, 104)
   - 应用: `table/sink/TableWriteImpl.java`
   - 传递: `Snapshot.java` 中的 `commitUser` 字段

2. **CommitIdentifier 的使用**
   - 参数: `StreamTableWrite.prepareCommit(boolean, long commitIdentifier)`
   - 声明: `table/sink/StreamTableWrite.java` (行 106)
   - 传递: `CommitMessage.java` 和 `Snapshot.java`

3. **重复检测**
   - 读: `table/sink/StreamTableCommitImpl.java`
   - 方法: 搜索 "commit" 方法实现
   - 比较: 与现有快照的 `commitIdentifier` 比较

### 快照机制

1. **Snapshot 创建**
   - 类: `Snapshot.java`
   - 字段: 行 X (查看源文件)
   - 建造者: `Snapshot.Builder` (内部类)

2. **SnapshotManager 使用**
   - 类: `utils/SnapshotManager.java`
   - 获取: `getSnapshot(long id)`
   - 创建: `commitSnapshot(Snapshot)`

3. **快照持久化**
   - 查看: `FileStoreCommitImpl.java`
   - 方法: 搜索快照保存方法

### 分桶和路由

1. **分桶提取**
   - 接口: `table/sink/KeyAndBucketExtractor.java`
   - 实现:
     - `FixedBucketRowKeyExtractor.java` - 固定分桶
     - `DynamicBucketRowKeyExtractor.java` - 动态分桶

2. **数据路由**
   - 类: `table/sink/TableWriteImpl.java` 或 `FileStoreWrite` 实现
   - 方法: 搜索 "bucket" 相关逻辑

---

## 💡 IDE 搜索技巧

### 快速定位关键方法

```
Ctrl+Shift+F (Find in Files) 搜索:

"public void write(InternalRow"          # 写入方法
"public List<CommitMessage> prepareCommit"  # 准备提交
"public Plan plan()"                      # 扫描计划
"public InternalRow next()"               # 读取方法
"public Long checkpoint()"                # 检查点
"commitIdentifier"                        # commitId 使用
"commitUser"                              # 应用标识
```

### 快速导航类和方法

```
Ctrl+F (Find) 搜索:
"class StreamWriteBuilderImpl"
"class DataTableStreamScan"
"interface StreamTableWrite"

Ctrl+G (Go to Line)
跳转到特定行号查看实现
```

---

## 📚 推荐阅读顺序

### 快速理解流写

1. **StreamWriteBuilderImpl.java** (60-119 行)
   - 了解初始化过程

2. **StreamTableWrite.java** (106 行)
   - 了解 prepareCommit 接口

3. **TableWriteImpl.java** (搜索 "write" 方法)
   - 了解实现细节

4. **FileStoreWrite.java** (47-54 行)
   - 了解整体流程

### 快速理解流读

1. **StreamTableScan.java** (92-160 行)
   - 了解扫描接口

2. **DataTableStreamScan.java** (55+ 行)
   - 了解实现细节

3. **StartingScanner.java** 和 **FollowUpScanner.java**
   - 了解扫描策略

4. **AbstractDataTableRead.java** (98+ 行)
   - 了解读取实现

---

## 🔍 常见问题的源码位置

### Q: commitIdentifier 是如何使用的？

A: 查看以下文件:
- `StreamTableWrite.java` (行 106) - 定义
- `TableWriteImpl.java` - 实现
- `CommitMessage.java` - 使用
- `Snapshot.java` - 存储

### Q: 如何实现 Exactly-Once？

A: 查看以下文件:
- `StreamWriteBuilderImpl.java` (行 80) - commitUser 设置
- `StreamTableWrite.java` (行 101-107) - commitIdentifier 参数
- `StreamTableCommitImpl.java` - 重复检测实现
- `Snapshot.java` - 快照中的标识

### Q: 如何跟踪读取进度？

A: 查看以下文件:
- `StreamTableScan.java` (行 142) - checkpoint() 定义
- `DataTableStreamScan.java` - checkpoint() 实现
- `StreamTableScan.java` (行 123) - restore() 定义
- `DataTableStreamScan.java` - restore() 实现

### Q: 如何执行增量扫描？

A: 查看以下文件:
- `DataTableStreamScan.java` - plan() 方法逻辑
- `FollowUpScanner.java` - 增量扫描接口
- `DeltaFollowUpScanner.java` - Delta 实现
- `AllDeltaFollowUpScanner.java` - All Delta 实现

---

## 🎯 按模块快速定位

### 表层 (Table Layer)

| 功能 | 文件 | 查看 |
|-----|------|------|
| 流写构建 | `table/sink/StreamWriteBuilderImpl.java` | 全文 |
| 流写接口 | `table/sink/StreamTableWrite.java` | 73-108 行 |
| 表写实现 | `table/sink/TableWriteImpl.java` | 83-144+ 行 |
| 流扫描接口 | `table/source/StreamTableScan.java` | 92-160 行 |
| 流扫描实现 | `table/source/DataTableStreamScan.java` | 55-120+ 行 |
| 表读实现 | `table/source/AbstractDataTableRead.java` | 98-100+ 行 |

### 操作层 (Operation Layer)

| 功能 | 文件 | 查看 |
|-----|------|------|
| 文件写接口 | `operation/FileStoreWrite.java` | 44-100+ 行 |
| 文件扫描接口 | `operation/FileStoreScan.java` | 47-100+ 行 |
| 文件提交接口 | `operation/FileStoreCommit.java` | - |
| 提交实现 | `operation/FileStoreCommitImpl.java` | - |

### 工具层 (Util Layer)

| 功能 | 文件 |
|-----|------|
| 快照 | `Snapshot.java` |
| 快照管理 | `utils/SnapshotManager.java` |
| 提交增量 | `utils/CommitIncrement.java` |
| 记录写入 | `utils/RecordWriter.java` |

---

## ⚠️ 注意事项

1. **行号可能变化** - 使用搜索而非直接行号
2. **查看 JavaDoc** - 方法上方有详细注释
3. **查看单元测试** - `src/test/` 目录有使用示例
4. **使用 IDE 的 Go to Definition** - F12 快速跳转

