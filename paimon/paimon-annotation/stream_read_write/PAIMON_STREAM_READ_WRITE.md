# Apache Paimon 流读流写实现分析

## 目录
1. [概述](#概述)
2. [流写（Stream Write）](#流写-stream-write)
3. [流读（Stream Read）](#流读-stream-read)
4. [核心交互机制](#核心交互机制)
5. [详细时序图](#详细时序图)

---

## 概述

Paimon 采用分层架构设计，将流读流写分为三个主要层次：

```
┌──────────────────────────────────────┐
│  应用层（Application Layer）         │
│  - Flink/Spark Streaming             │
└──────────────────────────────────────┘
           ↓ ↑
┌──────────────────────────────────────┐
│  表层（Table Layer）                 │
│  - StreamTableWrite/StreamTableRead   │
│  - TableWriteImpl/AbstractDataTableRead│
└──────────────────────────────────────┘
           ↓ ↑
┌──────────────────────────────────────┐
│  操作层（Operation Layer）           │
│  - FileStoreWrite/FileStoreScan       │
│  - BucketedFileStoreWriter            │
└──────────────────────────────────────┘
           ↓ ↑
┌──────────────────────────────────────┐
│  存储层（Storage Layer）             │
│  - 数据文件、Manifest文件            │
│  - 快照（Snapshot）                  │
└──────────────────────────────────────┘
```

---

## 流写（Stream Write）

### 核心类关系

```
StreamWriteBuilder (接口)
    ↓
StreamWriteBuilderImpl
    ↓ newWrite()
StreamTableWrite (接口)
    ↓ 实现
TableWriteImpl<T>
    ↓ 包含
FileStoreWrite<T>
    ↓ 实现
BucketedFileStoreWriter/AppendOnlyFileStoreWriter
    ↓
RecordWriter (底层写入器)
```

### 流写流程

#### 1. 初始化阶段

```
timeline
    title Paimon 流写初始化流程

    participant App as 应用程序
    participant Builder as StreamWriteBuilderImpl
    participant Write as StreamTableWrite
    participant TableWrite as TableWriteImpl
    participant FileWrite as FileStoreWrite

    App->>Builder: new StreamWriteBuilderImpl(table)
    Note right of Builder: 创建流写构建器
    Note right of Builder: commitUser = UUID

    App->>Builder: withCommitUser("app-id")
    Builder->>Builder: this.commitUser = "app-id"
    Note right of Builder: 设置唯一的应用标识

    App->>Builder: newWrite()
    Builder->>Write: table.newWrite(commitUser)
    Note right of Write: 创建流式写入器

    Write->>TableWrite: new TableWriteImpl(...)
    Note right of TableWrite: 初始化 KeyAndBucketExtractor
    Note right of TableWrite: 初始化 RecordExtractor
    Note right of TableWrite: 初始化 RowKindGenerator

    TableWrite->>FileWrite: 创建文件存储写入器
    Note right of FileWrite: 为每个分区/桶分配 Writer

    App-->>Write: 返回写入器
```

#### 2. 数据写入阶段（单行处理）

```
timeline
    title Paimon 单行写入流程

    participant App as 应用程序
    participant Write as StreamTableWrite
    participant TableWrite as TableWriteImpl
    participant Extractor as KeyAndBucketExtractor
    participant RecExt as RecordExtractor
    participant FileWrite as FileStoreWrite
    participant RecWriter as RecordWriter

    App->>Write: write(row)
    Write->>TableWrite: write(row)

    TableWrite->>Extractor: extract(row)
    Note right of Extractor: 提取分区键、桶键、主键
    Extractor-->>TableWrite: partition, bucket

    TableWrite->>RecExt: toRecord(row)
    Note right of RecExt: 转换为底层格式 (KeyValue or Row)
    RecExt-->>TableWrite: record

    TableWrite->>FileWrite: write(partition, bucket, record)
    Note right of FileWrite: 根据 partition+bucket 路由

    FileWrite->>RecWriter: write(record)
    Note right of RecWriter: 写入内存缓冲区

    alt 内存缓冲满
        RecWriter->>RecWriter: flush()
        Note right of RecWriter: 刷写缓冲区到临时文件
    end

    alt 达到触发条件
        FileWrite->>FileWrite: 触发压缩任务
        Note right of FileWrite: 后台异步压缩
    end
```

#### 3. Checkpoint 提交阶段

```
timeline
    title Paimon Checkpoint 提交流程（Stream Write）

    participant Flink as Flink Checkpoint
    participant Write as StreamTableWrite
    participant TableWrite as TableWriteImpl
    participant FileWrite as FileStoreWrite
    participant Commit as StreamTableCommit

    Flink->>Write: prepareCommit(waitCompaction, checkpointId)
    Note right of Write: Checkpoint 触发

    Write->>TableWrite: 调用底层实现

    TableWrite->>FileWrite: flush()
    Note right of FileWrite: 刷写所有缓冲区

    alt waitCompaction = true
        FileWrite->>FileWrite: waitForCompaction()
        Note right of FileWrite: 等待后台压缩完成
    end

    FileWrite->>FileWrite: getCommitIncrement()
    Note right of FileWrite: 收集本次检查点的文件变更

    FileWrite-->>TableWrite: List<CommitMessage>
    Note right of TableWrite: 包含新增/删除文件列表

    TableWrite-->>Write: messages

    Write-->>Flink: messages
    Note right of Flink: 发送到提交算子

    Flink->>Commit: commit(messages, checkpointId, commitUser)
    Note right of Commit: 应用文件变更到元数据

    Commit->>Commit: 创建新的快照
    Note right of Commit: nextSnapshotId = currentId + 1

    Commit-->>Flink: 提交完成
```

#### 4. 关键特性说明

**CommitIdentifier 管理：**
- 每个 Checkpoint 都有唯一的 commitIdentifier
- 从 0 开始递增（如 Checkpoint ID）
- 与 StreamTableCommit 使用相同的值
- 用于幂等性保证（避免重复提交）

**状态恢复：**
```java
// 故障恢复时
List<State<T>> saveState = write.snapshotState();
// ... 保存到 Checkpoint ...

// 恢复时
write.restoreState(savedState);
// 继续写入，不重复提交已确认的消息
```

**CommitUser 标识：**
- 唯一标识一个流式应用（如 UUID 或 Job ID）
- 支持多个应用并行写入同一表
- 与 Checkpoint ID 组合保证全局唯一性

---

## 流读（Stream Read）

### 核心类关系

```
StreamTableScan (接口)
    ↓
DataTableStreamScan
    ↓
├─ StartingScanner (起始扫描器)
├─ FollowUpScanner (跟随扫描器)
├─ BoundedChecker (边界检查器)
└─ NextSnapshotFetcher (快照获取器)
    ↓
FileStoreScan (底层扫描)
    ↓
ReadPlan (扫描计划)
    ↓
TableRead (表读取)
    ↓
AbstractDataTableRead (实现)
    ├─ KeyValueTableRead (主键表)
    └─ AppendTableRead (追加表)
```

### 流读流程

#### 1. 初始化和恢复阶段

```
timeline
    title Paimon 流扫描初始化与恢复流程

    participant App as 应用程序
    participant Scan as StreamTableScan
    participant DataScan as DataTableStreamScan
    participant StartScan as StartingScanner
    participant NextFetcher as NextSnapshotFetcher

    App->>Scan: table.newStreamScan()
    Note right of Scan: 创建流式扫描器

    Scan->>DataScan: new DataTableStreamScan(...)
    Note right of DataScan: 初始化各个扫描器组件

    DataScan->>StartScan: 创建起始扫描器
    Note right of StartScan: 根据 scan-mode 选择

    DataScan->>NextFetcher: new NextSnapshotFetcher(...)
    Note right of NextFetcher: 用于查找下一个快照

    App->>Scan: restore(savedNextSnapshotId)
    Note right of Scan: 从 Checkpoint 恢复

    Scan->>DataScan: restore(nextSnapshotId)
    Note right of DataScan: 保存恢复的快照 ID

    Note right of Scan: 现在准备好开始扫描
```

#### 2. 单次扫描循环（Batch 扫描）

```
timeline
    title Paimon 流式扫描单次循环

    participant App as 应用程序
    participant Scan as StreamTableScan
    participant DataScan as DataTableStreamScan
    participant StartScan as StartingScanner
    participant FollowScan as FollowUpScanner
    participant FileStoreScan as FileStoreScan
    participant SnapshotMgr as SnapshotManager

    loop 持续扫描
        App->>Scan: plan = scan.plan()

        Scan->>DataScan: plan()

        alt 第一次扫描（没有保存的快照 ID）
            DataScan->>StartScan: scan()
            Note right of StartScan: 确定起始快照
            Note right of StartScan: 例如：最新快照、指定快照、时间戳等

            StartScan->>SnapshotMgr: getSnapshot(snapshotId)
            SnapshotMgr-->>StartScan: snapshot

            StartScan->>FileStoreScan: 创建扫描计划
            FileStoreScan-->>StartScan: ReadPlan

            StartScan-->>DataScan: ScannedResult {plan, nextSnapshotId}

            DataScan->>DataScan: currentSnapshotId = result.nextSnapshotId

        else 后续扫描（有已保存的快照 ID）
            DataScan->>FollowScan: scan(currentSnapshotId)
            Note right of FollowScan: 扫描新快照的增量变化

            FollowScan->>NextFetcher: getNextSnapshot(currentSnapshotId)
            Note right of NextFetcher: 查找比 currentId 更新的快照
            NextFetcher-->>FollowScan: nextSnapshot or null

            alt 有新快照
                FollowScan->>FileStoreScan: 创建增量扫描计划
                FileStoreScan-->>FollowScan: IncrementalReadPlan

                FollowScan-->>DataScan: ScannedResult {plan, nextSnapshotId}
                DataScan->>DataScan: currentSnapshotId = result.nextSnapshotId

            else 无新快照
                Note right of FollowScan: 没有新数据
                DataScan-->>App: null or empty plan
            end
        end

        DataScan-->>Scan: plan
        Scan-->>App: plan

        alt 有扫描计划
            App->>App: 处理数据

            App->>Scan: checkpoint()
            Note right of Scan: 保存读取进度
            Scan-->>App: nextSnapshotId

            App->>App: 保存 nextSnapshotId 到 Checkpoint

            App->>Scan: notifyCheckpointComplete(nextSnapshotId)
            Note right of Scan: 清理过期快照、更新消费位置

        else 无新快照
            Note right of App: 等待新数据
            break
        end
    end
```

#### 3. 数据读取阶段

```
timeline
    title Paimon 数据读取流程

    participant App as 应用程序
    participant Plan as ReadPlan
    participant TableRead as TableRead
    participant DataRead as AbstractDataTableRead
    participant RecReader as RecordReader
    participant FileReader as 文件读取器

    App->>Plan: splits = plan.splits()
    Note right of Plan: 获取数据分片列表
    Plan-->>App: List<Split>

    loop 遍历每个 Split
        App->>TableRead: reader = createReader(split)

        TableRead->>DataRead: new KeyValueTableRead(split)
        Note right of DataRead: 初始化读取器

        DataRead->>RecReader: 创建下层记录读取器
        Note right of RecReader: 根据 Split 类型选择

        loop 读取记录
            App->>TableRead: row = reader.next()

            TableRead->>DataRead: next()

            DataRead->>RecReader: next()
            Note right of RecReader: 读取下一条记录

            RecReader->>FileReader: 从数据文件读取
            Note right of FileReader: 支持多种格式（Parquet、ORC等）
            FileReader-->>RecReader: data

            alt 应用了过滤条件
                DataRead->>DataRead: 执行谓词过滤
                Note right of DataRead: 在内存中过滤
            end

            alt 列裁剪
                DataRead->>DataRead: 投影到所需列
                Note right of DataRead: 减少返回的数据量
            end

            RecReader-->>DataRead: record
            DataRead-->>TableRead: row
            TableRead-->>App: row

        end

        App->>TableRead: close()
        TableRead->>RecReader: close()
        RecReader->>FileReader: close()

    end
```

#### 4. Checkpoint 保存和恢复

```
timeline
    title Paimon 流读 Checkpoint 机制

    participant Flink as Flink Checkpoint
    participant Scan as StreamTableScan
    participant DataScan as DataTableStreamScan

    Flink->>Scan: checkpoint()
    Note right of Scan: 请求保存读取进度

    Scan->>DataScan: checkpoint()
    Note right of DataScan: 获取下一个快照 ID

    DataScan->>DataScan: nextSnapshotId = ???
    Note right of DataScan: 返回下一个要读的快照ID

    DataScan-->>Scan: nextSnapshotId
    Scan-->>Flink: nextSnapshotId
    Note right of Flink: 保存到 Checkpoint

    Flink->>Scan: notifyCheckpointComplete(nextSnapshotId)
    Note right of Scan: Checkpoint 已成功

    Scan->>DataScan: notifyCheckpointComplete(nextSnapshotId)
    Note right of DataScan: 可以清理过期快照

    ... 故障发生 ...

    Flink->>Scan: restore(savedNextSnapshotId)
    Note right of Scan: 从保存的检查点恢复

    Scan->>DataScan: restore(nextSnapshotId)
    Note right of DataScan: 恢复到该快照

    Note right of DataScan: 下次 scan() 将读取该快照的数据
```

### 流读模式（StreamScanMode）

Paimon 支持多种流式扫描模式：

#### 模式对比

| 模式 | 描述 | 使用场景 | 文件数 |
|-----|------|---------|------|
| **COMPACT_BUCKET_TABLE** | 读取快照的 Delta 文件（新增/变化的文件） | 主键表、追加表（默认） | 较少 |
| **FILE_MONITOR** | 监控文件系统变化 | 外部系统写入的文件 | 可能较多 |

#### 扫描阶段

流式扫描通常分为两个阶段：

```
阶段一：全量扫描（Full Phase）
├─ 首次启动时执行
├─ 读取起始快照的完整数据
├─ 使用 StartingScanner 确定起始快照
└─ 示例：scan-mode=ALL

阶段二：增量扫描（Incremental Phase）
├─ 持续执行
├─ 读取后续快照的变化数据
├─ 使用 FollowUpScanner 跟踪新快照
└─ 示例：scan-mode=INCREMENTAL
```

---

## 核心交互机制

### 1. Exactly-Once 保证机制

#### 写入端（Stream Write）

```
应用启动
  ↓
创建 StreamWriteBuilder
  ↓
设置 commitUser = "app-id-uuid"
  ↓
newWrite() → 得到 StreamTableWrite
  ↓
开始持续写入
  ↓
Checkpoint 触发
  ↓
prepareCommit(waitCompaction=true, checkpointId=1)
  ↓
刷写缓冲区到文件
  ↓
生成 CommitMessage 列表
  ↓
发送到提交端
  ↓
Commit 端应用变更
  ↓
创建新快照 (snapshotId=2, commitUser="app-id-uuid", commitIdentifier=1)
  ↓
Checkpoint 完成
  ↓
下次 Checkpoint 时
  ↓
prepareCommit(checkpointId=2)
  ↓
... 重复 ...
```

**关键保证：**
- commitUser 唯一标识一个应用
- commitIdentifier 递增
- 结合使用可以过滤重复消息
- 支持从任何 Checkpoint 恢复

#### 读取端（Stream Read）

```
应用启动
  ↓
创建 StreamTableScan
  ↓
无恢复数据，使用 StartingScanner
  ↓
scan() → 读取起始快照的全量数据
  ↓
checkpoint() → 返回 nextSnapshotId
  ↓
保存到 Checkpoint
  ↓
处理数据
  ↓
notifyCheckpointComplete() → 清理过期快照
  ↓
Checkpoint 完成
  ↓
有故障，从 Checkpoint 恢复
  ↓
restore(savedNextSnapshotId)
  ↓
下次 scan() 时，使用 FollowUpScanner
  ↓
扫描从 savedNextSnapshotId 之后的增量数据
  ↓
checkpoint() → 返回新的 nextSnapshotId
  ↓
... 重复 ...
```

### 2. 快照（Snapshot）机制

快照是连接读写的关键数据结构：

```
Snapshot 内容
├─ snapshotId: 快照 ID（递增）
├─ timestamp: 快照创建时间
├─ partition entries: 分区元信息
├─ bucket entries: 桶元信息
│  ├─ level: 所属层级
│  └─ files: 该桶的数据文件列表
│      ├─ fileName
│      ├─ schemaId
│      ├─ level
│      ├─ recordCount
│      └─ fileSize
├─ commitUser: 提交者（对于流式写入）
├─ commitIdentifier: 提交标识符（对于流式写入）
├─ schemaId: 表结构版本
├─ baseManifestList: 基础 Manifest 文件列表
└─ deltaManifestList: 增量 Manifest 文件列表
```

**快照的用途：**
1. **写入端**：确定提交时的文件变更范围
2. **读取端**：确定要读取的数据文件
3. **时间旅行**：支持读取历史快照
4. **故障恢复**：支持从特定快照恢复

### 3. 分桶（Bucket）与路由

```
写入流程中的分桶
├─ 固定分桶（Fixed Bucketing）
│  ├─ bucket = hash(primary_key) % num_buckets
│  ├─ 分桶数固定
│  └─ 支持分布式写入
│
└─ 动态分桶（Dynamic Bucketing）
   ├─ bucket = 动态分配
   ├─ 分桶数可变
   └─ 自适应数据分布
```

### 4. 压缩（Compaction）

```
后台压缩流程
├─ 监控条件：文件数、文件大小、时间间隔
├─ 触发方式：
│  ├─ 自动触发（背景任务）
│  └─ 显式等待（waitCompaction=true）
├─ 压缩策略：
│  ├─ Universal Compaction
│  └─ Level Compaction
└─ 输出结果：
   ├─ 新的数据文件
   └─ 删除旧文件的记录
```

---

## 详细时序图

### 场景一：完整的实时写读流程

```
Timeline: 完整的实时写读流程

步骤 1: 初始化阶段
Time: T0
┌─────────────────────────────────────────────────────────────┐
│ Writer 侧                    │ Reader 侧                      │
├─────────────────────────────────────────────────────────────┤
│ new StreamWriteBuilder      │ new StreamTableScan            │
│   └─ commitUser="job-123"   │   └─ restore(null)             │
│ newWrite()                  │ restore() - 无保存数据         │
│   └─ StreamTableWrite       │                                │
└─────────────────────────────────────────────────────────────┘

步骤 2: 第一个 Checkpoint 周期（[0, T1]）
Time: T1
┌─────────────────────────────────────────────────────────────┐
│ Writer 侧                    │ Reader 侧                      │
├─────────────────────────────────────────────────────────────┤
│ write(row1, row2, row3)     │ plan() 触发 StartingScanner    │
│ write(row4, row5)           │   └─ 扫描快照 1 的全量数据    │
│                              │ checkpoint() → nextSnapshotId=1│
│ Checkpoint#1 触发           │                                │
│ prepareCommit(false, 1)     │ 处理数据（row1-row5）        │
│   └─ CommitMessage[] msgs   │                                │
│ 发送 msgs 到 Commit 端      │ notifyCheckpointComplete(1)   │
│                              │ Checkpoint 完成               │
│ Commit 端创建 Snapshot#2    │ 保存 nextSnapshotId=1         │
│   └─ commitUser="job-123"   │                                │
│   └─ commitIdentifier=1     │                                │
└─────────────────────────────────────────────────────────────┘

步骤 3: 第二个 Checkpoint 周期（[T1, T2]）
Time: T2
┌─────────────────────────────────────────────────────────────┐
│ Writer 侧                    │ Reader 侧                      │
├─────────────────────────────────────────────────────────────┤
│ write(row6, row7, row8)     │ plan() 触发 FollowUpScanner    │
│ write(row9)                 │   └─ nextSnapshotId=1 → 查找2 │
│                              │   └─ 扫描 Snapshot#2 的增量   │
│ Checkpoint#2 触发           │ checkpoint() → nextSnapshotId=2│
│ prepareCommit(false, 2)     │                                │
│   └─ CommitMessage[] msgs   │ 处理增量数据（row6-row9）    │
│ 发送 msgs 到 Commit 端      │                                │
│                              │ notifyCheckpointComplete(2)   │
│ Commit 端创建 Snapshot#3    │ Checkpoint 完成               │
│   └─ commitIdentifier=2     │ 保存 nextSnapshotId=2         │
└─────────────────────────────────────────────────────────────┘

步骤 4: 故障恢复（恢复到 Checkpoint#2）
Time: T2（故障）
┌─────────────────────────────────────────────────────────────┐
│ Writer 侧                    │ Reader 侧                      │
├─────────────────────────────────────────────────────────────┤
│ 检测到故障                   │ 检测到故障                    │
│ restore(savedState)          │ restore(nextSnapshotId=2)     │
│   └─ 恢复未提交的消息        │   └─ 设置起始快照为 2        │
│                              │                                │
│ write(row10, row11)          │ plan() 触发 FollowUpScanner   │
│ 写入恢复后的新数据           │   └─ nextSnapshotId=2 → 查找3│
│                              │   └─ 扫描 Snapshot#3 的增量   │
│ Checkpoint#3 触发           │                                │
│ prepareCommit(false, 3)     │ checkpoint() → nextSnapshotId=3│
│ 发送 msgs（包含已保存的 ID） │ notifyCheckpointComplete(3)   │
│                              │                                │
│ Commit 端过滤重复消息       │ 继续正常处理                  │
│ 创建 Snapshot#4             │                                │
│   └─ commitIdentifier=3     │ Checkpoint 完成               │
└─────────────────────────────────────────────────────────────┘
```

### 场景二：故障恢复检测重复消息

```
Timeline: 故障恢复中的重复检测

Checkpoint#1（成功）
├─ commitIdentifier=1
├─ Messages: {msg1, msg2, msg3}
├─ Snapshot#2 创建成功
└─ 故障发生在 Checkpoint 确认前

恢复时
├─ Writer restore(savedState)
│  └─ State 中包含 commitIdentifier=1
│
├─ 故障后新数据写入
│  └─ write(msg4, msg5)
│
└─ Checkpoint#2 尝试提交
   ├─ prepareCommit(false, 2)
   ├─ Messages: {msg1, msg2, msg3, msg4, msg5}
   │  └─ 前 3 个与 Checkpoint#1 重复
   │
   └─ Commit 端
      ├─ 检查 commitIdentifier=2
      ├─ 与之前的快照对比
      ├─ 识别出 msg1-msg3 已被 Snapshot#2 包含
      ├─ 只应用 msg4, msg5 的变更
      └─ 创建 Snapshot#3（增量）
```

---

## 总结

### 流写的关键特点

1. **分层设计**：应用层 → 表层 → 操作层 → 存储层
2. **分桶路由**：按分区和桶将数据路由到不同的 Writer
3. **异步压缩**：后台自动压缩小文件
4. **Checkpoint 驱动**：每个 Checkpoint 生成一个提交消息
5. **Exactly-Once**：通过 commitUser + commitIdentifier 实现

### 流读的关键特点

1. **两阶段扫描**：全量阶段 → 增量阶段
2. **快照跟踪**：持续查询新快照并读取增量
3. **状态管理**：checkpoint() 和 restore() 支持故障恢复
4. **灵活过滤**：支持谓词下推、列裁剪、分区过滤
5. **时间旅行**：支持从任意历史快照恢复

### 写读交互

```
StreamTableWrite
    ↓ prepareCommit(commitIdentifier)
    ↓ CommitMessage[]
    ↓
StreamTableCommit
    ↓ commit(messages, commitIdentifier)
    ↓ 创建新快照
    ↓ Snapshot (snapshotId, commitUser, commitIdentifier)
    ↓
StreamTableScan
    ↓ scan() / plan()
    ↓ 读取快照的数据
    ↓ 返回数据给应用
```

### 文件结构示意

```
表目录结构
├─ snapshots/
│  ├─ SNAPSHOT-1
│  ├─ SNAPSHOT-2 (commitUser="job-123", commitIdentifier=1)
│  └─ SNAPSHOT-3 (commitUser="job-123", commitIdentifier=2)
├─ manifest/
│  ├─ MANIFEST-1
│  ├─ MANIFEST-2
│  └─ MANIFEST-LIST
├─ data/
│  ├─ partition=20240101/
│  │  ├─ bucket-0/
│  │  │  ├─ data-1-0.parquet
│  │  │  ├─ data-2-0.parquet
│  │  │  └─ index-2-0.parquet
│  │  └─ bucket-1/
│  └─ partition=20240102/
└─ schema/
   ├─ SCHEMA-0
   └─ SCHEMA-1
```

