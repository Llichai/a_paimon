# Paimon 流读流写 - 核心类详解与代码示例

## 目录
1. [流写核心类](#流写核心类)
2. [流读核心类](#流读核心类)
3. [代码示例](#代码示例)
4. [常见问题](#常见问题)

---

## 流写核心类

### 1. StreamWriteBuilderImpl

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java`

**职责**：
- 创建流式写入器的构建器
- 管理 commitUser（应用标识）
- 支持与 StreamTableWrite 和 StreamTableCommit 的配置

**关键字段**：
```java
private final InnerTable table;           // 内部表引用
private String commitUser;                // 提交用户标识（应用 ID）
```

**关键方法**：
```java
public StreamTableWrite newWrite()
// 创建流式写入器
// 返回: StreamTableWrite 实例

public StreamTableCommit newCommit()
// 创建流式提交器
// 返回: StreamTableCommit 实例（ignoreEmptyCommit=false）

public StreamWriteBuilder withCommitUser(String commitUser)
// 设置 commitUser
// 参数: commitUser - 应用唯一标识
// 返回: this（支持链式调用）
```

**使用示例**：
```java
Table table = catalog.getTable(identifier);
StreamWriteBuilder builder = table.newStreamWriteBuilder();
builder.withCommitUser("flink-job-123");  // 设置应用 ID

StreamTableWrite write = builder.newWrite();
StreamTableCommit commit = builder.newCommit();
```

### 2. StreamTableWrite（接口）

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableWrite.java`

**职责**：
- 定义流式写入接口
- 支持多次提交（每个 Checkpoint 一次）
- 支持状态恢复

**继承关系**：
```
TableWrite (基础写入接口)
    ↓
StreamTableWrite (流式写入接口)
    ↓
InnerTableWrite (内部接口，包括 BatchTableWrite)
```

**关键方法**：
```java
@Override
void write(InternalRow row) throws Exception
// 写入单行数据
// 参数: row - 数据行

@Override
void flush() throws Exception
// 刷新所有缓冲区

@Override
List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
throws Exception
// 为提交准备消息
// 参数:
//   - waitCompaction: 是否等待后台压缩
//   - commitIdentifier: 提交 ID（通常是 Checkpoint ID）
// 返回: 提交消息列表
```

**特殊说明**：
- `commitIdentifier` 必须递增（通常从 0 开始）
- `waitCompaction=false` 时提交更快，但可能产生更多小文件
- `waitCompaction=true` 时提交较慢，但文件更少

### 3. TableWriteImpl<T>

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java`

**职责**：
- 实现 StreamTableWrite 接口
- 协调多个组件完成写入
- 管理行键提取、记录转换、行类型生成

**架构**：
```
TableWriteImpl
├─ KeyAndBucketExtractor
│  └─ 从行数据中提取分区键、桶键、主键
├─ RecordExtractor
│  └─ 将 SinkRecord 转换为底层格式
├─ RowKindGenerator
│  └─ 生成行类型（INSERT、UPDATE、DELETE）
├─ RowKindFilter
│  └─ 过滤特定的行类型
└─ FileStoreWrite<T>
   └─ 底层文件写入
```

**关键字段**：
```java
private final FileStoreWrite<T> write;                          // 底层写入器
private final KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor;
private final RecordExtractor<T> recordExtractor;
private final @Nullable RowKindGenerator rowKindGenerator;
private final @Nullable RowKindFilter rowKindFilter;
private boolean batchCommitted = false;                        // 批量提交标志
```

**泛型参数**：
- 对于主键表：`T = KeyValue`
- 对于追加表：`T = InternalRow`

### 4. FileStoreWrite<T>（接口）

**位置**: `paimon-core/src/main/java/org/apache/paimon/operation/FileStoreWrite.java`

**职责**：
- 底层文件存储写入操作
- 管理 RecordWriter 的创建
- 处理内存缓冲和压缩

**写入流程**：
```
数据路由 (route by partition+bucket)
  ↓
内存缓冲 (write to WriteBuffer)
  ↓
缓冲溢写 (flush to temporary files when full)
  ↓
文件写入 (create data files)
  ↓
自动压缩 (background compaction)
  ↓
准备提交 (collect CommitMessage)
```

**关键接口**：
```java
FileStoreWrite<T> withWriteRestore(WriteRestore writeRestore)
// 恢复写入状态（用于故障恢复）

FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory)
// 设置内存池工厂

RecordWriter<T> getWriter(BinaryRow partition, int bucket)
// 获取或创建特定分区和桶的写入器

List<CommitMessage> prepareCommit(boolean flush)
// 准备提交消息

CommitIncrement getCommitIncrement()
// 获取本次提交的文件变更
```

---

## 流读核心类

### 1. StreamTableScan（接口）

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java`

**职责**：
- 定义流式扫描接口
- 支持 Checkpoint 和恢复
- 管理读取进度

**继承关系**：
```
TableScan (基础扫描接口)
    ↓
StreamTableScan (流式扫描接口)
    ↓
Restorable<Long> (可恢复接口)
```

**关键方法**：
```java
@Override
void restore(@Nullable Long nextSnapshotId)
// 从检查点恢复
// 参数: nextSnapshotId - 下一个要读取的快照 ID

@Override
@Nullable
Long checkpoint()
// 执行检查点，返回下一个要读取的快照 ID
// 返回: 下一个快照 ID 或 null

void notifyCheckpointComplete(@Nullable Long nextSnapshot)
// 通知检查点完成（用于清理）
// 参数: nextSnapshot - 已确认的下一个快照 ID

@Nullable
Long watermark()
// 获取当前消费快照的水位线
// 返回: 时间戳或 null
```

**特殊说明**：
- `checkpoint()` 返回**下一个要读取的快照 ID**，而非当前快照
- 这样设计是为了避免重复消费
- `notifyCheckpointComplete()` 用于清理不再需要的快照

### 2. DataTableStreamScan

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java`

**职责**：
- 实现 StreamTableScan 接口
- 协调 StartingScanner 和 FollowUpScanner
- 管理快照跟踪

**核心组件**：
```
DataTableStreamScan
├─ StartingScanner (首次扫描)
│  ├─ FullStartingScanner (完整快照)
│  ├─ CompactedStartingScanner (已压缩快照)
│  ├─ ContinuousLatestStartingScanner (最新快照)
│  └─ StaticFromSnapshotStartingScanner (指定快照)
├─ FollowUpScanner (后续扫描)
│  ├─ DeltaFollowUpScanner (增量)
│  ├─ AllDeltaFollowUpScanner (全增量)
│  └─ ChangelogFollowUpScanner (变更日志)
├─ BoundedChecker (边界检查)
└─ NextSnapshotFetcher (快照获取)
```

**扫描模式**：
```
COMPACT_BUCKET_TABLE (紧凑桶表模式，默认)
└─ 读取快照的 Delta 数据（新增/变化的文件）

FILE_MONITOR (文件监控模式)
└─ 监控文件系统的变化
```

### 3. StartingScanner（接口）

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/StartingScanner.java`

**职责**：
- 确定流式扫描的起始快照
- 根据不同的起始模式返回起始快照信息

**实现类**：
```
FullStartingScanner
└─ 扫描全量快照

CompactedStartingScanner
└─ 扫描已压缩的快照（性能更好）

ContinuousLatestStartingScanner
└─ 从最新快照开始（实时场景）

StaticFromSnapshotStartingScanner
└─ 从指定快照开始

StaticFromTimestampStartingScanner
└─ 从指定时间戳开始

StaticFromTagStartingScanner
└─ 从指定 Tag 开始
```

**关键方法**：
```java
ScannedResult scan(SnapshotReader reader)
// 执行扫描
// 返回: ScannedResult {plan, nextSnapshotId}
```

### 4. FollowUpScanner（接口）

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/FollowUpScanner.java`

**职责**：
- 扫描新的快照
- 返回增量变更

**实现类**：
```
DeltaFollowUpScanner
└─ 返回快照之间的增量变更

AllDeltaFollowUpScanner
└─ 返回所有增量变更

ChangelogFollowUpScanner
└─ 返回变更日志
```

**关键方法**：
```java
ScannedResult scan(SnapshotReader reader)
// 执行扫描
// 返回: 新快照的增量变更 ReadPlan

boolean hasNextSnapshot(NextSnapshotFetcher fetcher, long currentSnapshotId)
// 检查是否有下一个快照
```

### 5. TableRead（接口）

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/source/TableRead.java`

**职责**：
- 定义表读取接口
- 支持行过滤和列裁剪

**关键方法**：
```java
RecordReader<InternalRow> createReader(Split split)
// 为指定的 Split 创建记录读取器

TableRead withFilter(Predicate predicate)
// 添加过滤条件

TableRead withProjection(int[] projection)
// 指定要读取的列

TableRead executeFilter()
// 启用过滤执行

void close()
// 关闭读取器
```

### 6. AbstractDataTableRead

**位置**: `paimon-core/src/main/java/org/apache/paimon/table/source/AbstractDataTableRead.java`

**职责**：
- 实现 TableRead 接口
- 提供通用的读取功能
- 协调过滤、列裁剪、授权

**子类**：
```
AbstractDataTableRead
├─ KeyValueTableRead (主键表)
└─ AppendTableRead (追加表)
```

**核心功能**：
```
过滤执行
├─ 在读取阶段执行谓词
├─ 支持下推优化
└─ 减少返回的数据量

列裁剪
├─ 只读取需要的列
├─ 减少 IO 量
└─ 提高读取性能

查询授权
├─ 支持列级别权限
└─ 支持行级别权限
```

---

## 代码示例

### 示例 1: 基础流写流读

```java
// 1. 打开表
Catalog catalog = new FileCatalog(new URI("file:///data/paimon"), new Options());
Identifier identifier = Identifier.create("default", "my_table");
Table table = catalog.getTable(identifier);

// ============= 写入端 =============

// 2. 创建流写构建器
StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
writeBuilder.withCommitUser("flink-job-001");  // 设置应用 ID

// 3. 创建流写器和提交器
StreamTableWrite write = writeBuilder.newWrite();
StreamTableCommit commit = writeBuilder.newCommit();

// 4. 写入数据
InternalRow row1 = GenericRow.of(1, "Alice", LocalDateTime.now());
InternalRow row2 = GenericRow.of(2, "Bob", LocalDateTime.now());

write.write(row1);
write.write(row2);

// 5. Checkpoint 时准备提交
List<CommitMessage> messages = write.prepareCommit(
    false,    // waitCompaction=false 以提高吞吐
    1         // commitIdentifier=1（第一个 Checkpoint）
);

// 6. 应用提交
commit.commit(messages);

// ============= 读取端 =============

// 7. 创建流读扫描器
StreamTableScan scan = table.newStreamScan();

// 8. 执行扫描获得读取计划
Plan plan = scan.plan();

// 9. 读取数据
for (Split split : plan.splits()) {
    RecordReader<InternalRow> reader = scan.createReader(split);
    InternalRow row;
    while ((row = reader.next()) != null) {
        System.out.println(row);  // 处理数据
    }
    reader.close();
}

// 10. 保存读取进度（Checkpoint）
Long nextSnapshotId = scan.checkpoint();
saveCheckpoint(nextSnapshotId);

// 11. 通知 Checkpoint 完成
scan.notifyCheckpointComplete(nextSnapshotId);
```

### 示例 2: 故障恢复

```java
// 恢复写入状态
List<FileStoreWrite.State> savedWriteState = loadWriteState();
if (savedWriteState != null) {
    write.restore(savedWriteState);
}

// 写入新数据
write.write(newRow);

// 准备提交（重复消息会被过滤）
List<CommitMessage> messages = write.prepareCommit(true, checkpointId);
commit.commit(messages);

// ----

// 恢复读取进度
Long savedNextSnapshotId = loadReadCheckpoint();
if (savedNextSnapshotId != null) {
    scan.restore(savedNextSnapshotId);
}

// 继续读取（从保存的进度开始）
Plan plan = scan.plan();
processPlan(plan);

// 保存新进度
Long newNextSnapshotId = scan.checkpoint();
saveReadCheckpoint(newNextSnapshotId);
```

### 示例 3: 高级功能 - 谓词下推和列裁剪

```java
// 创建表读取器
TableRead read = scan.createRead();

// 设置列裁剪（只读取 id 和 name 列）
int[] projection = new int[]{0, 1};
read.withProjection(projection);

// 设置过滤条件（只读取 age > 20 的行）
Predicate filter = new Predicate(
    OperatorType.GREATER_THAN,
    "age",
    20
);
read.withFilter(filter);

// 启用过滤执行
read.executeFilter();

// 创建读取器
RecordReader<InternalRow> reader = read.createReader(split);

// 读取数据（已经过滤和裁剪）
InternalRow row;
while ((row = reader.next()) != null) {
    // row 只包含 id 和 name，且 age > 20
    System.out.println(row);
}
```

### 示例 4: 时间旅行 - 读取历史快照

```java
// 获取历史快照
SnapshotManager snapshotManager = table.snapshotManager();
Snapshot snapshot = snapshotManager.snapshot(snapshotId);  // 读取指定快照

// 创建扫描计划
DataTableBatchScan batchScan = table.newBatchScan();
Plan plan = batchScan.withSnapshot(snapshot).plan();

// 读取历史数据
for (Split split : plan.splits()) {
    RecordReader<InternalRow> reader = batchScan.createReader(split);
    InternalRow row;
    while ((row = reader.next()) != null) {
        System.out.println(row);
    }
    reader.close();
}
```

### 示例 5: Flink 集成示例

```java
// Flink Source
public class PaimonStreamingSource extends SourceFunction<InternalRow> {
    private StreamTableScan scan;
    private boolean running = true;

    @Override
    public void run(SourceContext<InternalRow> ctx) {
        // 恢复读取进度
        Long savedNextSnapshot = context.getRestoredCheckpoint();
        if (savedNextSnapshot != null) {
            scan.restore(savedNextSnapshot);
        }

        // 持续读取
        while (running) {
            Plan plan = scan.plan();

            if (plan == null || plan.splits().isEmpty()) {
                Thread.sleep(1000);  // 等待新数据
                continue;
            }

            // 读取数据
            for (Split split : plan.splits()) {
                RecordReader<InternalRow> reader = scan.createReader(split);
                InternalRow row;
                while ((row = reader.next()) != null) {
                    ctx.collect(row);
                }
                reader.close();
            }

            // 保存读取进度
            Long nextSnapshotId = scan.checkpoint();
            scan.notifyCheckpointComplete(nextSnapshotId);

            Thread.sleep(100);  // 避免忙轮询
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

// Flink Sink
public class PaimonStreamingSink extends SinkFunction<InternalRow> {
    private StreamTableWrite write;
    private StreamTableCommit commit;

    @Override
    public void invoke(InternalRow row, Context ctx) {
        write.write(row);
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        long checkpointId = context.getCheckpointId();

        // 准备提交
        List<CommitMessage> messages = write.prepareCommit(
            false,
            checkpointId
        );

        // 异步提交
        executor.submit(() -> {
            try {
                commit.commit(messages);
            } catch (Exception e) {
                // 处理错误
            }
        });
    }
}
```

---

## 常见问题

### Q1: commitIdentifier 应该如何使用？

**A**: `commitIdentifier` 应该与 Checkpoint ID 对应，并且必须递增：

```java
// 正确做法
long checkpointId = getFlinkCheckpointId();  // 1, 2, 3, ...
List<CommitMessage> messages = write.prepareCommit(false, checkpointId);

// 错误做法（不递增）
write.prepareCommit(false, 1);  // first checkpoint
write.prepareCommit(false, 1);  // WRONG! 应该是 2
```

### Q2: waitCompaction 应该设置为 true 还是 false？

**A**: 取决于你的需求：

```java
// waitCompaction=false（推荐用于实时场景）
// 优点: 提交快速，吞吐高
// 缺点: 可能产生更多小文件
List<CommitMessage> messages = write.prepareCommit(false, checkpointId);

// waitCompaction=true（推荐用于批处理或最终一致性场景）
// 优点: 文件数少，查询性能好
// 缺点: 提交慢，吞吐降低
List<CommitMessage> messages = write.prepareCommit(true, checkpointId);
```

### Q3: 如何处理流读中的故障恢复？

**A**: 使用 `checkpoint()` 和 `restore()`：

```java
// 保存进度
Long nextSnapshotId = scan.checkpoint();
context.getStateStore().put("next_snapshot_id", nextSnapshotId);

// 恢复进度
Long savedId = context.getStateStore().get("next_snapshot_id");
if (savedId != null) {
    scan.restore(savedId);
}
```

### Q4: 写入端故障后，消息会重复吗？

**A**: 不会。Paimon 使用 `commitUser + commitIdentifier` 的组合来过滤重复：

```java
// 第一个 Checkpoint
write.prepareCommit(false, 1);  // 消息集合 M1
commit.commit(messages);         // 创建 Snapshot-2

// 故障发生...

// 恢复后，状态中包含 commitIdentifier=1
// 第二个提交尝试
write.prepareCommit(false, 2);  // 消息集合 M1 + M2
commit.commit(messages);         // Commit 端检测到 M1 已在 Snapshot-2
                                 // 只应用 M2，创建 Snapshot-3
```

### Q5: nextSnapshotId 为什么是"下一个"而不是"当前"？

**A**: 这样设计是为了避免重复消费：

```java
// 第一次读取
checkpoint() → 1  // "下一个要读的快照是 1"
// 处理快照 0 的数据

// 第二次读取
restore(1)        // "从快照 1 开始读"
checkpoint() → 2  // "下一个要读的快照是 2"
// 处理快照 1 的数据

// 如果设计反了，会导致快照被重复消费
```

### Q6: 如何实现列裁剪和过滤下推？

**A**: 使用 `withProjection()` 和 `withFilter()`：

```java
TableRead read = table.newRead();

// 列裁剪：只读取 id, name, age 列
int[] projection = new int[]{0, 1, 3};
read.withProjection(projection);

// 过滤条件：age > 18
Predicate filter = new Predicate(
    OperatorType.GREATER_THAN,
    "age",
    18
);
read.withFilter(filter);

// 启用过滤执行
read.executeFilter();

// 创建读取器
RecordReader<InternalRow> reader = read.createReader(split);
```

### Q7: 如何监控流写的性能？

**A**: 使用 MetricRegistry 和 CompactionMetrics：

```java
// 在写入构造时传入 MetricRegistry
FileStoreWrite<T> write = new KeyValueFileStoreWrite(
    fileStore,
    metricRegistry,  // 传入指标注册表
    ...
);

// 指标包括:
// - record.count: 写入的记录数
// - file.count: 创建的文件数
// - compaction.duration: 压缩耗时
// - memory.usage: 内存使用情况
```

### Q8: 如何处理大型表的流读？

**A**: 使用分片读取和进度跟踪：

```java
// 创建扫描计划
Plan plan = scan.plan();

// 分片处理（减少单次处理的数据量）
int batchSize = 10;
List<Split> splits = new ArrayList<>(plan.splits());

for (int i = 0; i < splits.size(); i += batchSize) {
    List<Split> batch = splits.subList(i, Math.min(i + batchSize, splits.size()));

    for (Split split : batch) {
        RecordReader<InternalRow> reader = scan.createReader(split);
        // 处理数据...
    }

    // 定期保存进度
    if ((i + batchSize) % (10 * batchSize) == 0) {
        Long nextSnapshotId = scan.checkpoint();
        saveProgressCheckpoint(nextSnapshotId);
    }
}

// 最终保存进度
Long finalNextSnapshotId = scan.checkpoint();
saveProgressCheckpoint(finalNextSnapshotId);
```

