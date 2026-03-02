# Paimon 流读流写 - 快速参考指南

## 快速启动

### 最小化写入示例

```java
Table table = catalog.getTable(identifier);
StreamWriteBuilder builder = table.newStreamWriteBuilder()
    .withCommitUser("app-id");

StreamTableWrite write = builder.newWrite();
StreamTableCommit commit = builder.newCommit();

// 写入数据
write.write(row1);
write.write(row2);

// Checkpoint 提交
List<CommitMessage> msgs = write.prepareCommit(false, checkpointId);
commit.commit(msgs);
```

### 最小化读取示例

```java
Table table = catalog.getTable(identifier);
StreamTableScan scan = table.newStreamScan();

// 恢复进度
Long savedId = loadCheckpoint();
if (savedId != null) {
    scan.restore(savedId);
}

// 扫描和读取
Plan plan = scan.plan();
for (Split split : plan.splits()) {
    RecordReader<InternalRow> reader = scan.createReader(split);
    InternalRow row;
    while ((row = reader.next()) != null) {
        processRow(row);
    }
    reader.close();
}

// 保存进度
Long nextId = scan.checkpoint();
saveCheckpoint(nextId);
```

---

## 类名速查

### 写入相关

| 类 | 位置 | 用途 |
|---|------|------|
| `StreamWriteBuilder` | `.table.sink` | 创建流写构建器 |
| `StreamWriteBuilderImpl` | `.table.sink` | 流写构建器实现 |
| `StreamTableWrite` | `.table.sink` | 流写接口 |
| `TableWriteImpl` | `.table.sink` | 表写实现 |
| `FileStoreWrite` | `.operation` | 底层文件写接口 |
| `StreamTableCommit` | `.table.sink` | 流提交接口 |
| `CommitMessage` | `.table.sink` | 提交消息 |

### 读取相关

| 类 | 位置 | 用途 |
|---|------|------|
| `StreamTableScan` | `.table.source` | 流扫描接口 |
| `DataTableStreamScan` | `.table.source` | 流扫描实现 |
| `TableRead` | `.table.source` | 表读接口 |
| `AbstractDataTableRead` | `.table.source` | 表读实现基类 |
| `StartingScanner` | `.table.source.snapshot` | 起始扫描器 |
| `FollowUpScanner` | `.table.source.snapshot` | 后续扫描器 |
| `FileStoreScan` | `.operation` | 底层文件扫描 |

---

## 常用方法速查

### 写入方法

```java
// 构建器方法
StreamWriteBuilder withCommitUser(String commitUser)
StreamTableWrite newWrite()
StreamTableCommit newCommit()

// 写入方法
void write(InternalRow row)
void flush()
List<CommitMessage> prepareCommit(boolean waitCompaction, long commitId)

// 提交方法
void commit(List<CommitMessage> messages)
void commit(List<CommitMessage> messages, boolean ignoreEmptyCommit)
```

### 读取方法

```java
// 扫描方法
void restore(Long nextSnapshotId)
Long checkpoint()
void notifyCheckpointComplete(Long nextSnapshot)
Plan plan()

// 读取方法
RecordReader<InternalRow> createReader(Split split)
InternalRow next()
void close()

// 过滤和投影
TableRead withFilter(Predicate predicate)
TableRead withProjection(int[] projection)
TableRead executeFilter()
```

---

## 关键概念对照表

| 概念 | 写入端 | 读取端 | 用途 |
|-----|------|------|------|
| **状态标识** | commitIdentifier | nextSnapshotId | 标记进度 |
| **应用标识** | commitUser | - | 标记应用 |
| **故障恢复** | restore(state) | restore(nextSnapshotId) | 恢复进度 |
| **进度保存** | prepareCommit | checkpoint() | 保存状态 |
| **进度确认** | commit() | notifyCheckpointComplete() | 确认完成 |
| **递增性** | commitId: 0,1,2,... | nextId: 1,2,3,... | 避免重复 |

---

## 配置参数

### 写入相关

```java
// 表选项
"write.buffer-size"                  // 写缓冲大小，默认 256 MB
"write.ignore-empty-commit"          // 忽略空提交，默认 false（流式为 false）
"write.manifest.target-file-size"    // Manifest 目标大小
"write.num-compact-writers"          // 压缩写入器数量
"write.stats-collect-map"            // 统计收集方式

// 流写特定
"write.commit-user"                  // 默认 commitUser（可通过 builder 覆盖）
```

### 读取相关

```java
// 表选项
"scan-parallelism"                   // 扫描并行度
"scan.mode"                          // 扫描模式（ALL, INCREMENTAL 等）
"streaming.scan-mode"                // 流扫描模式（COMPACT_BUCKET_TABLE, FILE_MONITOR）
"streaming-read-min-interval"        // 流读最小间隔
"streaming-read-max-interval"        // 流读最大间隔
```

---

## 错误处理

### 常见错误

| 错误 | 原因 | 解决方案 |
|-----|-----|--------|
| `EmptyCommitException` | 提交消息为空 | 检查是否有数据写入，或设置 `ignoreEmptyCommit=true` |
| `CheckpointFailedException` | Checkpoint 失败 | 检查磁盘空间、网络、文件权限 |
| `SnapshotNotFoundException` | 快照不存在 | 检查快照 ID 是否正确，或使用 `checkpoint()` 获取最新 ID |
| `CommitIdentifierMismatchException` | commitId 不匹配 | 确保 commitId 递增，不要重复使用相同 ID |
| `RecoverableException` | 可恢复异常 | 任务会自动重试，检查 Checkpoint 是否正确保存 |

### 异常处理示例

```java
try {
    List<CommitMessage> msgs = write.prepareCommit(false, checkpointId);
    commit.commit(msgs);
} catch (EmptyCommitException e) {
    logger.info("No data to commit: {}", e.getMessage());
} catch (RecoverableException e) {
    logger.warn("Recoverable error, will retry: {}", e.getMessage());
    throw e;  // 让框架重试
} catch (Exception e) {
    logger.error("Fatal error during commit", e);
    throw e;
}
```

---

## 性能调优建议

### 写入优化

```java
// 1. 选择合适的 waitCompaction 值
write.prepareCommit(false, id);  // 高吞吐场景

// 2. 调整缓冲大小
Options opts = new Options();
opts.set("write.buffer-size", "512MB");  // 增加缓冲

// 3. 配置压缩写入器
opts.set("write.num-compact-writers", "4");

// 4. 使用批量写入（如果可能）
for (InternalRow row : batchRows) {
    write.write(row);  // 批量写入更高效
}
```

### 读取优化

```java
// 1. 列裁剪
read.withProjection(new int[]{0, 1, 2});

// 2. 谓词下推
Predicate filter = ...;
read.withFilter(filter).executeFilter();

// 3. 调整扫描间隔
Options opts = new Options();
opts.set("streaming-read-min-interval", "1s");
opts.set("streaming-read-max-interval", "10s");

// 4. 分片处理大数据集
for (Split split : plan.splits()) {
    // 每个 split 单独处理
}
```

---

## 调试技巧

### 启用日志

```xml
<!-- pom.xml 添加日志 -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-log4j12</artifactId>
</dependency>

<!-- log4j.properties -->
log4j.logger.org.apache.paimon=DEBUG
log4j.logger.org.apache.paimon.table.sink=DEBUG
log4j.logger.org.apache.paimon.table.source=DEBUG
```

### 查看元数据

```java
// 查看最新快照
SnapshotManager snapMgr = table.snapshotManager();
Snapshot latest = snapMgr.latestSnapshot();
System.out.println("Latest Snapshot: " + latest.id());

// 查看所有快照
for (long id = 1; id <= latest.id(); id++) {
    Snapshot snapshot = snapMgr.snapshot(id);
    System.out.println("Snapshot " + id + ": " + snapshot.timestamp());
}

// 查看文件信息
ManifestEntry entry = ...;
System.out.println("File: " + entry.file().fileName());
System.out.println("Records: " + entry.file().recordCount());
System.out.println("Size: " + entry.file().fileSize());
```

### 监控指标

```java
// 获取指标
MetricRegistry metrics = ...;
long recordCount = metrics.counter("record.count").getCount();
long fileCount = metrics.counter("file.count").getCount();

// 输出指标
System.out.println("Records written: " + recordCount);
System.out.println("Files created: " + fileCount);
```

---

## 故障排查流程

```
问题: 读写不同步（读取不到最新数据）
↓
1. 检查 commitIdentifier 是否递增
   - write.prepareCommit(false, 1) 然后 write.prepareCommit(false, 2)
↓
2. 检查 commit() 是否成功
   - 查看是否有新的 Snapshot 创建
   - check table/.../snapshots/SNAPSHOT-X
↓
3. 检查 Checkpoint 是否正确保存
   - 读取端 scan.checkpoint() 是否返回正确的 nextSnapshotId
   - 写入端状态是否正确保存
↓
4. 检查是否有网络或磁盘问题
   - 确保文件系统可用
   - 检查网络连接
↓
5. 查看详细日志
   - 启用 DEBUG 日志
   - 查找异常堆栈


问题: 写入性能低下
↓
1. 检查是否在等待压缩
   - 尝试 waitCompaction=false
↓
2. 检查缓冲大小
   - 增加 write.buffer-size（需要足够内存）
↓
3. 检查并行度
   - 增加分桶数
   - 增加压缩写入器数
↓
4. 检查磁盘 I/O
   - 确保数据目录有足够速度的磁盘
   - 检查是否有其他 I/O 密集任务


问题: 读取性能低下
↓
1. 启用列裁剪
   - 只读取需要的列
↓
2. 启用谓词下推
   - 过滤条件在读取时执行
↓
3. 检查分片数
   - 增加并行度
↓
4. 检查快照大小
   - 定期执行主压缩（Full Compaction）
```

---

## 快速检查清单

### 部署前检查

- [ ] 已设置唯一的 `commitUser`
- [ ] `commitIdentifier` 设置为递增值
- [ ] Checkpoint 保存位置可写
- [ ] 磁盘空间充足（至少 2x 预期数据大小）
- [ ] 网络连接稳定
- [ ] 已配置适当的日志级别

### 运行时监控

- [ ] 定期检查写入吞吐
- [ ] 监控 Checkpoint 耗时
- [ ] 检查内存使用情况
- [ ] 验证快照是否定期创建
- [ ] 观察压缩进度

### 故障恢复验证

- [ ] 测试任务重启后数据一致性
- [ ] 验证 Checkpoint 恢复流程
- [ ] 检查重复消息是否被过滤
- [ ] 确认无数据丢失或重复

---

## 参考资源

### 官方文档
- [Paimon Official Site](https://paimon.apache.org/)
- [GitHub Repository](https://github.com/apache/incubator-paimon)

### 源代码位置
- 流写: `paimon-core/src/main/java/org/apache/paimon/table/sink/`
- 流读: `paimon-core/src/main/java/org/apache/paimon/table/source/`
- 操作: `paimon-core/src/main/java/org/apache/paimon/operation/`

### 测试用例参考
- 流写测试: `paimon-core/src/test/java/org/apache/paimon/table/sink/StreamTableWriteTest.java`
- 流读测试: `paimon-core/src/test/java/org/apache/paimon/table/source/StreamTableScanTest.java`

### Flink 集成
- Flink Connector: `paimon-flink/paimon-flink-cdc/`
- Flink Source: `paimon-flink/paimon-flink-core/src/main/java/org/apache/flink/table/connectors/paimon/`

