# Paimon 流读流写 - 源码导航索引

> 这个文档提供了完整的源码文件位置和关键位置导航，便于快速查看实现细节。

## 目录结构

```
paimon-core/src/main/java/org/apache/paimon/
├── table/
│   ├── sink/                          # 流写实现
│   │   ├── StreamWriteBuilder.java
│   │   ├── StreamWriteBuilderImpl.java
│   │   ├── StreamTableWrite.java
│   │   ├── StreamTableCommit.java
│   │   ├── TableWrite.java
│   │   ├── TableWriteImpl.java
│   │   ├── InnerTableWrite.java
│   │   └── ... (其他写入相关类)
│   └── source/                        # 流读实现
│       ├── StreamTableScan.java
│       ├── DataTableStreamScan.java
│       ├── DataTableBatchScan.java
│       ├── TableRead.java
│       ├── AbstractDataTableRead.java
│       ├── KeyValueTableRead.java
│       ├── AppendTableRead.java
│       ├── snapshot/                  # 快照扫描
│       │   ├── StartingScanner.java
│       │   ├── FollowUpScanner.java
│       │   ├── SnapshotReader.java
│       │   ├── DeltaFollowUpScanner.java
│       │   └── ... (其他扫描器)
│       └── splitread/                 # 分片读取
└── operation/                         # 底层操作
    ├── FileStoreWrite.java
    ├── FileStoreScan.java
    ├── FileStoreCommit.java
    └── ... (其他操作类)
```

---

## 流写核心类源码导航

### 1. StreamWriteBuilder 接口
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilder.java`

**关键方法位置**:
```
StreamWriteBuilder
├─ withCommitUser(String) - 第 X 行
├─ newWrite() - 返回 StreamTableWrite
└─ newCommit() - 返回 StreamTableCommit
```

**查看**: [StreamWriteBuilder.java](../../paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilder.java)

---

### 2. StreamWriteBuilderImpl 实现类
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java`

**核心字段**:
| 字段 | 行号 | 说明 |
|-----|------|------|
| `table` | 65 | InnerTable 引用 |
| `commitUser` | 68 | 应用唯一标识 |

**关键方法**:
| 方法 | 行号 | 说明 |
|-----|------|------|
| `StreamWriteBuilderImpl(InnerTable)` | 78 | 构造函数，初始化 commitUser |
| `withCommitUser(String)` | 104 | 设置 commitUser |
| `newWrite()` | 110 | 创建 StreamTableWrite |
| `newCommit()` | 115 | 创建 StreamTableCommit，设置 ignoreEmptyCommit=false |

**重要代码段**:
```java
// 行 80: commitUser 初始化
this.commitUser = createCommitUser(new Options(table.options()));

// 行 117: 流式提交不忽略空提交
return table.newCommit(commitUser).ignoreEmptyCommit(false);
```

**查看源码**: [StreamWriteBuilderImpl.java](../../paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java)

---

### 3. StreamTableWrite 接口
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableWrite.java`

**继承关系** (行 73):
```java
public interface StreamTableWrite extends TableWrite
```

**关键方法**:
| 方法 | 行号 | 返回值 | 说明 |
|-----|------|--------|------|
| `prepareCommit(boolean, long)` | 106 | `List<CommitMessage>` | 准备提交，参数为 waitCompaction 和 commitIdentifier |

**重要参数说明** (行 87-98):
```java
// commitIdentifier 必须满足：
// - 从 0 开始（或任意起始值）
// - 每次提交递增
// - 与 StreamTableCommit#commit 使用相同的值

// 性能考虑：
// - waitCompaction=false: 提交更快，但可能产生更多小文件
// - waitCompaction=true: 提交较慢，但文件更少更大
```

**查看源码**: [StreamTableWrite.java](../../paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableWrite.java)

---

### 4. TableWriteImpl 实现类
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java`

**核心字段** (行 83-110):
| 字段 | 行号 | 说明 |
|-----|------|------|
| `write` | 86 | FileStoreWrite - 底层写入 |
| `keyAndBucketExtractor` | 89 | 提取分区、桶、主键 |
| `recordExtractor` | 92 | 记录转换 |
| `rowKindGenerator` | 95 | 行类型生成（可选） |
| `rowKindFilter` | 98 | 行类型过滤（可选） |
| `batchCommitted` | 101 | 批量提交标志 |

**关键方法查找**:
- `write(InternalRow)` - 查找 "public void write" 方法
- `flush()` - 查找 "public void flush" 方法
- `prepareCommit()` - 流式提交实现

**架构说明** (行 63-73):
```java
// TableWriteImpl 的分层架构：
// TableWriteImpl (Table 层实现)
//   ├─ KeyAndBucketExtractor
//   ├─ RecordExtractor
//   ├─ RowKindGenerator
//   ├─ RowKindFilter
//   └─ FileStoreWrite (底层文件写入)
//       ├─ KeyValueFileStoreWrite
//       └─ AppendOnlyFileStoreWrite
```

**查看源码**: [TableWriteImpl.java](../../paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java)

---

### 5. FileStoreWrite 接口
**文件**: `paimon-core/src/main/java/org/apache/paimon/operation/FileStoreWrite.java`

**关键方法** (行 67-):
| 方法 | 行号 | 说明 |
|-----|------|------|
| `withWriteRestore(WriteRestore)` | 70 | 恢复写入状态 |
| `withMemoryPoolFactory(MemoryPoolFactory)` | 92 | 设置内存池 |
| `withBlobConsumer(BlobConsumer)` | 99 | 设置 Blob 消费者 |

**写入流程** (行 47-54):
```java
// 流程包括：
// 1. 数据路由：根据分区和桶将记录路由到对应的 Writer
// 2. 内存缓冲：数据先写入内存缓冲区（WriteBuffer）
// 3. 文件写入：缓冲区数据刷写成数据文件
// 4. 自动压缩：根据策略触发后台压缩任务
// 5. 准备提交：等待所有异步操作完成，生成 CommitMessage
```

**查看源码**: [FileStoreWrite.java](../../paimon-core/src/main/java/org/apache/paimon/operation/FileStoreWrite.java)

---

## 流读核心类源码导航

### 1. StreamTableScan 接口
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java`

**继承关系** (行 92):
```java
public interface StreamTableScan extends TableScan, Restorable<Long>
```

**关键方法** (行 92-159):
| 方法 | 行号 | 返回值 | 说明 |
|-----|------|--------|------|
| `watermark()` | 107 | `Long` | 获取水位线 |
| `restore(Long)` | 123 | `void` | 从检查点恢复 |
| `checkpoint()` | 142 | `Long` | 获取下一个快照 ID |
| `notifyCheckpointComplete(Long)` | 159 | `void` | 通知完成（清理） |

**重要说明** (行 82-85):
```java
// checkpoint() 返回的是"下一个要读取的快照 ID"，
// 而不是当前正在读取的快照 ID。
// 这样设计是为了避免重复消费。
```

**查看源码**: [StreamTableScan.java](../../paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java)

---

### 2. DataTableStreamScan 实现类
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java`

**核心组件** (行 80-86):
```java
// 用于流式扫描的各个扫描器：
// - StartingScanner: 首次扫描确定起始快照
// - FollowUpScanner: 后续扫描读取增量
// - BoundedChecker: 边界检查
// - NextSnapshotFetcher: 下一个快照获取
```

**流式扫描模式** (行 88-102):
```java
// COMPACT_BUCKET_TABLE (默认):
//   - 读取快照的 Delta 数据（新增/变化的文件）
//   - 适用于主键表和追加表
//
// FILE_MONITOR:
//   - 监控文件系统的变化
//   - 适用于外部系统写入的场景
```

**两阶段扫描** (行 104-119):
```java
// Full Phase（全量阶段）:
//   - 首次启动时，读取起始快照的完整数据
//   - 使用 StartingScanner 确定起始快照
//
// Incremental Phase（增量阶段）:
//   - 持续读取后续快照的变化数据
//   - 使用 FollowUpScanner 跟踪新快照
```

**查看源码**: [DataTableStreamScan.java](../../paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java)

---

### 3. StartingScanner 接口
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/StartingScanner.java`

**关键方法**:
```java
ScannedResult scan(SnapshotReader reader)
// 执行扫描，返回 {plan, nextSnapshotId}
```

**实现类大全**:
| 实现类 | 文件 | 用途 |
|------|------|------|
| `FullStartingScanner` | snapshot/ | 扫描全量快照 |
| `CompactedStartingScanner` | snapshot/ | 扫描已压缩快照 |
| `ContinuousLatestStartingScanner` | snapshot/ | 从最新快照开始 |
| `StaticFromSnapshotStartingScanner` | snapshot/ | 从指定快照开始 |
| `StaticFromTimestampStartingScanner` | snapshot/ | 从时间戳开始 |
| `StaticFromTagStartingScanner` | snapshot/ | 从 Tag 开始 |

**查看源码**: [StartingScanner.java](../../paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/StartingScanner.java)

---

### 4. FollowUpScanner 接口
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/FollowUpScanner.java`

**关键方法**:
```java
ScannedResult scan(SnapshotReader reader)
// 执行增量扫描

boolean hasNextSnapshot(NextSnapshotFetcher fetcher, long currentSnapshotId)
// 检查是否有下一个快照
```

**实现类**:
| 实现类 | 用途 |
|------|------|
| `DeltaFollowUpScanner` | 返回快照间的增量变更 |
| `AllDeltaFollowUpScanner` | 返回所有增量变更 |
| `ChangelogFollowUpScanner` | 返回变更日志 |

**查看源码**: [FollowUpScanner.java](../../paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/FollowUpScanner.java)

---

### 5. AbstractDataTableRead 抽象基类
**文件**: `paimon-core/src/main/java/org/apache/paimon/table/source/AbstractDataTableRead.java`

**架构** (行 47-55):
```java
// InnerTableRead (接口)
//     ↓
// AbstractDataTableRead (抽象基类，通用逻辑)
//     ↓
// ├── KeyValueTableRead (主键表读取)
// └── AppendTableRead (追加表读取)
```

**核心功能** (行 57-63):
| 功能 | 说明 |
|-----|------|
| **过滤执行** | 在读取阶段执行过滤条件（精确过滤） |
| **列裁剪** | 只读取需要的列，减少 IO 量 |
| **查询授权** | 支持细粒度的数据访问控制 |
| **投影下推** | 将列投影下推到底层读取器 |

**关键方法**:
- `withFilter(Predicate)` - 设置过滤条件
- `withProjection(int[])` - 指定列裁剪
- `executeFilter()` - 启用过滤执行
- `createReader(Split)` - 创建记录读取器

**查看源码**: [AbstractDataTableRead.java](../../paimon-core/src/main/java/org/apache/paimon/table/source/AbstractDataTableRead.java)

---

## 关键辅助类源码导航

### CommitMessage 相关

**文件**: `paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessage.java`

**说明**:
- 包含 Checkpoint 中的文件变更信息
- 包括新增文件、删除文件、压缩任务等
- 在 Commit 端被应用以创建新快照

**查看源码**: [CommitMessage.java](../../paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessage.java)

---

### KeyAndBucketExtractor 相关

**文件**: `paimon-core/src/main/java/org/apache/paimon/table/sink/KeyAndBucketExtractor.java`

**职责**:
- 从 InternalRow 中提取分区键
- 从 InternalRow 中提取桶号
- 从 InternalRow 中提取主键

**实现类**:
| 实现类 | 用途 |
|------|------|
| `FixedBucketRowKeyExtractor` | 固定分桶提取 |
| `DynamicBucketRowKeyExtractor` | 动态分桶提取 |
| `PostponeBucketRowKeyExtractor` | 延迟分桶提取 |

**查看源码**: [KeyAndBucketExtractor.java](../../paimon-core/src/main/java/org/apache/paimon/table/sink/KeyAndBucketExtractor.java)

---

### Snapshot 相关

**文件**: `paimon-core/src/main/java/org/apache/paimon/Snapshot.java`

**关键字段**:
| 字段 | 说明 |
|-----|------|
| `id` | 快照 ID（递增） |
| `schemaId` | 表结构版本 |
| `commitUser` | 提交者（对于流式写入） |
| `commitIdentifier` | 提交标识符（对于流式写入） |
| `timestamp` | 快照创建时间 |
| `baseManifestList` | 基础 Manifest 文件列表 |
| `deltaManifestList` | 增量 Manifest 文件列表 |

**查看源码**: [Snapshot.java](../../paimon-core/src/main/java/org/apache/paimon/Snapshot.java)

---

### SnapshotManager 相关

**文件**: `paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java`

**关键方法**:
```java
Snapshot snapshot(long snapshotId)
// 获取指定 ID 的快照

Snapshot latestSnapshot()
// 获取最新快照

long getLatestSnapshotId()
// 获取最新快照 ID
```

**查看源码**: [SnapshotManager.java](../../paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java)

---

## 索引文件位置总表

### 写入相关文件

| 类名 | 文件路径 | 关键行 |
|-----|--------|-------|
| StreamWriteBuilder | `table/sink/StreamWriteBuilder.java` | - |
| StreamWriteBuilderImpl | `table/sink/StreamWriteBuilderImpl.java` | 60-119 |
| StreamTableWrite | `table/sink/StreamTableWrite.java` | 73-108 |
| StreamTableCommit | `table/sink/StreamTableCommit.java` | - |
| TableWrite | `table/sink/TableWrite.java` | - |
| TableWriteImpl | `table/sink/TableWriteImpl.java` | 83-144+ |
| InnerTableWrite | `table/sink/InnerTableWrite.java` | 44-74 |
| CommitMessage | `table/sink/CommitMessage.java` | - |
| KeyAndBucketExtractor | `table/sink/KeyAndBucketExtractor.java` | - |
| FileStoreWrite | `operation/FileStoreWrite.java` | 44-100+ |

### 读取相关文件

| 类名 | 文件路径 | 关键行 |
|-----|--------|-------|
| StreamTableScan | `table/source/StreamTableScan.java` | 92-160 |
| DataTableStreamScan | `table/source/DataTableStreamScan.java` | 55-120+ |
| TableRead | `table/source/TableRead.java` | - |
| AbstractDataTableRead | `table/source/AbstractDataTableRead.java` | 98-100+ |
| KeyValueTableRead | `table/source/KeyValueTableRead.java` | - |
| AppendTableRead | `table/source/AppendTableRead.java` | - |
| StartingScanner | `table/source/snapshot/StartingScanner.java` | - |
| FollowUpScanner | `table/source/snapshot/FollowUpScanner.java` | - |
| SnapshotReader | `table/source/snapshot/SnapshotReader.java` | - |
| DeltaFollowUpScanner | `table/source/snapshot/DeltaFollowUpScanner.java` | - |

### 操作相关文件

| 类名 | 文件路径 |
|-----|--------|
| FileStoreWrite | `operation/FileStoreWrite.java` |
| FileStoreScan | `operation/FileStoreScan.java` |
| FileStoreCommit | `operation/FileStoreCommit.java` |
| FileStoreCommitImpl | `operation/FileStoreCommitImpl.java` |

### 辅助类文件

| 类名 | 文件路径 |
|-----|--------|
| Snapshot | `Snapshot.java` |
| SnapshotManager | `utils/SnapshotManager.java` |
| CommitIncrement | `utils/CommitIncrement.java` |
| RecordWriter | `utils/RecordWriter.java` |

---

## 快速查找技巧

### 按功能快速查找

**想查看写入流程的实现**：
1. 从 `StreamWriteBuilderImpl.java` (行 60-119) 开始
2. 跳转到 `TableWriteImpl.java` (行 83+) 看写入实现
3. 查看 `FileStoreWrite.java` (行 44-54) 的写入流程

**想查看读取流程的实现**：
1. 从 `StreamTableScan.java` (行 92-160) 开始了解接口
2. 跳转到 `DataTableStreamScan.java` (行 55+) 看实现
3. 查看 `StartingScanner.java` 和 `FollowUpScanner.java`

**想查看 Checkpoint 实现**：
1. 查看 `StreamTableWrite.java` (行 106) 的 `prepareCommit()` 方法
2. 跳转到 `TableWriteImpl.java` 看实现细节
3. 查看 `CommitMessage.java` 的消息结构

**想查看快照管理**：
1. 查看 `Snapshot.java` 的快照结构
2. 查看 `SnapshotManager.java` 的快照操作
3. 查看 `DataTableStreamScan.java` 中快照的使用

---

## VS Code 快速打开技巧

在 VS Code 中快速打开文件：

```
按 Ctrl+P，输入文件名：

StreamWriteBuilderImpl        # 快速打开实现类
StreamTableWrite             # 快速打开接口
TableWriteImpl                # 快速打开写入实现
DataTableStreamScan          # 快速打开扫描实现
StartingScanner              # 快速打开起始扫描
```

在 VS Code 中快速跳转到行：

```
打开文件后按 Ctrl+G，输入行号：

60  # 跳转到第 60 行
119 # 跳转到第 119 行
```

---

## Git 命令快速查看

```bash
# 查看文件历史和改动
git log -p paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java

# 查看特定提交的改动
git show <commit-hash>:paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java

# 查看最近修改该文件的人
git blame paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java

# 搜索包含特定字符串的提交
git log -S "commitUser" -- "*.java"

# 查看文件的完整路径（方便复制）
git ls-files | grep StreamWriteBuilderImpl
```

---

## 调试技巧

### 在 IDE 中添加书签

对于常用的源文件，在 IDE 中添加书签：

**IntelliJ IDEA**:
1. 打开要标记的文件
2. 在行号上右键 → Bookmark Line
3. 给书签起名（如 "StreamWrite Initial"）
4. 在 Bookmarks 窗口快速跳转

**VS Code**:
1. 安装 Bookmarks 扩展
2. 在需要的行上 Ctrl+Alt+K 添加书签
3. 使用 Bookmarks 视图快速跳转

### 关键方法断点设置

```java
// 断点建议位置：

// 1. 写入入口（TableWriteImpl.java）
write.write(row)                // 查看行数据如何处理

// 2. 分桶提取（KeyAndBucketExtractor.java）
keyAndBucketExtractor.extract() // 查看分区/桶提取

// 3. 文件写入（FileStoreWrite.java）
write.write()                   // 查看底层写入

// 4. Checkpoint 提交（TableWriteImpl.java）
prepareCommit()                 // 查看消息生成

// 5. 扫描入口（DataTableStreamScan.java）
plan()                          // 查看扫描过程

// 6. 数据读取（AbstractDataTableRead.java）
next()                          // 查看行数据读取
```

---

## 注意事项

⚠️ **行号可能变化**：
- 不同版本的 Paimon 行号可能不同
- 建议使用搜索功能而不是直接跳转行号
- 使用类名和方法名进行搜索更稳定

✅ **最佳实践**：
1. 使用 IDE 的 "Find in Files" (Ctrl+Shift+F) 搜索类和方法
2. 使用 IDE 的 "Go to Definition" (F12) 快速跳转
3. 使用源码注释和 JavaDoc 理解设计意图
4. 查看单元测试理解使用方式

