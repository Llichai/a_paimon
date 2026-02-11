# Apache Paimon 代码库中文注释项目 - 总体进度

## 项目概览
为 Apache Paimon 代码库的核心模块添加详细的中文注释。

## 总体目标
**总文件数**: 1541个文件
- paimon-core: 767个文件
- paimon-common: 575个文件
- paimon-api: 199个文件

## 完成进度

### 批次1: paimon-core/mergetree/compact/aggregate ✅
- **状态**: 已完成
- **文件数**: 45/45 (100%)
- **内容**: 聚合函数工厂和实现类

### 批次2: paimon-core/mergetree/compact ✅
- **状态**: 已完成
- **文件数**: 33/33 (100%)
- **内容**: 压缩策略、合并函数、重写器

### 批次3: paimon-core/mergetree（主包） ✅
- **状态**: 已完成
- **文件数**: 27/27 (100%)
- **内容**:
  - 核心类（Levels, SortedRun, LevelSortedRun）
  - 读取器（DataFileReader, DropDeleteReader, MergeTreeReaders）
  - 写入器（MergeTreeWriter - 645行核心写入器）
  - 排序器（MergeSorter - 266行归并排序器）
  - 写入缓冲区（WriteBuffer, SortBufferWriteBuffer）
  - 本地合并（LocalMerger, HashMapLocalMerger, SortBufferLocalMerger）
  - Lookup相关（14个文件全部完成）

### 批次4: paimon-core/disk（磁盘I/O管理） ✅
- **状态**: 已完成
- **文件数**: 19/19 (100%)
- **内容**:
  - I/O管理器（IOManager, IOManagerImpl）
  - 文件通道（FileIOChannel, AbstractFileIOChannel, FileChannelManager等）
  - 缓冲区（RowBuffer, InMemoryBuffer, ExternalBuffer）
  - 通道读写器（9个文件）
    - ChannelReaderInputView - 分块读取+自动解压
    - ChannelWriterOutputView - 分块写入+自动压缩
    - BufferFileWriter/Reader - 缓冲文件读写
  - 零拷贝溢写机制

### 批次5: paimon-core/io（文件I/O） ✅
- **状态**: 已完成
- **文件数**: 39/39 (100%)
- **内容**:
  - 元数据类（DataFileMeta, PojoDataFileMeta）
  - 序列化器（7个版本序列化器）
  - 增量数据（DataIncrement, CompactIncrement）
  - 文件路径工厂（2个文件）
  - 文件读取器（6个文件）
  - 文件写入器（13个文件）
    - KeyValue写入器（标准模式和精简模式）
    - 行数据写入器（Append-Only表）
    - 格式表写入器（Format Table）
  - 索引和统计（DataFileIndexWriter, SimpleStatsProducer）
  - 工具类（RecordLevelExpire, FileWriterContext等）

### 批次6: paimon-core/operation（核心操作） ✅
- **状态**: 已完成
- **文件数**: 36/36 (100%)
- **内容**:
  - **核心接口**（3个）：
    - FileStoreScan - 三种扫描模式
    - FileStoreWrite - 写入流程和内存管理
    - FileStoreCommit - 两阶段提交和冲突检测
  - **扫描实现**（5个）：
    - 合并扫描、非合并扫描
    - KeyValue扫描、Append-Only扫描
    - 数据演化扫描
  - **写入实现**（8个）：
    - 追加写入、分桶写入
    - 内存管理、捆绑写入
    - 故障恢复机制
  - **提交实现**（1个，核心）：
    - FileStoreCommitImpl - 详细的 commit() 方法注释（400+行）
    - 两阶段提交协议
    - 冲突检测和重试机制
  - **文件管理**（8个）：
    - 快照删除、Changelog删除、标签删除
    - 分区过期
    - 孤儿文件清理（本地和分布式）
  - **读取器**（4个）：
    - 分片读取、合并读取
    - 原始文件读取、数据演化读取
  - **辅助工具**（7个）：
    - 分布式锁、Manifest合并
    - 文件恢复、桶选择
    - 反向读取、一致性检查

### 批次12: paimon-core/utils（工具包） ✅
- **状态**: 已完成
- **文件数**: 52/52 (100%)
- **内容**:
  - 路径管理（FileStorePathFactory等4个）
  - 快照和分支管理（SnapshotManager, BranchManager等8个）
  - Changelog管理（ChangelogManager等3个）
  - 序列化工具（ObjectSerializer等6个）
  - 读写工具（AsyncRecordReader等8个）
  - 缓存工具（SegmentsCache, DVMetaCache等5个）
  - 文件工具（FileUtils, CompressUtils等6个）
  - 线程池和其他工具（12个）

### 批次13: paimon-core/table（主包） ✅
- **状态**: 已完成
- **文件数**: 24/24 (100%)
- **内容**:
  - **Table 抽象层**（11个）：
    - 核心接口（Table, DataTable, InnerTable, FileStoreTable）
    - 抽象基类（AbstractFileStoreTable）
    - 表实现（PrimaryKeyFileStoreTable, AppendOnlyFileStoreTable）
    - 表工厂（FileStoreTableFactory）
    - 环境和规格（CatalogEnvironment, BucketSpec）
    - 快照过期（ExpireSnapshots）
  - **特殊表实现**（7个）：
    - FormatTable（外部格式表）
    - ReadonlyTable（只读表）
    - DelegatedFileStoreTable（委托表）
    - FallbackReadFileStoreTable（回退读取）
    - ChainGroupReadTable（链式分组读取）
    - KnownSplitsTable（已知分片，Spark优化）
    - VectorSearchTable（向量搜索）
  - **管理和辅助**（6个）：
    - 快照过期实现（ExpireSnapshotsImpl, ExpireChangelogImpl）
    - 分区处理（PartitionHandler）
    - 回滚辅助（RollbackHelper）
    - 工具类（PrimaryKeyTableUtils, PostponeUtils）

### 批次14: paimon-core/table/format（Format表） ✅
- **状态**: 已完成
- **文件数**: 11/11 (100%)
- **内容**:
  - **Format 表读写**（9个）：
    - 读取构建器（FormatReadBuilder）
    - 扫描和读取（FormatTableScan, FormatTableRead）
    - 数据分片（FormatDataSplit）
    - 写入构建器（FormatBatchWriteBuilder）
    - 写入实现（FormatTableWrite, FormatTableRecordWriter, FormatTableFileWriter）
    - 提交（FormatTableCommit）
  - **辅助类**（2个）：
    - 两阶段提交消息（TwoPhaseCommitMessage）
    - 谓词工具（PredicateUtils）

### 批次15: paimon-core/table/sink（表写入和提交） ✅
- **状态**: 已完成
- **文件数**: 39/39 (100%)
- **内容**:
  - **核心接口和构建器**（6个）：
    - 写入和提交接口（TableWrite, TableCommit）
    - 写入构建器（WriteBuilder, BatchWriteBuilder, StreamWriteBuilder）
    - 内部表写入（InnerTableWrite）
  - **核心实现**（4个）：
    - 表写入实现（TableWriteImpl）
    - 表提交实现（TableCommitImpl）
    - 构建器实现（BatchWriteBuilderImpl, StreamWriteBuilderImpl）
  - **批量和流式**（4个）：
    - 批量写入提交（BatchTableWrite, BatchTableCommit）
    - 流式写入提交（StreamTableWrite, StreamTableCommit）
  - **CommitMessage**（4个）：
    - 提交消息和序列化（CommitMessage, CommitMessageImpl）
    - 序列化器（CommitMessageSerializer, CommitMessageLegacyV2Serializer）
  - **RowKeyExtractor**（9个）：
    - 五种分桶模式的行键提取器
    - HASH_FIXED, HASH_DYNAMIC, KEY_DYNAMIC, POSTPONE_MODE, BUCKET_UNAWARE
  - **辅助类**（12个）：
    - 分布式写入（ChannelComputer, WriteSelector）
    - 回调机制（CommitCallback, TagCallback, CallbackUtils）
    - 其他工具（SinkRecord, RowKindGenerator等）

### 批次16: paimon-core/table/source（表读取和扫描） ✅
- **状态**: 已完成
- **文件数**: 78/78 (100%)
- **内容**:
  - **source 主包**（40个）：
    - 核心接口（TableScan, TableRead, InnerTableScan, InnerTableRead）
    - 扫描实现（AbstractDataTableScan, DataTableBatchScan, DataTableStreamScan等）
    - 读取实现（AbstractDataTableRead, KeyValueTableRead, AppendTableRead）
    - 读取构建器（ReadBuilder, ReadBuilderImpl）
    - 分片相关（Split, DataSplit, IncrementalSplit, ChainSplit等）
    - 分片生成（SplitGenerator, MergeTreeSplitGenerator, AppendOnlySplitGenerator等）
    - 计划和授权（DataFilePlan, PlanImpl, TableQueryAuth等）
    - 辅助工具（PushDownUtils, TopNDataSplitEvaluator等）
  - **snapshot 子包**（28个）：
    - 核心接口（SnapshotReader, StartingScanner, FollowUpScanner）
    - StartingScanner 实现（17个：Full/Compacted/Static/Continuous/Incremental）
    - FollowUpScanner 实现（3个：Delta/AllDelta/Changelog）
    - 辅助类（StartingContext, BoundedChecker, TimeTravelUtil）
  - **splitread 子包**（10个）：
    - 核心接口（SplitReadProvider, SplitReadConfig）
    - 原始文件读取（RawFileSplitReadProvider系列）
    - 合并文件读取（MergeFileSplitReadProvider）
    - 增量读取（IncrementalChangelogReadProvider, IncrementalDiffReadProvider）
    - 数据演化读取（DataEvolutionSplitReadProvider）

### 批次17: paimon-core/table 剩余子包 ✅
- **状态**: 已完成
- **文件数**: 32/32 (100%)
- **内容**:
  - **query 包**（2个）：TableQuery, LocalTableQuery
  - **iceberg 包**（2个）：IcebergTable, IcebergTableImpl
  - **lance 包**（2个）：LanceTable, LanceTableImpl
  - **object 包**（2个）：ObjectTable, ObjectTableImpl
  - **system 包**（24个）：
    - 全局系统表（AllTablesTable, AllPartitionsTable, CatalogOptionsTable等）
    - 表级系统表（OptionsTable, SchemasTable, SnapshotsTable, ManifestsTable等）
    - 数据文件表（FilesTable, PartitionsTable, BucketsTable等）
    - 版本管理（TagsTable, BranchesTable）
    - 变更日志（AuditLogTable, BinlogTable）
    - 监控优化（FileMonitorTable, ReadOptimizedTable等）

### 批次18: paimon-core 中小型包合集 ✅
- **状态**: 已完成
- **文件数**: 96/96 (100%)
- **内容**:
  - **stats 包**（8个）：列统计、简单统计、统计文件处理
  - **tag 包**（10个）：标签管理、自动创建、时间过期
  - **partition 包**（7个）：分区过期策略、分区谓词、时间提取
  - **sort 包**（13个）：外部排序、堆排序、快速排序、溢出管理
  - **index 包**（18个）：索引文件、Bucket分配、索引元数据
  - **privilege 包**（14个）：权限管理、权限检查、特权包装
  - **deletionvectors 包**（12个）：删除向量、DV维护、DV应用
  - **memory 包**（3个）：缓冲区、内存所有者、内存池
  - **consumer 包**（2个）：消费者管理
  - **format 包**（2个）：格式发现
  - **codegen 包**（1个）：代码生成工具

## 统计汇总
**已完成文件**: 621个
**完成率**: 40.3% (621/1541)
**paimon-core完成率**: 81.0% (621/767)

## 批次进度详情

| 批次 | 模块 | 文件数 | 完成数 | 完成率 | 状态 |
|------|------|--------|--------|--------|------|
| 1 | mergetree/compact/aggregate | 45 | 45 | 100% | ✅ |
| 2 | mergetree/compact | 33 | 33 | 100% | ✅ |
| 3 | mergetree | 27 | 27 | 100% | ✅ |
| 4 | disk | 19 | 19 | 100% | ✅ |
| 5 | io | 39 | 39 | 100% | ✅ |
| 6 | operation | 36 | 36 | 100% | ✅ |
| 7 | paimon-core根目录 | 8 | 8 | 100% | ✅ |
| 8 | manifest | 27 | 27 | 100% | ✅ |
| 9 | catalog | 22 | 22 | 100% | ✅ |
| 10 | append | 23 | 23 | 100% | ✅ |
| 11 | schema + bucket | 10 | 10 | 100% | ✅ |
| 12 | utils | 52 | 52 | 100% | ✅ |
| 13 | table（主包） | 24 | 24 | 100% | ✅ |
| 14 | table/format | 11 | 11 | 100% | ✅ |
| 15 | table/sink | 39 | 39 | 100% | ✅ |
| 16 | table/source（部分） | 30 | 30 | 100% | 🔄 |
| **总计** | | **445** | **445** | **100%** | |

## 技术亮点总结

### Batch 1-3: MergeTree 完整体系
- LSM-Tree 核心实现
- 多种合并函数（58个聚合函数）
- 压缩策略（Universal、ForceUp、EarlyFull）
- Lookup 机制（LookupLevels, LookupStrategy）

### Batch 5: 文件I/O完整体系
- **写入器体系**:
  - 标准模式 vs 精简模式（Thin Mode）
  - Primary Key表 vs Append-Only表
  - 滚动写入机制
- **统计信息**:
  - Collector模式（Avro）
  - Extractor模式（Parquet/ORC）
- **索引支持**:
  - Bloom Filter、Bitmap、Hash Index
  - 嵌入式 vs 独立文件存储
- **记录级过期**:
  - 文件级判断
  - 记录级过滤
  - 模式演化支持

### Batch 6: 核心操作完整体系 ⭐ **重点批次**
- **扫描体系**:
  - 三种扫描模式（BATCH, INCREMENTAL, STREAMING）
  - 双重过滤机制（分区+桶）
  - 数据演化扫描（跨Schema版本）
- **写入体系**:
  - 写入器容器管理
  - 内存共享和溢写
  - 批量写入优化
  - 故障恢复机制
- **提交体系** ⭐ **核心亮点**:
  - 两阶段提交协议（详细注释400+行）
  - 三种冲突检测策略
  - 重试和超时机制
  - 快照管理和标记
  - 分区覆盖和删除
- **文件管理**:
  - 快照生命周期管理
  - Changelog清理策略
  - 标签管理
  - 孤儿文件识别和清理
- **读取器**:
  - 分片读取策略
  - 合并读取优化
  - 数据演化读取（Blob文件支持）
- **辅助工具**:
  - 分布式锁机制
  - Manifest文件合并优化
  - 文件一致性检查

### Batch 7: paimon-core 根目录核心文件 ⭐ **架构核心**
- **FileStore 体系**:
  - FileStore 接口（顶层抽象）
  - AbstractFileStore（通用实现）
  - KeyValueFileStore（Primary Key 表）
  - AppendOnlyFileStore（Append-Only 表）
- **分桶模式完整体系**:
  - HASH_FIXED（固定哈希分桶）
  - HASH_DYNAMIC（动态哈希分桶）
  - KEY_DYNAMIC（主键动态分桶，跨分区更新）
  - BUCKET_UNAWARE（无分桶，Append-Only 专用）
  - POSTPONE_MODE（延迟分桶模式）
- **核心数据结构**:
  - KeyValue（5 字段：key, seq, kind, value, level）
  - Changelog（独立生命周期的变更日志）
- **序列化体系**:
  - KeyValueSerializer（标准模式：[key, seq, kind, value]）
  - KeyValueThinSerializer（精简模式：[seq, kind, value]，节省 30-50% 空间）
- **外部存储支持**:
  - 多存储介质路径（HDFS、OSS、S3）
  - 路由策略（SPECIFIC_FS/ALL_FS）
- **Commit Callback 机制**:
  - 分区元数据更新
  - Iceberg 兼容
  - 链式表覆盖
  - 自定义 Callback

### Batch 8: paimon-core/manifest（元数据管理） ⭐ **元数据核心**
- **三层元数据结构**:
  - Snapshot → ManifestList → ManifestFile → DataFileMeta
  - 层次清晰，职责分离
- **三种 Manifest 类型**:
  - Base Manifest（基础数据，所有有效文件）
  - Delta Manifest（增量数据，本次提交变更）
  - Changelog Manifest（变更日志，CDC 支持）
- **索引 Manifest 独立管理**:
  - IndexManifestEntry（索引条目）
  - IndexManifestFile（索引文件读写）
  - IndexManifestFileHandler（三种合并器）
    - DeletionVectorIndexMerger
    - BloomFilterIndexMerger
    - GlobalIndexMerger
- **FileEntry 完整继承体系**:
  - FileEntry → PartitionEntry → BucketEntry → SimpleFileEntry
  - SimpleFileEntryWithDV（Copy-On-Write 删除向量）
- **四级过滤优化**:
  - 分区过滤 → 桶过滤 → 层级过滤 → 文件名过滤
  - BucketFilter（四种策略）
- **并行读取优化**:
  - ManifestEntrySegments（分段读取）
  - ManifestEntryCache（线程安全缓存）
  - 裁剪优化（tryTrim）
- **序列化版本演化**:
  - ManifestEntrySerializer
  - ManifestFileMetaSerializer
  - ManifestCommittableSerializer
  - 向后兼容，字段自动处理

### Batch 9: paimon-core/catalog（Catalog 管理） ⭐ **元数据顶层**
- **Catalog 层次结构**:
  - Catalog → Database → Table
  - 三层管理，职责清晰
- **核心功能模块**:
  - 数据库管理（createDatabase, dropDatabase）
  - 表管理（createTable, dropTable, alterTable）
  - 分区管理（dropPartition, listPartitions）
  - 版本管理（createTag, deleteTag, rollbackTo）
- **FileSystemCatalog 实现**:
  - 目录结构：warehouse/database.db/table_name
  - 元数据存储：Schema 文件、Snapshot 文件、Manifest 文件
  - 支持多文件系统（HDFS、S3、OSS、本地）
- **CachingCatalog 缓存优化**:
  - 三种缓存策略（读后过期、写后过期、软引用）
  - 缓存内容（Table、Schema、分区列表）
  - 失效策略（主动失效、自动过期、容量限制）
- **分布式锁机制**:
  - 锁粒度（Catalog、Database、Table）
  - 锁实现（NoLock、HiveLock、ZookeeperLock、JdbcLock）
  - 死锁避免（按序获取、锁超时、自动释放）
- **RenamingSnapshotCommit**:
  - 原子性保证（atomic rename）
  - 对象存储优化（两阶段提交）
  - 锁集成、回滚支持
- **设计模式应用**:
  - 工厂模式（CatalogFactory、CatalogLockFactory）
  - 装饰器模式（CachingCatalog、DelegateCatalog）
  - 模板方法模式（AbstractCatalog）
  - 加载器模式（CatalogLoader）

### Batch 10: paimon-core/append（Append-Only 表） ⭐ **追加写入**
- **Append-Only 表特点**:
  - 无需主键，只追加
  - 高吞吐量（无合并开销）
  - 不支持更新删除
  - 适用场景：日志、事件流、审计数据
- **自动压缩机制**:
  - 触发条件（小文件数量、文件大小）
  - 后台压缩（AppendCompactCoordinator）
  - 提交前压缩（AppendPreCommitCompactCoordinator）
  - 删除向量支持
- **Blob 字段分离**:
  - 大字段单独存储
  - MultipleBlobFileWriter（多 Blob 管理）
  - RollingBlobFileWriter（滚动写入）
  - 行数一致性校验
- **数据聚类**:
  - Hilbert 曲线排序（最优空间局部性）
  - Z-Order 曲线排序（计算简单）
  - Order 排序（单字段优化）
  - 层级结构（BucketedAppendLevels）
  - 查询性能提升（多维范围查询）
- **数据演化压缩**:
  - 跨 Schema 版本压缩
  - DataEvolutionCompactCoordinator
  - 类型转换和字段映射
- **分桶压缩**:
  - BucketedAppendCompactManager
  - 每个桶独立压缩
  - 并行压缩优化

### Batch 11: paimon-core/schema + bucket（Schema 演化和分桶函数） ⭐ **Schema 管理**
- **Schema 管理完整体系**:
  - SchemaManager（Schema 版本管理）
  - SchemaValidation（验证机制）
  - SchemaMergingUtils（自动演化合并）
  - SchemaEvolutionUtil（IndexCastMapping，跨版本读取）
- **KeyValue 字段提取**:
  - KeyValueFieldsExtractor（提取 Key 和 Value 字段）
  - Thin Mode 优化（节省 30-50% 空间）
- **分桶函数**:
  - DefaultBucketFunction（Murmur3 哈希，雪崩效应）
  - ModBucketFunction（简单取模，适合连续整数）
- **嵌套类型支持**:
  - NestedSchemaUtils（ROW、ARRAY、MAP 演化）
  - IndexCastMapping（嵌套字段映射）

### Batch 12: paimon-core/utils（工具包完整体系） ⭐ **基础设施**
- **路径管理**:
  - FileStorePathFactory（数据文件、Manifest、索引路径）
  - DataFilePathFactories（缓存和优化）
- **快照和分支管理**:
  - SnapshotManager（快照生命周期，LATEST/EARLIEST 快速访问）
  - BranchManager（分支创建、删除、合并）
  - TagManager（标签管理）
- **Changelog 管理**:
  - ChangelogManager（独立生命周期）
  - CompactedChangelogPathResolver（压缩 Changelog 路径）
- **序列化和缓存**:
  - ObjectSerializer（序列化基类）
  - SegmentsCache（分段缓存，LRU 淘汰）
  - DVMetaCache（删除向量缓存）
- **异步读取**:
  - AsyncRecordReader（线程池异步读取）
  - ManifestReadThreadPool（并行 Manifest 读取）
- **零拷贝优化**:
  - OffsetRow（零拷贝行包装）
  - PartialRow（部分字段访问）

### Batch 13: paimon-core/table（Table 抽象层） ⭐ **顶层 API**
- **Table 接口层次**:
  - Table（顶层接口，标签、分支、快照过期）
  - InnerTable（内部接口，Scan、Read、Write、Commit）
  - DataTable（数据表，SnapshotManager、SchemaManager）
  - FileStoreTable（FileStore 表，缓存管理、表复制）
  - AbstractFileStoreTable（抽象基类，750+ 行详细注释）
- **两种表类型**:
  - PrimaryKeyFileStoreTable（主键表，KeyValueFileStore）
  - AppendOnlyFileStoreTable（追加表，AppendOnlyFileStore）
  - 对比表格：主键、更新删除、谓词下推、Compaction
- **特殊表实现**:
  - FormatTable（外部格式表，ORC/Parquet/CSV/JSON/TEXT）
  - ReadonlyTable（只读表，接口隔离原则）
  - DelegatedFileStoreTable（委托模式）
  - FallbackReadFileStoreTable（分区级回退，主分支 → 回退分支）
  - ChainGroupReadTable（文件级链式合并，快照 + 增量）
  - KnownSplitsTable（Spark 优化，避免重复扫描）
  - VectorSearchTable（向量搜索下推）
- **快照生命周期管理**:
  - ExpireSnapshotsImpl（时间策略、数量策略、Tag/Consumer 保护）
  - ExpireChangelogImpl（Changelog 独立过期）
- **回滚和分桶**:
  - RollbackHelper（原子性回滚，数据一致性）
  - PostponeUtils（延迟分桶，动态确定分桶数）
- **设计模式应用**:
  - 接口隔离、委托、装饰器、工厂、模板方法、策略模式

### Batch 14: paimon-core/table/format（Format 表） ⭐ **外部格式支持**
- **FormatTable 特点**:
  - 无快照管理（不生成 Snapshot 和 Manifest）
  - 无 MergeTree（不需要 LSM-Tree 合并）
  - 无 Compaction（每次写入都是新文件）
  - 支持格式：ORC、Parquet、CSV、JSON、TEXT
- **读取优化**:
  - 分区裁剪（等值条件前缀优化，谓词下推到目录遍历）
  - 文件拆分（CSV、JSON 支持范围读取）
  - 谓词下推（排除分区列，推到文件格式层）
- **写入机制**:
  - 滚动写入（文件达到 targetFileSize 时创建新文件）
  - 两阶段提交（.tmp 临时文件 → 原子重命名）
- **覆盖写入**:
  - 静态分区覆盖（指定分区）
  - 动态分区覆盖（所有写入的分区）
- **Hive 集成**:
  - 可选同步分区到 Hive Metastore
  - 支持两种分区路径格式

### Batch 15: paimon-core/table/sink（表写入和提交） ⭐ **用户层 API**
- **Table 层写入架构**:
  - WriteBuilder（批量/流式构建器）
  - TableWrite（写入接口，封装 FileStoreWrite）
  - TableCommit（提交接口，封装 FileStoreCommit）
- **批量 vs 流式写入**:
  - 批量：固定 CommitIdentifier，一次提交，适合 ETL
  - 流式：递增 CommitIdentifier，多次提交，支持状态恢复，适合实时数据接入
- **五种分桶模式的行键提取**:
  - HASH_FIXED（FixedBucketRowKeyExtractor，固定哈希分桶）
  - HASH_DYNAMIC（DynamicBucketRowKeyExtractor，动态哈希分桶）
  - KEY_DYNAMIC（RowPartitionAllPrimaryKeyExtractor，主键动态分桶）
  - POSTPONE_MODE（PostponeBucketRowKeyExtractor，延迟分桶）
  - BUCKET_UNAWARE（AppendTableRowKeyExtractor，无分桶）
- **分布式写入**:
  - ChannelComputer（通道路由：channel = (hash(partition) + bucket) % numChannels）
  - WriteSelector（写入选择器，固定分桶优化）
- **回调机制**:
  - CommitCallback（保证调用，幂等性要求，Hive 分区同步）
  - TagCallback（不保证调用，建议幂等，外部系统通知）
- **CDC 支持**:
  - RowKindGenerator（从数据字段提取 RowKind：'I', '+I', 'U', '+U', '-U', 'D', '-D'）
- **提交后维护**:
  - 标签自动创建 → 标签过期 → 分区过期 → 消费者过期 → 快照过期

## 下一步计划
1. ✅ 完成批次4（disk包，19个文件）
2. ✅ 完成批次5（io包，39个文件）
3. ✅ 完成批次6（operation包，36个文件）
4. ✅ 完成批次7（paimon-core根目录，8个文件）
5. ✅ 完成批次8（manifest包，27个文件）
6. ✅ 完成批次9（catalog包，22个文件）
7. ✅ 完成批次10（append包，23个文件）
8. ✅ 完成批次11（schema + bucket包，10个文件）
9. ✅ 完成批次12（utils包，52个文件）
10. ✅ 完成批次13（table主包，24个文件）
11. ✅ 完成批次14（table/format包，11个文件）
12. ✅ 完成批次15（table/sink包，39个文件）
13. 继续处理 paimon-core 其他核心包（table/source等）
14. 处理 paimon-common 模块
15. 处理 paimon-api 模块

## 注释质量标准
- ✅ JavaDoc 格式（类和方法）
- ✅ 内联注释（复杂逻辑）
- ✅ 中文说明
- ✅ 使用场景和示例
- ✅ 算法和数据结构说明

**最后更新**: 2026-02-11 (Batch 15 完成)
