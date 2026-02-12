# Apache Paimon 中文注释项目 - 技术文档索引

> **最后更新**: 2026-02-12
> **项目进度**: 844/1541 文件已完成 (54.8%)
> **文档版本**: v1.0

---

## 📚 索引导航

- [1. 核心技术主题索引](#1-核心技术主题索引)
- [2. 按包组织的文档导航](#2-按包组织的文档导航)
- [3. 重要文件快速查找](#3-重要文件快速查找)
- [4. 学习路径建议](#4-学习路径建议)
- [5. 技术对比和架构图](#5-技术对比和架构图)

---

## 1. 核心技术主题索引

### 1.1 LSM-Tree 和 MergeTree 实现

#### 核心概念
- **LSM-Tree 架构**: Log-Structured Merge-Tree，分层存储优化写入性能
- **Levels 管理**: 多层级数据组织，从 Level 0（最新）到 Level N（最旧）
- **SortedRun**: 每层内的有序文件集合

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/mergetree/Levels.java` | 多层级管理、层级选择策略 | ✅ 完整 |
| `paimon-core/mergetree/SortedRun.java` | 有序运行集合、文件组织 | ✅ 完整 |
| `paimon-core/mergetree/LevelSortedRun.java` | 单层级的 SortedRun 实现 | ✅ 完整 |
| `paimon-core/mergetree/MergeTreeWriter.java` | 写入器核心逻辑（645行） | ✅ 完整 |
| `paimon-core/mergetree/WriteBuffer.java` | 写入缓冲区、内存管理 | ✅ 完整 |

#### 技术亮点
- **三种压缩策略**:
  - `UniversalCompaction`: 通用策略，适合各种场景
  - `ForceUpLevel0Compaction`: 强制提升 Level 0，减少读放大
  - `EarlyFullCompaction`: 早期全量压缩，优化小表性能
- **58 个聚合函数**: SUM、MAX、MIN、COLLECT、LISTAGG 等
- **Lookup 机制**: 快速查找键值对，支持 RocksDB 和内存存储

#### 相关文档
- 📄 `paimon-annotation/BATCH1_PROGRESS.md` - 聚合函数详细文档
- 📄 `paimon-annotation/BATCH2_PROGRESS.md` - 压缩策略详细文档
- 📄 `paimon-annotation/BATCH3_PROGRESS.md` - MergeTree 主包文档

---

### 1.2 两阶段提交协议

#### 核心概念
- **Prepare 阶段**: 生成 CommitMessage，包含待提交的数据文件元信息
- **Commit 阶段**: 原子性写入 Snapshot，更新 ManifestList

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/operation/FileStoreCommitImpl.java` | 两阶段提交实现（400+行注释） | ✅ 完整 |
| `paimon-core/table/sink/TableCommitImpl.java` | Table 层提交封装 | ✅ 完整 |
| `paimon-core/catalog/RenamingSnapshotCommit.java` | 原子性快照提交 | ✅ 完整 |

#### 提交流程图
```
1. Prepare 阶段
   ↓
   Writer.prepareCommit()
   ↓
   生成 CommitMessage（包含新增/删除文件）
   ↓
2. Commit 阶段
   ↓
   Commit.commit(messages)
   ↓
   冲突检测（三种策略）
   ├── Snapshot ID 检查
   ├── 分区冲突检查
   └── Append-Only 冲突检查
   ↓
   写入 ManifestList
   ↓
   原子性写入 Snapshot
   ↓
   触发 CommitCallback
```

#### 冲突检测策略
1. **Snapshot ID 检测**: 检查是否有其他提交已生成新快照
2. **分区级检测**: 仅检查本次写入的分区是否冲突
3. **Append-Only 检测**: 追加表的宽松检测

#### 相关文档
- 📄 `paimon-annotation/BATCH6_PROGRESS.md` - FileStore 操作详细文档

---

### 1.3 五种分桶模式

#### 对比表格
| 分桶模式 | 桶数量 | 扩容能力 | 典型场景 | 实现类 |
|---------|-------|---------|---------|--------|
| **HASH_FIXED** | 固定 | ❌ 不支持 | 数据均匀分布 | `FixedBucketRowKeyExtractor` |
| **HASH_DYNAMIC** | 动态增长 | ✅ 支持 | 数据倾斜场景 | `DynamicBucketRowKeyExtractor` |
| **KEY_DYNAMIC** | 按主键动态 | ✅ 支持 | 跨分区主键更新 | `RowPartitionAllPrimaryKeyExtractor` |
| **POSTPONE_MODE** | 延迟决策 | ✅ 支持 | 全局优化分桶 | `PostponeBucketRowKeyExtractor` |
| **BUCKET_UNAWARE** | 固定为 0 | ❌ 不支持 | Append-Only 表 | `AppendTableRowKeyExtractor` |

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/table/sink/FixedBucketRowKeyExtractor.java` | 固定哈希分桶 | ✅ 完整 |
| `paimon-core/table/sink/DynamicBucketRowKeyExtractor.java` | 动态哈希分桶 | ✅ 完整 |
| `paimon-core/table/sink/RowPartitionAllPrimaryKeyExtractor.java` | 主键动态分桶 | ✅ 完整 |
| `paimon-core/table/sink/PostponeBucketRowKeyExtractor.java` | 延迟分桶 | ✅ 完整 |
| `paimon-core/table/sink/AppendTableRowKeyExtractor.java` | 无分桶模式 | ✅ 完整 |

#### 分布式路由公式
```java
// 方式1: 基于分区和桶（推荐）
channel = (hash(partition) + bucket) % numChannels

// 方式2: 仅基于桶（简化）
channel = bucket % numChannels
```

#### 相关文档
- 📄 `paimon-annotation/TABLE_SINK_TECH_SUMMARY.md` - 分桶模式对比详解

---

### 1.4 列式存储系统

#### 核心概念
- **ColumnVector**: 列向量，单列的批量数据存储
- **VectorizedColumnBatch**: 列向量批次，多列组成的数据块
- **WritableColumnVector**: 可写列向量，用于数据写入

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/data/columnar/ColumnVector.java` | 列向量接口 | ✅ 完整 |
| `paimon-common/data/columnar/VectorizedColumnBatch.java` | 列批次 | ✅ 完整 |
| `paimon-common/data/columnar/heap/HeapIntVector.java` | 堆内存整数列 | ✅ 完整 |
| `paimon-common/data/columnar/writable/WritableIntVector.java` | 可写整数列 | 🔄 部分 |

#### 列式存储优势
1. **查询性能**: 只读取需要的列，减少 I/O
2. **压缩效率**: 同类型数据连续存储，压缩比更高
3. **向量化执行**: 批量处理，CPU 缓存友好

#### 相关文档
- 📄 `SESSION_2026-02-11_FINAL_SUMMARY.md` - 列式存储详细说明

---

### 1.5 类型系统和转换

#### 类型层次
```
DataType (抽象基类)
├── AtomicDataType (原子类型)
│   ├── BooleanType
│   ├── TinyIntType
│   ├── IntType
│   ├── BigIntType
│   ├── FloatType
│   ├── DoubleType
│   ├── DecimalType
│   ├── CharType
│   ├── VarCharType
│   ├── BinaryType
│   ├── VarBinaryType
│   ├── DateType
│   ├── TimeType
│   └── TimestampType
├── ArrayType (数组类型)
├── MapType (映射类型)
├── RowType (行类型/结构体)
└── MultisetType (多重集类型)
```

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/types/DataType.java` | 类型系统基类 | ✅ 完整 |
| `paimon-common/types/RowType.java` | 行类型（结构体） | ✅ 完整 |
| `paimon-common/types/DecimalType.java` | 高精度小数类型 | ✅ 完整 |
| `paimon-common/casting/CastExecutor.java` | 类型转换执行器 | ✅ 完整 |

#### 类型转换规则
- **隐式转换**: 数值类型窄到宽（INT → BIGINT）
- **显式转换**: 需要显式 CAST（STRING → INT）
- **不支持转换**: 返回错误或 NULL

#### 相关文档
- 📄 `PAIMON_COMMON_DATA_TYPES_ANNOTATION_PROGRESS.md` - 类型系统文档

---

### 1.6 谓词下推优化

#### 核心概念
- **Partition Pruning**: 分区裁剪，过滤不需要的分区
- **File Pruning**: 文件裁剪，基于统计信息过滤文件
- **Index Pruning**: 索引裁剪，使用 Bloom Filter、Bitmap 等

#### 谓词类型
| 谓词类型 | 类名 | 示例 | 注释质量 |
|---------|------|------|----------|
| 比较谓词 | `LeafPredicate` | `age > 18` | ✅ 完整 |
| 逻辑谓词 | `And`, `Or` | `age > 18 AND city = 'Beijing'` | 🔄 部分 |
| NULL 谓词 | `IsNull`, `IsNotNull` | `name IS NOT NULL` | ✅ 完整 |
| IN 谓词 | `In` | `status IN ('active', 'pending')` | ✅ 完整 |
| LIKE 谓词 | `LeafPredicate` | `name LIKE 'test%'` | ✅ 完整 |

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/predicate/Predicate.java` | 谓词基类 | ✅ 完整 |
| `paimon-common/predicate/LeafPredicate.java` | 叶子谓词（比较） | ✅ 完整 |
| `paimon-common/predicate/PredicateBuilder.java` | 谓词构建器 | ✅ 完整 |
| `paimon-core/table/source/PushDownUtils.java` | 谓词下推工具 | ✅ 完整 |

#### 下推流程
```
SQL WHERE 子句
↓
转换为 Predicate
↓
分区级过滤（Partition Pruning）
↓
文件级过滤（File Pruning，基于统计信息）
↓
索引级过滤（Index Pruning，Bloom Filter/Bitmap）
↓
数据级过滤（Record Filter）
```

#### 相关文档
- 📄 `PREDICATE_BATCH23_PROGRESS.md` - 谓词详细文档

---

### 1.7 文件索引

#### 三种文件索引类型

##### 1.7.1 Bloom Filter 索引
- **用途**: 快速判断值是否存在（允许假阳性）
- **存储格式**: Bit 数组 + 多个哈希函数
- **适用场景**: 点查询、IN 查询、等值连接

##### 1.7.2 Bitmap 索引
- **用途**: 低基数列的快速过滤
- **存储格式**: RoaringBitmap（压缩位图）
- **适用场景**: 状态、性别、类型等枚举列

##### 1.7.3 BSI 索引（Bit-Sliced Index）
- **用途**: 数值列的范围查询
- **存储格式**: 按位切片的位图
- **适用场景**: 年龄、价格等数值范围查询

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/fileindex/bloomfilter/BloomFilter.java` | Bloom Filter 实现 | ✅ 完整 |
| `paimon-common/fileindex/bitmap/BitmapIndexFile.java` | Bitmap 索引 | ✅ 完整 |
| `paimon-common/fileindex/bsi/BSIIndexFile.java` | BSI 索引 | ✅ 完整 |

#### 索引存储方式
1. **嵌入式存储**: 索引数据嵌入数据文件的 Footer
2. **独立文件存储**: 索引数据存储在单独的 `.index` 文件

#### 相关文档
- 📄 `BATCH24_FS_IO_PROGRESS.md` - 文件索引详细文档

---

### 1.8 全局索引

#### 核心概念
- **跨分区更新**: 主键可能位于任意分区和桶
- **索引存储**: RocksDB 或内存 HashMap
- **索引内容**: Primary Key → (Partition, Bucket) 映射

#### 两种全局索引类型

##### 1.8.1 BTree 索引（RocksDB）
- **存储引擎**: RocksDB（基于 LSM-Tree）
- **数据结构**: 有序键值对
- **优势**: 支持大规模数据、持久化
- **劣势**: 需要额外的磁盘 I/O

##### 1.8.2 Bitmap 索引
- **存储引擎**: 内存或文件
- **数据结构**: RoaringBitmap
- **优势**: 极致压缩、快速查询
- **劣势**: 仅适用于低基数主键

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/globalindex/btree/BTreeGlobalIndex.java` | BTree 全局索引 | ✅ 完整 |
| `paimon-common/globalindex/bitmap/BitmapGlobalIndex.java` | Bitmap 全局索引 | ✅ 完整 |
| `paimon-core/globalindex/GlobalIndexManager.java` | 索引管理器 | ✅ 完整 |

#### 工作流程
```
1. 写入数据
   ↓
2. 更新全局索引: PK → (Partition, Bucket)
   ↓
3. 根据索引路由到正确的分区/桶
   ↓
4. 执行更新/删除操作
```

#### 相关文档
- 📄 `BATCH28_GLOBALINDEX_PROGRESS.md` - 全局索引详细文档

---

### 1.9 压缩算法

#### 支持的压缩算法
| 算法 | 压缩比 | 速度 | 适用场景 | 类名 |
|------|-------|------|---------|------|
| **LZ4** | 中等 | ⚡ 极快 | 实时写入、日志数据 | `Lz4BlockCompressionFactory` |
| **ZSTD** | 高 | 🚀 快 | 平衡场景、通用数据 | `ZstdCompressionFactory` |
| **GZIP** | 很高 | 🐢 慢 | 归档数据、网络传输 | `GzipCompressionFactory` |
| **SNAPPY** | 中等 | ⚡ 很快 | Hadoop 生态兼容 | `SnappyCompressionFactory` |
| **LZO** | 中等 | ⚡ 很快 | Hadoop 旧版本兼容 | `LzoCompressionFactory` |

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/compression/CompressionFactory.java` | 压缩工厂基类 | ✅ 完整 |
| `paimon-common/compression/Lz4BlockCompressionFactory.java` | LZ4 实现 | ✅ 完整 |
| `paimon-common/compression/ZstdCompressionFactory.java` | ZSTD 实现 | ✅ 完整 |

#### 压缩配置
```java
// 写入压缩
options.put("file.compression", "zstd");

// 压缩级别
options.put("file.compression.zstd.level", "3");
```

#### 相关文档
- 📄 `BATCH24_FS_IO_PROGRESS.md` - 压缩算法文档

---

### 1.10 序列化格式

#### 两种序列化模式

##### 1.10.1 标准模式（Standard Mode）
- **字段**: [key, sequenceNumber, valueKind, value]
- **存储内容**: 完整的 Key 和 Value
- **适用场景**: 需要完整键的场景（跨分区更新）

##### 1.10.2 精简模式（Thin Mode）
- **字段**: [sequenceNumber, valueKind, value]
- **存储内容**: 只存储 Value，Key 从文件名推断
- **空间节省**: 30-50%
- **适用场景**: 固定分桶、Key 可从元数据获取

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/KeyValueSerializer.java` | 标准模式序列化 | ✅ 完整 |
| `paimon-core/KeyValueThinSerializer.java` | 精简模式序列化 | ✅ 完整 |
| `paimon-common/data/serializer/Serializer.java` | 序列化器基类 | ✅ 完整 |

#### 序列化器层次
```
Serializer<T>
├── BasicTypeSerializer (基本类型)
│   ├── IntSerializer
│   ├── LongSerializer
│   ├── StringSerializer
│   └── ByteArraySerializer
├── CompositeSerializer (复合类型)
│   ├── RowSerializer
│   ├── ArraySerializer
│   └── MapSerializer
└── VersionedSerializer (版本化)
    └── KeyValueSerializer
```

#### 相关文档
- 📄 `paimon-annotation/BATCH7_PROGRESS.md` - 序列化格式详解

---

### 1.11 REST API 和认证

#### REST Catalog 架构
```
Client
↓
RESTClient (HTTP Client)
↓
AuthProvider (认证层)
├── BearTokenAuthProvider (Bearer Token)
└── DLFAuthProvider (阿里云 DLF)
    ├── DLFTokenLoader (Token 加载)
    └── DLFRequestSigner (请求签名)
↓
RESTApi (API 层)
├── Namespace API
├── Table API
├── View API
└── Config API
↓
REST Catalog Server
```

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/rest/RESTApi.java` | REST API 接口（300+行注释） | ✅ 完整 |
| `paimon-common/rest/RESTClient.java` | HTTP 客户端 | ✅ 完整 |
| `paimon-common/rest/auth/BearTokenAuthProvider.java` | Bearer Token 认证 | ✅ 完整 |
| `paimon-common/rest/auth/DLFAuthProvider.java` | 阿里云 DLF 认证 | 🔄 部分 |

#### 两种认证方式

##### 1.11.1 Bearer Token 认证
```java
Map<String, String> options = new HashMap<>();
options.put("authentication.type", "bearer");
options.put("token", "your-bearer-token");
```

##### 1.11.2 DLF 认证（阿里云）
```java
Map<String, String> options = new HashMap<>();
options.put("authentication.type", "dlf");
options.put("dlf.access-key-id", "your-access-key");
options.put("dlf.access-key-secret", "your-secret-key");
options.put("dlf.region", "cn-hangzhou");
```

#### 相关文档
- 📄 `REST_ANNOTATION_SUMMARY.md` - REST API 详细文档

---

### 1.12 Kerberos 安全

#### 核心概念
- **认证协议**: Kerberos 基于票据的认证
- **Principal**: 用户或服务的唯一标识
- **Keytab**: 包含密钥的文件，用于自动认证

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/security/SecurityContext.java` | 安全上下文管理 | 🔄 待完成 |
| `paimon-common/security/KerberosUtils.java` | Kerberos 工具类 | 🔄 待完成 |

#### Kerberos 配置示例
```java
Map<String, String> options = new HashMap<>();
options.put("security.kerberos.login.principal", "user@REALM");
options.put("security.kerberos.login.keytab", "/path/to/user.keytab");
```

---

### 1.13 空间填充曲线

#### 核心概念
- **多维索引**: 将多维数据映射到一维曲线
- **空间局部性**: 相邻的多维点在曲线上也相邻
- **范围查询优化**: 减少多维范围查询的扫描范围

#### 两种空间填充曲线

##### 1.13.1 Hilbert 曲线
- **特点**: 最优的空间局部性
- **复杂度**: 计算复杂
- **适用场景**: 对查询性能要求极高的场景

##### 1.13.2 Z-Order 曲线
- **特点**: 计算简单、局部性次优
- **复杂度**: 位交错操作，O(n)
- **适用场景**: 平衡计算成本和查询性能

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-common/sort/hilbert/HilbertIndexer.java` | Hilbert 曲线实现 | ✅ 完整 |
| `paimon-common/sort/zorder/ZOrderByteUtils.java` | Z-Order 曲线实现 | ✅ 完整 |
| `paimon-core/append/cluster/HilbertSorter.java` | Hilbert 排序器 | ✅ 完整 |

#### 应用场景
```java
// 创建表时指定聚类列
CREATE TABLE geospatial_data (
  id BIGINT,
  longitude DOUBLE,
  latitude DOUBLE,
  ...
) WITH (
  'cluster.columns' = 'longitude,latitude',
  'cluster.sort' = 'hilbert'
);
```

#### 相关文档
- 📄 `paimon-annotation/BATCH10_PROGRESS.md` - 数据聚类详细文档

---

### 1.14 删除向量（Deletion Vectors）

#### 核心概念
- **Copy-On-Write 优化**: 避免重写整个文件，只记录删除的行
- **位图存储**: 使用 RoaringBitmap 压缩存储删除行号
- **独立生命周期**: 删除向量可独立于数据文件管理

#### 工作流程
```
1. 写入数据文件
   ↓
2. 接收删除请求
   ↓
3. 创建/更新删除向量
   ├── 记录删除的行号（0-based）
   └── 使用 RoaringBitmap 压缩
   ↓
4. 读取时应用删除向量
   └── 跳过标记为删除的行
```

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/deletionvectors/DeletionVector.java` | 删除向量核心 | ✅ 完整 |
| `paimon-core/deletionvectors/append/AppendDeletionFileMaintainer.java` | 追加表维护 | ✅ 完整 |
| `paimon-common/deletionvectors/RoaringBitmap32.java` | 位图实现 | ✅ 完整 |

#### 优势
1. **减少写放大**: 无需重写完整文件
2. **快速删除**: 只更新位图，O(1) 操作
3. **空间效率**: RoaringBitmap 高效压缩

#### 相关文档
- 📄 `paimon-annotation/INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md` - 删除向量详细文档

---

### 1.15 状态管理（Lookup）

#### 核心概念
- **状态后端**: 存储 MergeTree 的 Lookup 数据
- **两种实现**: 内存 HashMap 和 RocksDB LSM-Tree

#### 状态后端对比
| 后端类型 | 存储位置 | 容量限制 | 性能 | 适用场景 |
|---------|---------|---------|------|---------|
| **Memory** | 堆内存 | 受内存限制 | ⚡ 极快 | 小数据集、高频查询 |
| **RocksDB** | 磁盘（本地） | 无限制 | 🚀 快 | 大数据集、持久化 |

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/lookup/LookupLevels.java` | Lookup 层级管理 | ✅ 完整 |
| `paimon-core/lookup/memory/MemoryLookupStoreFactory.java` | 内存后端工厂 | 🔄 待完成 |
| `paimon-core/lookup/rocksdb/RocksDBStateFactory.java` | RocksDB 后端工厂 | 🔄 待完成 |
| `paimon-common/lookup/LookupStrategy.java` | Lookup 策略 | ✅ 完整 |

#### 配置示例
```java
// 使用内存后端
options.put("lookup.cache.type", "memory");

// 使用 RocksDB 后端
options.put("lookup.cache.type", "rocksdb");
options.put("lookup.cache.rocksdb.path", "/path/to/rocksdb");
```

---

### 1.16 事务提交（Snapshot）

#### 快照元数据
```
Snapshot
├── id: 快照 ID
├── schemaId: Schema 版本 ID
├── baseManifestList: 基础 Manifest 列表
├── deltaManifestList: 增量 Manifest 列表
├── changelogManifestList: Changelog Manifest 列表（可选）
├── commitUser: 提交用户
├── commitIdentifier: 提交标识符
├── commitKind: 提交类型（APPEND/COMPACT/OVERWRITE）
├── timeMillis: 提交时间戳
└── statistics: 统计信息
```

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/Snapshot.java` | 快照元数据 | ✅ 完整 |
| `paimon-core/utils/SnapshotManager.java` | 快照管理器 | ✅ 完整 |
| `paimon-core/manifest/ManifestList.java` | Manifest 列表 | ✅ 完整 |

#### 快照类型
1. **APPEND**: 追加写入
2. **COMPACT**: 压缩合并
3. **OVERWRITE**: 覆盖写入

#### 相关文档
- 📄 `paimon-annotation/BATCH8_PROGRESS.md` - Manifest 管理详细文档

---

### 1.17 权限管理

#### 权限模型
```
Catalog
├── Database
│   ├── SELECT
│   ├── INSERT
│   ├── DROP
│   └── ALTER
└── Table
    ├── SELECT
    ├── INSERT
    ├── UPDATE
    ├── DELETE
    ├── DROP
    └── ALTER
```

#### 关键文件
| 文件路径 | 核心功能 | 注释质量 |
|---------|---------|----------|
| `paimon-core/privilege/PrivilegeManager.java` | 权限管理器 | ✅ 完整 |
| `paimon-core/privilege/PrivilegeChecker.java` | 权限检查器 | ✅ 完整 |
| `paimon-core/privilege/FileBasedPrivilegeManager.java` | 文件权限管理 | ✅ 完整 |

#### 权限配置示例
```java
// 授予用户对表的 SELECT 权限
privilegeManager.grant(
    new Identifier("db1", "table1"),
    "user1",
    Privilege.SELECT
);

// 检查权限
boolean hasPrivilege = privilegeChecker.check(
    user,
    table,
    Privilege.INSERT
);
```

#### 相关文档
- 📄 `paimon-annotation/INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md` - 权限管理详细文档

---

## 2. 按包组织的文档导航

### 2.1 paimon-core 包（762/767，99.3%）

#### 2.1.1 核心根目录（8个文件）
| 文件 | 核心功能 | 重要性 |
|------|---------|--------|
| `FileStore.java` | FileStore 接口，顶层抽象 | ⭐⭐⭐⭐⭐ |
| `KeyValue.java` | 核心数据结构（5 字段） | ⭐⭐⭐⭐⭐ |
| `KeyValueFileStore.java` | 主键表实现 | ⭐⭐⭐⭐⭐ |
| `AppendOnlyFileStore.java` | 追加表实现 | ⭐⭐⭐⭐ |
| `KeyValueSerializer.java` | 标准序列化器 | ⭐⭐⭐⭐ |
| `KeyValueThinSerializer.java` | 精简序列化器 | ⭐⭐⭐ |

**技术亮点**:
- 五种分桶模式完整体系
- 标准模式 vs 精简模式序列化
- Commit Callback 机制

📄 **文档**: `paimon-annotation/BATCH7_PROGRESS.md`

---

#### 2.1.2 mergetree 包（105个文件）

##### 主包（27个文件）
| 子包/类 | 核心功能 | 重要性 |
|---------|---------|--------|
| `Levels.java` | 多层级管理 | ⭐⭐⭐⭐⭐ |
| `MergeTreeWriter.java` | 写入器（645行） | ⭐⭐⭐⭐⭐ |
| `WriteBuffer.java` | 写入缓冲区 | ⭐⭐⭐⭐ |
| `MergeSorter.java` | 归并排序器（266行） | ⭐⭐⭐⭐ |
| `localmerge/` | 本地合并（3个文件） | ⭐⭐⭐ |
| `lookup/` | Lookup 机制（14个文件） | ⭐⭐⭐⭐ |

**技术亮点**:
- LSM-Tree 完整实现
- Lookup 快速查找
- 本地合并优化

📄 **文档**: `paimon-annotation/BATCH3_PROGRESS.md`

##### compact 子包（33个文件）
| 类型 | 核心类 | 数量 |
|------|--------|------|
| 压缩策略 | `UniversalCompaction`, `ForceUpLevel0Compaction` | 3个 |
| 合并函数 | `DeduplicateMergeFunction`, `PartialUpdateMergeFunction` | 8个 |
| 重写器 | `MergeTreeCompactRewriter`, `ChangelogMergeTreeRewriter` | 5个 |
| 读取器 | `SortMergeReader`, `ConcatRecordReader` | 4个 |

**技术亮点**:
- 三种压缩策略
- 完整的合并函数体系
- LoserTree 和 Heap 两种归并实现

📄 **文档**: `paimon-annotation/BATCH2_PROGRESS.md`

##### compact/aggregate 子包（45个文件）
| 聚合类型 | 数量 | 示例 |
|---------|------|------|
| 数学聚合 | 4个 | SUM, PRODUCT, MAX, MIN |
| 字符串聚合 | 2个 | LISTAGG, COLLECT |
| 逻辑聚合 | 2个 | BOOL_AND, BOOL_OR |
| 复杂类型聚合 | 3个 | MERGE_MAP, NESTED_UPDATE, NESTED_PARTIAL_UPDATE |
| 特殊聚合 | 6个 | HLL_SKETCH, THETA_SKETCH, ROARING_BITMAP32, ROARING_BITMAP64 |

**技术亮点**:
- 58 个聚合函数（包括工厂类）
- 支持嵌套类型聚合
- 概率数据结构（HLL、Theta Sketch）

📄 **文档**: `paimon-annotation/BATCH1_PROGRESS.md`

---

#### 2.1.3 io 包（39个文件）

| 类型 | 核心类 | 功能 |
|------|--------|------|
| 元数据 | `DataFileMeta.java` | 数据文件元信息 |
| 增量数据 | `DataIncrement.java`, `CompactIncrement.java` | 提交增量 |
| 读取器 | `KeyValueFileReaderFactory.java` | 文件读取工厂 |
| 写入器 | `KeyValueFileWriterFactory.java` | 文件写入工厂（13个） |
| 索引 | `DataFileIndexWriter.java` | 索引写入器 |
| 统计 | `SimpleStatsProducer.java` | 统计信息生成 |

**技术亮点**:
- 7个版本的序列化器（向后兼容）
- 标准模式 vs 精简模式写入器
- 索引嵌入式存储和独立存储

📄 **文档**: `paimon-annotation/BATCH5_PROGRESS.md`

---

#### 2.1.4 operation 包（57个文件）

##### 核心接口（3个）
| 接口 | 功能 | 重要性 |
|------|------|--------|
| `FileStoreScan.java` | 三种扫描模式 | ⭐⭐⭐⭐⭐ |
| `FileStoreWrite.java` | 写入和内存管理 | ⭐⭐⭐⭐⭐ |
| `FileStoreCommit.java` | 两阶段提交 | ⭐⭐⭐⭐⭐ |

##### 核心实现
| 类型 | 数量 | 核心类 |
|------|------|--------|
| 扫描实现 | 5个 | `MergedFileStoreScan`, `UnmergedFileStoreScan` |
| 写入实现 | 8个 | `KeyValueFileStoreWrite`, `AppendOnlyFileStoreWrite` |
| 提交实现 | 1个 | `FileStoreCommitImpl`（400+行注释） |
| 文件管理 | 8个 | `SnapshotDeletion`, `OrphanFilesClean` |

**技术亮点**:
- 详细的两阶段提交注释
- 三种冲突检测策略
- 分布式锁机制

📄 **文档**: `paimon-annotation/BATCH6_PROGRESS.md`

---

#### 2.1.5 manifest 包（27个文件）

##### 三层元数据结构
```
Snapshot
↓
ManifestList
↓
ManifestFile
↓
DataFileMeta
```

##### 核心类
| 类型 | 核心类 | 功能 |
|------|--------|------|
| 元数据 | `Snapshot.java` | 快照元信息 |
| 列表 | `ManifestList.java` | Manifest 文件列表 |
| 文件 | `ManifestFile.java` | 单个 Manifest 文件 |
| 条目 | `SimpleFileEntry.java` | 文件条目 |
| 索引 | `IndexManifestFile.java` | 索引 Manifest |

**技术亮点**:
- 三种 Manifest 类型（Base、Delta、Changelog）
- 四级过滤优化
- 并行读取优化（分段缓存）

📄 **文档**: `paimon-annotation/BATCH8_PROGRESS.md`

---

#### 2.1.6 table 包（146个文件）

##### table 主包（24个文件）
| 层次 | 接口/类 | 功能 |
|------|---------|------|
| 顶层 | `Table.java` | 表接口 |
| 数据层 | `DataTable.java` | 数据表接口 |
| 内部层 | `InnerTable.java` | 内部接口 |
| 文件存储层 | `FileStoreTable.java` | FileStore 表接口 |
| 实现 | `PrimaryKeyFileStoreTable.java` | 主键表实现 |
| 实现 | `AppendOnlyFileStoreTable.java` | 追加表实现 |

**技术亮点**:
- 完整的接口层次
- 特殊表实现（FormatTable、ReadonlyTable 等）
- 快照生命周期管理

📄 **文档**: `paimon-annotation/BATCH13_PROGRESS.md`

##### table/sink 子包（39个文件）
| 类型 | 核心类 | 功能 |
|------|--------|------|
| 核心接口 | `TableWrite.java`, `TableCommit.java` | 写入和提交 |
| 构建器 | `WriteBuilder.java` | 写入构建器 |
| RowKeyExtractor | 5个分桶模式类 | 行键提取 |
| CommitMessage | `CommitMessageSerializer.java` | 提交消息序列化 |
| 回调 | `CommitCallback.java`, `TagCallback.java` | 回调机制 |

**技术亮点**:
- 五种分桶模式的行键提取器
- 分布式写入路由
- CommitCallback vs TagCallback

📄 **文档**: `paimon-annotation/BATCH15_PROGRESS.md`

##### table/source 子包（78个文件）
| 子包 | 文件数 | 核心功能 |
|------|--------|---------|
| source 主包 | 40个 | 扫描、读取、分片、计划 |
| snapshot 子包 | 28个 | Starting/FollowUp Scanner |
| splitread 子包 | 10个 | 分片读取提供者 |

**技术亮点**:
- 17种 StartingScanner 实现
- 增量读取和 Changelog 读取
- 谓词下推和分片生成

📄 **文档**: `paimon-annotation/BATCH16_PROGRESS.md`

##### table/system 子包（24个文件）
| 系统表类型 | 示例 | 用途 |
|-----------|------|------|
| 全局系统表 | `AllTablesTable.java` | 查询所有表信息 |
| 表级系统表 | `SchemasTable.java` | 查询 Schema 历史 |
| 数据文件表 | `FilesTable.java` | 查询数据文件详情 |
| 版本管理表 | `TagsTable.java` | 查询标签 |
| 变更日志表 | `AuditLogTable.java` | 查询变更历史 |

**技术亮点**:
- 24种系统表
- 元数据可查询
- 监控和调试支持

📄 **文档**: `paimon-annotation/BATCH17_PROGRESS.md`

---

#### 2.1.7 其他核心包

##### catalog 包（22个文件）
- **核心类**: `Catalog.java`, `FileSystemCatalog.java`, `CachingCatalog.java`
- **技术亮点**: 三层管理（Catalog/Database/Table）、缓存优化、分布式锁
- 📄 **文档**: `paimon-annotation/BATCH9_PROGRESS.md`

##### append 包（23个文件）
- **核心类**: `AppendCompactCoordinator.java`, `HilbertSorter.java`
- **技术亮点**: 自动压缩、Hilbert/Z-Order 聚类、Blob 字段分离
- 📄 **文档**: `paimon-annotation/BATCH10_PROGRESS.md`

##### disk 包（19个文件）
- **核心类**: `IOManager.java`, `FileIOChannel.java`
- **技术亮点**: 零拷贝溢写、分块读写、自动压缩
- 📄 **文档**: `paimon-annotation/BATCH4_PROGRESS.md`

##### utils 包（52个文件）
- **核心类**: `SnapshotManager.java`, `FileStorePathFactory.java`
- **技术亮点**: 快照管理、Changelog 管理、缓存工具
- 📄 **文档**: `paimon-annotation/BATCH12_PROGRESS.md`

##### deletionvectors 包（12个文件）
- **核心类**: `DeletionVector.java`, `AppendDeletionFileMaintainer.java`
- **技术亮点**: RoaringBitmap 压缩、Copy-On-Write 优化
- 📄 **文档**: `paimon-annotation/INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md`

##### privilege 包（14个文件）
- **核心类**: `PrivilegeManager.java`, `PrivilegeChecker.java`
- **技术亮点**: 文件权限管理、权限检查、授权/撤销
- 📄 **文档**: `paimon-annotation/INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md`

##### iceberg 包（34个文件）
- **核心类**: `IcebergTable.java`, `IcebergMetadata.java`
- **技术亮点**: Iceberg 兼容、元数据转换、数据迁移
- 📄 **文档**: `ICEBERG_ANNOTATION_SUMMARY.md`

##### globalindex 包（9个文件）
- **核心类**: `GlobalIndexManager.java`, `BTreeGlobalIndex.java`
- **技术亮点**: 跨分区更新、RocksDB 存储、B树索引
- 📄 **文档**: `BATCH28_GLOBALINDEX_PROGRESS.md`

---

### 2.2 paimon-common 包（82/575，14.3%）

#### 2.2.1 types 包（完成度：100%）
| 类型分类 | 核心类 | 数量 |
|---------|--------|------|
| 基础类型 | `DataType.java`, `DataTypeRoot.java` | 2个 |
| 原子类型 | `IntType.java`, `VarCharType.java`, `DecimalType.java` | 15个 |
| 复杂类型 | `RowType.java`, `ArrayType.java`, `MapType.java` | 4个 |
| 工具类 | `DataTypeUtils.java`, `TypeCheckUtils.java` | 8个 |

📄 **文档**: `PAIMON_COMMON_DATA_TYPES_ANNOTATION_PROGRESS.md`

---

#### 2.2.2 data 包（完成度：39.3%）

##### columnar 子包（44.2%）
| 子包 | 核心功能 | 完成度 |
|------|---------|--------|
| 主目录 | `ColumnVector.java`, `VectorizedColumnBatch.java` | ✅ 完整 |
| heap 子目录 | `HeapIntVector.java` 等 | ✅ 完整 |
| writable 子目录 | `WritableIntVector.java` 等 | 🔄 部分 |

##### serializer 子包（53.8%）
| 类型 | 核心类 | 完成度 |
|------|--------|--------|
| 基础序列化器 | `Serializer.java` | ✅ 完整 |
| 基本类型 | `IntSerializer.java` 等 | ✅ 完整 |
| 复杂类型 | `RowSerializer.java` 等 | 🔄 部分 |

##### variant 子包（56.3%）
- **核心类**: `Variant.java`, `VariantWriter.java`
- **功能**: 半结构化数据支持（JSON-like）

📄 **文档**: `SESSION_2026-02-11_FINAL_SUMMARY.md`

---

#### 2.2.3 casting 包（完成度：41.3%）
| 转换类型 | 核心类 | 完成度 |
|---------|--------|--------|
| 转换规则 | `CastRule.java` | ✅ 完整 |
| 执行器 | `CastExecutor.java` | ✅ 完整 |
| 数值转换 | `NumericToNumericCastRule.java` | ✅ 完整 |
| 时间转换 | `StringToDateCastRule.java` 等 | 🔄 部分 |

📄 **文档**: `PREDICATE_BATCH23_PROGRESS.md`

---

#### 2.2.4 predicate 包（完成度：21.3%）
| 谓词类型 | 核心类 | 完成度 |
|---------|--------|--------|
| 基类 | `Predicate.java` | ✅ 完整 |
| 叶子谓词 | `LeafPredicate.java` | ✅ 完整 |
| 比较谓词 | `Equal.java`, `GreaterThan.java` | ✅ 完整 |
| NULL 谓词 | `IsNull.java`, `IsNotNull.java` | ✅ 完整 |
| 逻辑谓词 | `And.java`, `Or.java` | 🔄 待完成 |

📄 **文档**: `PREDICATE_BATCH23_PROGRESS.md`

---

#### 2.2.5 fileindex 包（完成度：100%）
| 索引类型 | 核心类 | 功能 |
|---------|--------|------|
| Bloom Filter | `BloomFilter.java` | 点查询过滤 |
| Bitmap | `BitmapIndexFile.java` | 低基数列过滤 |
| BSI | `BSIIndexFile.java` | 数值范围查询 |
| Range Bitmap | `RangeBitmapIndexFile.java` | 范围位图 |

📄 **文档**: `BATCH24_FS_IO_PROGRESS.md`

---

#### 2.2.6 fs 包（完成度：100%）
| 类型 | 核心类 | 功能 |
|------|--------|------|
| 核心接口 | `FileIO.java`, `Path.java` | 文件系统抽象 |
| 实现 | `HadoopFileIO.java`, `LocalFileIO.java` | Hadoop/本地实现 |
| 流 | `SeekableInputStream.java` | 可定位输入流 |

📄 **文档**: `BATCH24_FS_IO_PROGRESS.md`

---

#### 2.2.7 io 包（完成度：100%）
| 类型 | 核心类 | 功能 |
|------|--------|------|
| 序列化 | `DataInputView.java`, `DataOutputView.java` | 序列化接口 |
| 缓存 | `FileIOCache.java` | 文件 I/O 缓存 |

📄 **文档**: `BATCH24_FS_IO_PROGRESS.md`

---

#### 2.2.8 globalindex 包（完成度：100%）
| 索引类型 | 核心类 | 功能 |
|---------|--------|------|
| BTree | `BTreeGlobalIndex.java` | B树全局索引 |
| Bitmap | `BitmapGlobalIndex.java` | 位图全局索引 |

📄 **文档**: `BATCH28_GLOBALINDEX_PROGRESS.md`

---

#### 2.2.9 rest 包（完成度：88%）
| 类型 | 核心类 | 完成度 |
|------|--------|--------|
| 核心 API | `RESTApi.java`, `RESTClient.java` | ✅ 完整 |
| 认证 | `BearTokenAuthProvider.java` | ✅ 完整 |
| DLF 认证 | `DLFAuthProvider.java` | 🔄 部分 |
| 请求/响应 | `requests/`, `responses/` | ✅ 完整 |

📄 **文档**: `REST_ANNOTATION_SUMMARY.md`

---

#### 2.2.10 其他包

##### compression 包（完成度：100%）
- **核心类**: `Lz4BlockCompressionFactory.java`, `ZstdCompressionFactory.java`
- **技术亮点**: LZ4、ZSTD、GZIP、SNAPPY、LZO 压缩

##### memory 包（完成度：100%）
- **核心类**: `MemorySegment.java`, `MemorySegmentPool.java`
- **技术亮点**: 堆内存/堆外内存、内存池管理

##### reader 包（完成度：100%）
- **核心类**: `RecordReader.java`, `PackChangelogReader.java`
- **技术亮点**: 记录读取器、Changelog 打包

##### sort 包（完成度：100%）
- **核心类**: `HilbertIndexer.java`, `ZOrderByteUtils.java`
- **技术亮点**: Hilbert 曲线、Z-Order 曲线

---

### 2.3 paimon-api 包（0/199，0%）

#### 待完成模块
- types 包
- table 包
- options 包
- annotation 包
- function 包
- partition 包
- schema 包
- view 包

---

## 3. 重要文件快速查找

### 3.1 按功能分类的核心类

#### 3.1.1 数据写入路径
```
用户数据（InternalRow）
↓
RowKeyExtractor → 提取分区、桶、主键
↓
SinkRecord → 封装提取结果
↓
TableWrite → 写入数据
↓
TableCommit → 提交数据
```

**关键文件**:
1. `paimon-core/table/sink/RowKeyExtractor.java` - 行键提取基类
2. `paimon-core/table/sink/FixedBucketRowKeyExtractor.java` - 固定分桶提取器
3. `paimon-core/table/sink/TableWriteImpl.java` - 写入实现
4. `paimon-core/table/sink/TableCommitImpl.java` - 提交实现
5. `paimon-core/operation/FileStoreCommitImpl.java` - 底层提交实现（400+行注释）

---

#### 3.1.2 数据读取路径
```
用户查询（SQL WHERE）
↓
Predicate → 谓词下推
↓
TableScan → 扫描快照
↓
Split → 生成分片
↓
TableRead → 读取数据
```

**关键文件**:
1. `paimon-core/table/source/TableScan.java` - 扫描接口
2. `paimon-core/table/source/DataTableScan.java` - 扫描实现
3. `paimon-core/table/source/SplitGenerator.java` - 分片生成器
4. `paimon-core/table/source/TableRead.java` - 读取接口
5. `paimon-core/operation/FileStoreScanImpl.java` - 底层扫描实现

---

#### 3.1.3 压缩和合并路径
```
MergeTree 层级
↓
CompactStrategy → 选择压缩策略
↓
CompactRewriter → 执行压缩重写
↓
MergeFunction → 合并记录
↓
新的 SortedRun
```

**关键文件**:
1. `paimon-core/mergetree/compact/UniversalCompaction.java` - 通用压缩策略
2. `paimon-core/mergetree/compact/MergeTreeCompactRewriter.java` - 压缩重写器
3. `paimon-core/mergetree/compact/DeduplicateMergeFunction.java` - 去重合并函数
4. `paimon-core/mergetree/compact/PartialUpdateMergeFunction.java` - 部分更新合并
5. `paimon-core/mergetree/compact/AggregateMergeFunction.java` - 聚合合并

---

#### 3.1.4 元数据管理路径
```
Catalog → 管理数据库和表
↓
SnapshotManager → 管理快照
↓
ManifestList → 管理 Manifest 文件
↓
ManifestFile → 管理数据文件元信息
```

**关键文件**:
1. `paimon-core/catalog/Catalog.java` - Catalog 接口
2. `paimon-core/catalog/FileSystemCatalog.java` - 文件系统 Catalog
3. `paimon-core/utils/SnapshotManager.java` - 快照管理器
4. `paimon-core/manifest/ManifestList.java` - Manifest 列表
5. `paimon-core/manifest/ManifestFile.java` - Manifest 文件

---

### 3.2 包含示例代码的文件

#### 3.2.1 完整使用示例
| 文件 | 示例内容 | 行数 |
|------|---------|------|
| `paimon-core/table/sink/FixedBucketRowKeyExtractor.java` | 分桶配置和使用 | 50行+ |
| `paimon-core/table/sink/TableCommitImpl.java` | 提交流程示例 | 80行+ |
| `paimon-common/rest/RESTApi.java` | REST API 调用示例 | 100行+ |
| `paimon-common/rest/auth/DLFTokenLoader.java` | DLF 认证示例 | 60行+ |
| `paimon-core/append/cluster/HilbertSorter.java` | Hilbert 排序示例 | 40行+ |

#### 3.2.2 架构设计示例
| 文件 | 架构内容 | 详细度 |
|------|---------|--------|
| `paimon-core/operation/FileStoreCommitImpl.java` | 两阶段提交协议 | ⭐⭐⭐⭐⭐ |
| `paimon-core/mergetree/MergeTreeWriter.java` | LSM-Tree 写入架构 | ⭐⭐⭐⭐⭐ |
| `paimon-core/catalog/FileSystemCatalog.java` | Catalog 层次架构 | ⭐⭐⭐⭐ |
| `paimon-core/table/AbstractFileStoreTable.java` | Table 抽象层设计 | ⭐⭐⭐⭐⭐ |

---

### 3.3 架构图和对比表格所在文件

#### 3.3.1 架构图
| 文档 | 架构图内容 | 位置 |
|------|-----------|------|
| `TABLE_SINK_TECH_SUMMARY.md` | 五种分桶模式对比、分布式路由、回调机制 | 完整文档 |
| `SESSION_2026-02-11_FINAL_SUMMARY.md` | 列式存储架构、类型系统、Variant 编码 | 完整文档 |
| `BATCH8_PROGRESS.md` | 三层元数据结构、四级过滤优化 | BATCH8 |
| `BATCH6_PROGRESS.md` | 两阶段提交流程、冲突检测策略 | BATCH6 |

#### 3.3.2 对比表格
| 文档 | 对比内容 | 详细度 |
|------|---------|--------|
| `TABLE_SINK_TECH_SUMMARY.md` | 五种分桶模式对比 | ⭐⭐⭐⭐⭐ |
| `TABLE_SINK_TECH_SUMMARY.md` | CommitCallback vs TagCallback | ⭐⭐⭐⭐⭐ |
| `BATCH13_PROGRESS.md` | 主键表 vs 追加表对比 | ⭐⭐⭐⭐ |
| `BATCH14_PROGRESS.md` | FormatTable vs FileStoreTable | ⭐⭐⭐⭐ |
| `SESSION_2026-02-11_FINAL_SUMMARY.md` | Memory vs RocksDB 状态后端 | ⭐⭐⭐⭐ |

---

## 4. 学习路径建议

### 4.1 新手入门路径

#### 第一阶段：核心概念（1-2周）
1. **理解表类型**
   - 📖 阅读: `paimon-core/table/AbstractFileStoreTable.java`（750+行注释）
   - 📖 文档: `BATCH13_PROGRESS.md`
   - 🎯 重点: 主键表 vs 追加表

2. **理解数据写入**
   - 📖 阅读: `paimon-core/table/sink/TableWriteImpl.java`
   - 📖 阅读: `paimon-core/table/sink/FixedBucketRowKeyExtractor.java`
   - 📖 文档: `TABLE_SINK_TECH_SUMMARY.md`
   - 🎯 重点: 分桶模式、写入流程

3. **理解数据读取**
   - 📖 阅读: `paimon-core/table/source/DataTableScan.java`
   - 📖 阅读: `paimon-core/table/source/TableRead.java`
   - 📖 文档: `BATCH16_PROGRESS.md`
   - 🎯 重点: 扫描模式、分片生成

4. **理解元数据管理**
   - 📖 阅读: `paimon-core/Snapshot.java`
   - 📖 阅读: `paimon-core/manifest/ManifestList.java`
   - 📖 文档: `BATCH8_PROGRESS.md`
   - 🎯 重点: 三层元数据结构

---

#### 第二阶段：核心机制（2-3周）
5. **掌握两阶段提交**
   - 📖 阅读: `paimon-core/operation/FileStoreCommitImpl.java`（400+行注释）
   - 📖 文档: `BATCH6_PROGRESS.md`
   - 🎯 重点: Prepare/Commit 阶段、冲突检测

6. **掌握 LSM-Tree**
   - 📖 阅读: `paimon-core/mergetree/Levels.java`
   - 📖 阅读: `paimon-core/mergetree/MergeTreeWriter.java`（645行）
   - 📖 文档: `BATCH3_PROGRESS.md`
   - 🎯 重点: 层级管理、写入流程

7. **掌握压缩策略**
   - 📖 阅读: `paimon-core/mergetree/compact/UniversalCompaction.java`
   - 📖 阅读: `paimon-core/mergetree/compact/MergeTreeCompactRewriter.java`
   - 📖 文档: `BATCH2_PROGRESS.md`
   - 🎯 重点: 压缩触发、合并函数

8. **掌握文件 I/O**
   - 📖 阅读: `paimon-core/io/KeyValueFileWriterFactory.java`
   - 📖 阅读: `paimon-core/io/DataFileMeta.java`
   - 📖 文档: `BATCH5_PROGRESS.md`
   - 🎯 重点: 标准模式 vs 精简模式

---

#### 第三阶段：高级特性（2-3周）
9. **掌握文件索引**
   - 📖 阅读: `paimon-common/fileindex/bloomfilter/BloomFilter.java`
   - 📖 阅读: `paimon-common/fileindex/bitmap/BitmapIndexFile.java`
   - 📖 文档: `BATCH24_FS_IO_PROGRESS.md`
   - 🎯 重点: Bloom Filter、Bitmap、BSI

10. **掌握谓词下推**
    - 📖 阅读: `paimon-common/predicate/Predicate.java`
    - 📖 阅读: `paimon-core/table/source/PushDownUtils.java`
    - 📖 文档: `PREDICATE_BATCH23_PROGRESS.md`
    - 🎯 重点: 四级过滤、统计信息

11. **掌握全局索引**
    - 📖 阅读: `paimon-common/globalindex/btree/BTreeGlobalIndex.java`
    - 📖 阅读: `paimon-core/globalindex/GlobalIndexManager.java`
    - 📖 文档: `BATCH28_GLOBALINDEX_PROGRESS.md`
    - 🎯 重点: 跨分区更新、索引存储

12. **掌握删除向量**
    - 📖 阅读: `paimon-core/deletionvectors/DeletionVector.java`
    - 📖 阅读: `paimon-common/deletionvectors/RoaringBitmap32.java`
    - 📖 文档: `INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md`
    - 🎯 重点: Copy-On-Write 优化、位图压缩

---

### 4.2 核心概念学习顺序

```
1. Table 抽象层
   ├── Table 接口
   ├── FileStoreTable 实现
   └── 主键表 vs 追加表
   ↓
2. 数据写入流程
   ├── RowKeyExtractor（分桶）
   ├── TableWrite（写入）
   └── TableCommit（提交）
   ↓
3. 数据读取流程
   ├── TableScan（扫描）
   ├── Split（分片）
   └── TableRead（读取）
   ↓
4. 元数据管理
   ├── Snapshot
   ├── ManifestList
   └── ManifestFile
   ↓
5. LSM-Tree 核心
   ├── Levels（层级）
   ├── MergeTreeWriter（写入器）
   └── CompactStrategy（压缩策略）
   ↓
6. 高级特性
   ├── 文件索引
   ├── 全局索引
   └── 删除向量
```

---

### 4.3 深入理解路径

#### 路径1：存储引擎深入
1. **LSM-Tree 实现细节**
   - 📖 阅读所有 mergetree 包文件（105个）
   - 📖 研究三种压缩策略的实现
   - 📖 研究 58 个聚合函数的实现
   - 🎯 目标: 理解 LSM-Tree 的完整生命周期

2. **文件格式和序列化**
   - 📖 阅读 io 包所有文件（39个）
   - 📖 研究 7 个版本序列化器
   - 📖 研究标准模式 vs 精简模式
   - 🎯 目标: 理解文件存储格式

3. **索引和优化**
   - 📖 阅读 fileindex 包所有文件
   - 📖 阅读 globalindex 包所有文件
   - 📖 阅读 deletionvectors 包所有文件
   - 🎯 目标: 理解查询优化机制

---

#### 路径2：分布式系统深入
1. **两阶段提交**
   - 📖 深入研究 `FileStoreCommitImpl.java`
   - 📖 研究冲突检测策略
   - 📖 研究分布式锁机制
   - 🎯 目标: 理解分布式事务

2. **Catalog 和元数据**
   - 📖 研究 catalog 包所有文件（22个）
   - 📖 研究 FileSystemCatalog 实现
   - 📖 研究 CachingCatalog 优化
   - 🎯 目标: 理解元数据管理

3. **分布式写入**
   - 📖 研究 ChannelComputer 路由算法
   - 📖 研究五种分桶模式
   - 📖 研究 WriteSelector 优化
   - 🎯 目标: 理解分布式数据分发

---

#### 路径3：生态集成深入
1. **Iceberg 兼容**
   - 📖 阅读 iceberg 包所有文件（34个）
   - 📖 研究元数据转换
   - 📖 研究数据迁移
   - 🎯 目标: 理解表格式兼容性

2. **REST Catalog**
   - 📖 阅读 rest 包所有文件（95个）
   - 📖 研究 RESTApi 完整接口
   - 📖 研究认证机制
   - 🎯 目标: 理解 REST Catalog 协议

3. **文件系统集成**
   - 📖 阅读 fs 包所有文件
   - 📖 研究 Hadoop 集成
   - 📖 研究对象存储支持
   - 🎯 目标: 理解多文件系统支持

---

### 4.4 按角色定制的学习路径

#### 用户角色：使用 Paimon 的开发者
**学习重点**: 表操作、SQL 集成、性能调优
1. Table API 使用（BATCH13-15）
2. 分桶模式选择（TABLE_SINK_TECH_SUMMARY.md）
3. 查询优化（BATCH16、PREDICATE_BATCH23_PROGRESS.md）
4. 配置参数（CoreOptions.java）

---

#### 贡献者角色：为 Paimon 贡献代码
**学习重点**: 架构设计、代码规范、测试
1. 完整的 LSM-Tree 实现（BATCH1-3）
2. 两阶段提交协议（BATCH6）
3. 元数据管理（BATCH8）
4. 设计模式应用（各批次文档）

---

#### 研究者角色：研究存储引擎
**学习重点**: 算法、数据结构、优化技术
1. LSM-Tree 理论和实现（BATCH1-3）
2. 空间填充曲线（BATCH10）
3. Bloom Filter 和 Bitmap 索引（BATCH24）
4. 删除向量（INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md）

---

## 5. 技术对比和架构图

### 5.1 五种分桶模式完整对比

#### 详细对比表
| 特性 | HASH_FIXED | HASH_DYNAMIC | KEY_DYNAMIC | POSTPONE_MODE | BUCKET_UNAWARE |
|------|-----------|--------------|-------------|---------------|----------------|
| **桶数量** | 固定 | 动态增长 | 按主键动态 | 延迟决策 | 固定为 0 |
| **扩容能力** | ❌ 不支持 | ✅ 支持 | ✅ 支持 | ✅ 支持 | ❌ 不支持 |
| **跨分区更新** | ❌ 不支持 | ❌ 不支持 | ✅ 支持 | ✅ 支持 | ❌ 不支持 |
| **全局索引** | ❌ 不需要 | ✅ 需要 | ✅ 需要 | ✅ 需要 | ❌ 不需要 |
| **写入性能** | ⚡ 极快 | 🚀 快 | 🐢 中等 | 🐢 中等 | ⚡ 极快 |
| **查询性能** | ⚡ 快 | ⚡ 快 | 🚀 中等 | 🚀 中等 | 🐢 慢 |
| **内存开销** | 低 | 中等 | 高 | 高 | 极低 |
| **适用场景** | 数据均匀分布 | 数据倾斜 | 跨分区主键更新 | 全局优化分桶 | Append-Only 表 |
| **典型应用** | OLAP 查询表 | 实时写入表 | CDC 同步表 | 批量 ETL | 日志/事件流 |

#### 架构对比图
```
HASH_FIXED:
User Data → hash(bucket_key) % N → Bucket N → Write

HASH_DYNAMIC:
User Data → DynamicBucketAssigner → Bucket N (动态增长) → Update Index → Write

KEY_DYNAMIC:
User Data → GlobalIndex.get(primary_key) → (Partition, Bucket) → Write

POSTPONE_MODE:
User Data → Mark for postpone → Global Coordinator → Assign Bucket → Write

BUCKET_UNAWARE:
User Data → Bucket 0 (固定) → Write
```

📄 **详细文档**: `TABLE_SINK_TECH_SUMMARY.md`

---

### 5.2 主键表 vs 追加表对比

| 特性 | 主键表（Primary Key Table） | 追加表（Append-Only Table） |
|------|---------------------------|---------------------------|
| **主键** | ✅ 必须 | ❌ 无 |
| **更新** | ✅ 支持 | ❌ 不支持 |
| **删除** | ✅ 支持 | ❌ 不支持（仅删除向量） |
| **LSM-Tree** | ✅ 使用 | ❌ 不使用（分层但不合并键） |
| **压缩** | MergeTree 压缩 | 文件合并压缩 |
| **谓词下推** | 部分支持（主键前缀） | 完全支持 |
| **文件格式** | KeyValue（标准/精简） | InternalRow |
| **典型场景** | 维度表、CDC 同步 | 事实表、日志、事件流 |
| **写入性能** | 🚀 快（需合并） | ⚡ 极快（无合并） |
| **查询性能** | ⚡ 快（主键查询） | 🚀 快（范围查询） |

📄 **详细文档**: `BATCH13_PROGRESS.md`

---

### 5.3 三层元数据结构

```
┌──────────────────────────────────────────────────────┐
│                     Snapshot                         │
│  - id: 快照 ID                                       │
│  - schemaId: Schema 版本 ID                          │
│  - baseManifestList: 基础 Manifest 列表路径          │
│  - deltaManifestList: 增量 Manifest 列表路径         │
│  - changelogManifestList: Changelog 列表路径（可选） │
│  - commitUser, commitIdentifier, commitKind          │
│  - timeMillis, statistics                            │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│                  ManifestList                        │
│  - 包含多个 ManifestFileMeta                         │
│  - 每个 ManifestFileMeta 指向一个 ManifestFile       │
│  - 支持三种类型：Base、Delta、Changelog              │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│                  ManifestFile                        │
│  - 包含多个 ManifestEntry                            │
│  - 每个 ManifestEntry 包含:                          │
│    - FileKind: ADD, DELETE                           │
│    - Partition: 分区信息                             │
│    - Bucket: 桶编号                                  │
│    - DataFileMeta: 数据文件元信息                    │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│                  DataFileMeta                        │
│  - fileName: 文件名                                  │
│  - fileSize: 文件大小                                │
│  - rowCount: 行数                                    │
│  - minKey, maxKey: 键范围                            │
│  - keyStats, valueStats: 统计信息                    │
│  - minSequenceNumber, maxSequenceNumber              │
│  - level: LSM-Tree 层级                              │
│  - extraFiles: 删除向量、索引文件等                  │
└──────────────────────────────────────────────────────┘
```

📄 **详细文档**: `BATCH8_PROGRESS.md`

---

### 5.4 两阶段提交流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    阶段1: Prepare                           │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  1. Writer.prepareCommit()                                  │
│     - 触发内存数据刷写                                       │
│     - 生成新的数据文件                                       │
│     - 收集文件元信息（DataFileMeta）                         │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  2. 生成 CommitMessage                                      │
│     - newFilesIncrement: 新增文件                           │
│     - compactIncrement: 压缩结果                            │
│     - indexIncrement: 索引更新                              │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    阶段2: Commit                            │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  3. Commit.commit(messages)                                 │
│     - 汇总所有 CommitMessage                                │
│     - 过滤空消息                                            │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  4. 冲突检测                                                │
│     ├── Snapshot ID 检查                                    │
│     ├── 分区冲突检查                                        │
│     └── Append-Only 冲突检查                                │
│     - 如果冲突 → 重试或抛异常                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  5. 写入 ManifestList                                       │
│     - 合并 Base Manifest 和 Delta Manifest                  │
│     - 写入 Changelog Manifest（如果启用）                   │
│     - 返回 ManifestList 路径                                │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  6. 原子性写入 Snapshot                                     │
│     - 生成新的 Snapshot ID                                  │
│     - 创建 Snapshot 对象                                    │
│     - 原子性写入文件（snapshot-N）                          │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  7. 更新 LATEST 快照                                        │
│     - 更新 LATEST 符号链接/文件                             │
│     - 确保可见性                                            │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  8. 触发 CommitCallback                                     │
│     - 同步 Hive 分区                                        │
│     - 触发下游任务                                          │
│     - 记录审计日志                                          │
└─────────────────────────────────────────────────────────────┘
```

📄 **详细文档**: `BATCH6_PROGRESS.md`

---

### 5.5 LSM-Tree 层级结构

```
┌─────────────────────────────────────────────────────────────┐
│                        Level 0                              │
│  - 最新数据                                                 │
│  - 文件可能有重叠键                                         │
│  - 来源：内存刷写（MemTable → Immutable MemTable → Flush）│
│  - 大小：小文件（几MB到几十MB）                             │
└─────────────────────────────────────────────────────────────┘
                         ↓ Compaction
┌─────────────────────────────────────────────────────────────┐
│                        Level 1                              │
│  - 已合并一次的数据                                         │
│  - 文件键不重叠（同层内）                                   │
│  - 大小：中等文件（几十MB）                                 │
└─────────────────────────────────────────────────────────────┘
                         ↓ Compaction
┌─────────────────────────────────────────────────────────────┐
│                        Level 2                              │
│  - 已合并多次的数据                                         │
│  - 文件键不重叠（同层内）                                   │
│  - 大小：大文件（上百MB）                                   │
└─────────────────────────────────────────────────────────────┘
                         ↓ Compaction
┌─────────────────────────────────────────────────────────────┐
│                        Level N                              │
│  - 最老的数据                                               │
│  - 文件键不重叠（同层内）                                   │
│  - 大小：巨型文件（几百MB到GB）                             │
└─────────────────────────────────────────────────────────────┘

读取流程:
1. 从 Level 0 开始查找
2. 如果未找到，继续查找 Level 1
3. 如此递进，直到找到或到达最后一层
4. 使用 Bloom Filter 加速查找（跳过不包含键的文件）
```

📄 **详细文档**: `BATCH3_PROGRESS.md`

---

### 5.6 文件索引对比

| 索引类型 | 数据结构 | 空间复杂度 | 查询复杂度 | 假阳性 | 适用场景 |
|---------|---------|-----------|-----------|--------|---------|
| **Bloom Filter** | Bit 数组 + 哈希函数 | O(n) | O(k) | ✅ 有 | 点查询、IN 查询 |
| **Bitmap** | RoaringBitmap | O(n × cardinality) | O(1) | ❌ 无 | 低基数列（枚举） |
| **BSI** | 按位切片位图 | O(n × bits) | O(bits) | ❌ 无 | 数值范围查询 |
| **Range Bitmap** | 范围编码位图 | O(n × ranges) | O(log ranges) | ❌ 无 | 范围过滤 |

#### Bloom Filter 工作原理
```
插入 "hello":
1. hash1("hello") % m → 位置 a
2. hash2("hello") % m → 位置 b
3. hash3("hello") % m → 位置 c
4. 设置 bit[a], bit[b], bit[c] = 1

查询 "hello":
1. 计算 hash1, hash2, hash3
2. 检查 bit[a], bit[b], bit[c]
3. 全为 1 → 可能存在（假阳性）
4. 有 0 → 一定不存在（准确）
```

#### Bitmap 工作原理
```
列值: ['red', 'blue', 'red', 'green', 'blue']

Bitmap 索引:
red:   [1, 0, 1, 0, 0]
blue:  [0, 1, 0, 0, 1]
green: [0, 0, 0, 1, 0]

查询 WHERE color = 'red':
→ 返回位图 [1, 0, 1, 0, 0]
→ 行号 0, 2
```

#### BSI 工作原理
```
数值列: [5, 3, 7, 2]
二进制:  [101, 011, 111, 010]

BSI 位图:
bit2: [1, 0, 1, 0]
bit1: [0, 1, 1, 1]
bit0: [1, 1, 1, 0]

查询 WHERE value > 4:
→ 位运算组合
→ 返回位图 [1, 0, 1, 0]
→ 行号 0, 2
```

📄 **详细文档**: `BATCH24_FS_IO_PROGRESS.md`

---

### 5.7 全局索引架构

```
┌─────────────────────────────────────────────────────────────┐
│                    写入流程                                 │
└─────────────────────────────────────────────────────────────┘

User Data (Primary Key = "user123", Partition = "2024-01-01")
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  1. 查询全局索引                                            │
│     GlobalIndex.get("user123")                              │
│     → 返回 (Partition="2024-01-02", Bucket=5)              │
│       （旧数据位置）                                        │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  2. 删除旧数据                                              │
│     - 路由到 Partition="2024-01-02", Bucket=5               │
│     - 写入删除记录（RowKind.DELETE）                        │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  3. 写入新数据                                              │
│     - 路由到 Partition="2024-01-01", Bucket=3               │
│     - 写入插入记录（RowKind.INSERT）                        │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  4. 更新全局索引                                            │
│     GlobalIndex.put("user123", (Partition="2024-01-01", Bucket=3))│
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  全局索引存储                               │
└─────────────────────────────────────────────────────────────┘

BTree 索引（RocksDB）:
┌──────────────┬─────────────────────────┐
│ Primary Key  │ (Partition, Bucket)     │
├──────────────┼─────────────────────────┤
│ "user123"    │ ("2024-01-01", 3)       │
│ "user456"    │ ("2024-01-02", 7)       │
│ "user789"    │ ("2024-01-01", 5)       │
└──────────────┴─────────────────────────┘

Bitmap 索引（低基数主键）:
┌──────────────┬─────────────────────────┐
│ Primary Key  │ RoaringBitmap           │
├──────────────┼─────────────────────────┤
│ "VIP"        │ [bucket1, bucket3, ...]  │
│ "NORMAL"     │ [bucket2, bucket5, ...]  │
└──────────────┴─────────────────────────┘
```

📄 **详细文档**: `BATCH28_GLOBALINDEX_PROGRESS.md`

---

### 5.8 删除向量工作流程

```
┌─────────────────────────────────────────────────────────────┐
│              初始状态：数据文件                             │
└─────────────────────────────────────────────────────────────┘

data-file-1.parquet:
┌──────┬──────┬──────┐
│ Row0 │ Row1 │ Row2 │
│ Row3 │ Row4 │ Row5 │
│ Row6 │ Row7 │ Row8 │
└──────┴──────┴──────┘

                         ↓ 删除 Row1, Row4, Row7

┌─────────────────────────────────────────────────────────────┐
│       传统 Copy-On-Write: 重写整个文件                      │
└─────────────────────────────────────────────────────────────┘

data-file-2.parquet (新文件):
┌──────┬──────┬──────┐
│ Row0 │ Row2 │ Row3 │
│ Row5 │ Row6 │ Row8 │
└──────┴──────┴──────┘

问题:
- 需要重写整个文件
- 写放大严重
- I/O 开销大

┌─────────────────────────────────────────────────────────────┐
│       使用删除向量: 只记录删除行号                          │
└─────────────────────────────────────────────────────────────┘

data-file-1.parquet (原文件，不变):
┌──────┬──────┬──────┐
│ Row0 │ Row1 │ Row2 │
│ Row3 │ Row4 │ Row5 │
│ Row6 │ Row7 │ Row8 │
└──────┴──────┴──────┘

dv-file-1.dv (删除向量文件):
┌────────────────────────────┐
│ Deleted Rows: [1, 4, 7]    │
│ RoaringBitmap: 0b...       │
│ Cardinality: 3             │
└────────────────────────────┘

读取时:
1. 读取 data-file-1.parquet
2. 加载 dv-file-1.dv
3. 跳过行号 1, 4, 7
4. 返回 Row0, Row2, Row3, Row5, Row6, Row8

优势:
- ✅ 无需重写文件
- ✅ 减少写放大
- ✅ 快速删除（O(1)）
- ✅ RoaringBitmap 高效压缩
```

📄 **详细文档**: `INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md`

---

### 5.9 压缩算法对比

#### 性能对比
```
压缩比（越高越好）:
GZIP > ZSTD > LZ4 ≈ SNAPPY ≈ LZO

压缩速度（越快越好）:
LZ4 > SNAPPY ≈ LZO > ZSTD > GZIP

解压速度（越快越好）:
LZ4 > SNAPPY ≈ LZO > ZSTD > GZIP

综合平衡:
ZSTD（压缩比和速度平衡最佳）
```

#### 使用建议
```
实时写入场景:
→ 选择 LZ4（极快压缩/解压）

批量 ETL 场景:
→ 选择 ZSTD（平衡压缩比和速度）

归档存储场景:
→ 选择 GZIP（最高压缩比）

Hadoop 生态兼容:
→ 选择 SNAPPY 或 LZO
```

#### 配置示例
```java
// LZ4 压缩（推荐实时写入）
options.put("file.compression", "lz4");

// ZSTD 压缩，级别 3（推荐通用场景）
options.put("file.compression", "zstd");
options.put("file.compression.zstd.level", "3");

// GZIP 压缩，级别 6（归档场景）
options.put("file.compression", "gzip");
options.put("file.compression.gzip.level", "6");
```

---

### 5.10 序列化模式对比

#### 标准模式 vs 精简模式

```
标准模式（Standard Mode）:
┌──────────────────────────────────────────────────────────┐
│ 文件内容: [key, sequenceNumber, valueKind, value]       │
└──────────────────────────────────────────────────────────┘

示例记录:
Record 1: [key=(p1, 100), seq=1, kind=INSERT, value=(name="Alice", age=30)]
Record 2: [key=(p1, 200), seq=2, kind=UPDATE, value=(name="Bob", age=25)]

优点:
- ✅ 完整的 Key 信息
- ✅ 支持跨分区更新
- ✅ 文件可独立解析

缺点:
- ❌ Key 重复存储
- ❌ 文件体积大

┌──────────────────────────────────────────────────────────┐
│ 精简模式（Thin Mode）:                                   │
│ 文件内容: [sequenceNumber, valueKind, value]            │
│ Key 从文件名推断: bucket-{partition}-{bucket}.parquet   │
└──────────────────────────────────────────────────────────┘

示例记录:
Record 1: [seq=1, kind=INSERT, value=(name="Alice", age=30)]
Record 2: [seq=2, kind=UPDATE, value=(name="Bob", age=25)]

文件名: bucket-p1-5.parquet
→ Key 前缀: (partition=p1, bucket=5)

优点:
- ✅ 节省 30-50% 存储空间
- ✅ 减少序列化开销
- ✅ 提高压缩比

缺点:
- ❌ 依赖文件元数据
- ❌ 不支持跨分区更新

适用场景对比:
┌────────────────────┬──────────────┬──────────────┐
│ 场景               │ 标准模式     │ 精简模式     │
├────────────────────┼──────────────┼──────────────┤
│ 固定分桶           │ ✅           │ ✅ 推荐      │
│ 动态分桶           │ ✅           │ ✅ 推荐      │
│ 跨分区更新         │ ✅ 必须      │ ❌           │
│ KEY_DYNAMIC 模式   │ ✅ 必须      │ ❌           │
│ 存储成本敏感       │ ❌           │ ✅ 推荐      │
└────────────────────┴──────────────┴──────────────┘
```

📄 **详细文档**: `BATCH7_PROGRESS.md`

---

## 6. 附录

### 6.1 完成进度总览

#### 按模块统计
| 模块 | 总文件数 | 已完成 | 完成率 | 状态 |
|------|---------|--------|--------|------|
| **paimon-core** | 767 | 762 | 99.3% | ✅ 基本完成 |
| **paimon-common** | 575 | 82 | 14.3% | 🔄 进行中 |
| **paimon-api** | 199 | 0 | 0% | ⏳ 待开始 |
| **总计** | **1541** | **844** | **54.8%** | **🔄** |

#### 技术主题覆盖度
| 技术主题 | 核心文件数 | 已完成 | 完成率 |
|---------|-----------|--------|--------|
| LSM-Tree | 105 | 105 | 100% ✅ |
| 两阶段提交 | 10 | 10 | 100% ✅ |
| 分桶模式 | 15 | 15 | 100% ✅ |
| 列式存储 | 52 | 23 | 44% 🔄 |
| 类型系统 | 29 | 29 | 100% ✅ |
| 谓词下推 | 47 | 10 | 21% 🔄 |
| 文件索引 | 30 | 30 | 100% ✅ |
| 全局索引 | 13 | 13 | 100% ✅ |
| 压缩算法 | 7 | 7 | 100% ✅ |
| 序列化 | 50 | 35 | 70% 🔄 |
| REST API | 95 | 84 | 88% 🔄 |
| 删除向量 | 12 | 12 | 100% ✅ |
| 状态管理 | 12 | 3 | 25% 🔄 |
| 事务提交 | 15 | 15 | 100% ✅ |
| 权限管理 | 14 | 14 | 100% ✅ |

---

### 6.2 文档清单

#### 批次进度文档
1. `BATCH1_PROGRESS.md` - 聚合函数（45个文件）
2. `BATCH2_PROGRESS.md` - 压缩策略和合并函数（33个文件）
3. `BATCH3_PROGRESS.md` - MergeTree 主包（27个文件）
4. `BATCH4_PROGRESS.md` - 磁盘 I/O（19个文件）
5. `BATCH5_PROGRESS.md` - 文件 I/O（39个文件）
6. `BATCH6_PROGRESS.md` - 核心操作（36个文件）
7. `BATCH7_PROGRESS.md` - paimon-core 根目录（8个文件）
8. `BATCH8_PROGRESS.md` - Manifest 管理（27个文件）
9. `BATCH9_PROGRESS.md` - Catalog 管理（22个文件）
10. `BATCH10_PROGRESS.md` - Append-Only 表（23个文件）
11. `BATCH11_PROGRESS.md` - Schema 和分桶函数（10个文件）
12. `BATCH12_PROGRESS.md` - Utils 工具包（52个文件）
13. `BATCH13_PROGRESS.md` - Table 主包（24个文件）
14. `BATCH14_PROGRESS.md` - Table/Format（11个文件）
15. `BATCH15_PROGRESS.md` - Table/Sink（39个文件）
16. `BATCH16_PROGRESS.md` - Table/Source（78个文件）
17. `BATCH17_PROGRESS.md` - Table 剩余（32个文件）
18. `BATCH18_PROGRESS.md` - 中小型包（96个文件）
19-21. 其他批次（141个文件）

#### 技术总结文档
- `TABLE_SINK_TECH_SUMMARY.md` - 分桶模式、回调机制详解
- `SESSION_2026-02-11_FINAL_SUMMARY.md` - 列式存储、类型系统
- `REST_ANNOTATION_SUMMARY.md` - REST API 认证详解
- `INDEX_PRIVILEGE_DV_ANNOTATION_SUMMARY.md` - 索引、权限、删除向量
- `ICEBERG_ANNOTATION_SUMMARY.md` - Iceberg 兼容层
- `OVERALL_PROGRESS.md` - 总体进度和技术亮点汇总

---

### 6.3 快速参考卡片

#### 常用配置参数
```java
// 分桶模式
"bucket" = "10"                      // 固定分桶数
"bucket" = "-1"                      // 动态分桶
"bucket-key" = "user_id"             // 分桶键

// 压缩配置
"file.compression" = "zstd"          // 压缩算法
"file.compression.zstd.level" = "3"  // 压缩级别

// 写入配置
"write-buffer-size" = "256 MB"       // 写入缓冲区
"write-buffer-spillable" = "true"    // 允许溢写

// 查询配置
"scan.mode" = "all"                  // 扫描模式
"scan.snapshot-id" = "10"            // 指定快照

// 索引配置
"file.index.bloom-filter.columns" = "user_id,order_id"
"file.index.bitmap.columns" = "status,type"
```

#### 常用 SQL 命令
```sql
-- 创建主键表
CREATE TABLE users (
  user_id BIGINT,
  name STRING,
  age INT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket' = '10',
  'file.compression' = 'zstd'
);

-- 创建追加表
CREATE TABLE events (
  event_id BIGINT,
  event_time TIMESTAMP,
  event_type STRING
) WITH (
  'bucket' = '-1',
  'cluster.columns' = 'event_time'
);

-- 查询系统表
SELECT * FROM users$snapshots;
SELECT * FROM users$files;
SELECT * FROM users$schemas;
```

---

## 🎯 总结

本技术文档索引提供了 Apache Paimon 中文注释项目的全面导航，涵盖：

1. **17 个核心技术主题**，从 LSM-Tree 到权限管理
2. **三个模块的完整包导航**（paimon-core、paimon-common、paimon-api）
3. **快速查找核心类**，按功能分类
4. **三种学习路径**（新手入门、核心机制、高级特性）
5. **10 个技术对比和架构图**，可视化核心概念

**推荐学习路径**:
- 新手：从 Table 抽象层开始 → 写入/读取流程 → 元数据管理
- 贡献者：深入 LSM-Tree → 两阶段提交 → 完整架构
- 研究者：算法和数据结构 → 优化技术 → 论文对照

**项目亮点**:
- ✅ 99.3% 的 paimon-core 模块已完成详细中文注释
- ✅ 所有核心技术主题都有文档索引
- ✅ 提供完整的学习路径和技术对比

---

**文档维护**: 随着注释项目进展，本索引将持续更新。

**反馈渠道**: 如发现索引不准确或需要补充，请提交 Issue。

---

📖 **开始学习**: 选择一个技术主题或学习路径，开始探索 Apache Paimon 的技术细节！
