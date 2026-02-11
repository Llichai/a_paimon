# Table Sink 包技术要点总结

## 一、五种分桶模式对比

| 分桶模式 | 类名 | 桶ID来源 | 典型场景 | 优缺点 |
|---------|------|---------|---------|--------|
| **HASH_FIXED** | `FixedBucketRowKeyExtractor` | `hash(bucket_key) % N` | 数据均匀分布 | ✅ 简单高效 ❌ 无法扩容 |
| **HASH_DYNAMIC** | `DynamicBucketRowKeyExtractor` | 上层动态分配 | 数据倾斜场景 | ✅ 应对倾斜 ❌ 需要索引 |
| **KEY_DYNAMIC** | `RowPartitionAllPrimaryKeyExtractor` | 基于完整主键 | 主键动态分桶 | ✅ 灵活 ❌ 实现复杂 |
| **POSTPONE_MODE** | `PostponeBucketRowKeyExtractor` | 返回特殊标记 | 延迟分桶决策 | ✅ 可全局优化 ❌ 有延迟 |
| **BUCKET_UNAWARE** | `AppendTableRowKeyExtractor` | 固定返回 0 | 追加表 | ✅ 无开销 ❌ 无并行度 |

## 二、核心概念图

```
记录（InternalRow）
    ↓
KeyAndBucketExtractor
    ├── partition()        → 分区键（确定物理分区）
    ├── bucket()           → 桶ID（确定桶编号）
    └── trimmedPrimaryKey() → 裁剪主键（去除分区字段）
        ↓
SinkRecord
    ├── partition: BinaryRow
    ├── bucket: int
    ├── primaryKey: BinaryRow
    └── row: InternalRow
        ↓
写入到对应的分区-桶
```

## 三、分布式写入路由

### ChannelComputer 路由公式

```java
// 方式1：基于分区和桶（推荐）
channel = (hash(partition) + bucket) % numChannels
  ↓
- 同一分区的桶分散到相邻通道（局部性）
- 不同分区错开分布（避免热点）

// 方式2：仅基于桶（简化）
channel = bucket % numChannels
  ↓
- 简单直接
- 适用于无分区或分区不重要的场景
```

### 特殊处理：Integer.MIN_VALUE

```java
if (hashCode == Integer.MIN_VALUE) {
    hashCode = Integer.MAX_VALUE;
}
// 原因：Math.abs(MIN_VALUE) 仍为负数
// 影响：向后兼容性（Flink 用户可能从状态恢复）
```

## 四、回调机制对比

| 特性 | CommitCallback | TagCallback |
|------|---------------|-------------|
| **调用保证** | ✅ 保证被调用 | ❌ 不保证被调用 |
| **重复调用** | ✅ 可能多次 | ⚠️ 可能多次 |
| **幂等性要求** | ✅ 必须幂等 | ⚠️ 建议幂等 |
| **典型场景** | Hive 分区同步<br>数据质量检查<br>触发下游任务 | 外部系统标签同步<br>版本管理<br>数据发布 |

### CommitCallback 示例

```java
public class HivePartitionCallback implements CommitCallback {
    @Override
    public void call(..., Snapshot snapshot) {
        Set<String> newPartitions = extractPartitions(deltaFiles);
        for (String partition : newPartitions) {
            // 幂等操作：IF NOT EXISTS
            metastore.addPartitionIfNotExists(partition);
        }
    }
}
```

### TagCallback 示例

```java
public class IcebergTagCallback implements TagCallback {
    @Override
    public void notifyCreation(String tagName, long snapshotId) {
        // Iceberg 需要 snapshotId
        catalog.createTag(tagName, snapshotId);
    }
}
```

## 五、RowKindGenerator（CDC支持）

### 使用场景

从数据字段中提取 RowKind，而非使用 Paimon 的 RowKind API。

### 支持的字符串格式

| 字符串 | RowKind | 说明 |
|--------|---------|------|
| `'I'` 或 `'+I'` | INSERT | 插入 |
| `'U'` 或 `'+U'` | UPDATE_AFTER | 更新后 |
| `'-U'` | UPDATE_BEFORE | 更新前 |
| `'D'` 或 `'-D'` | DELETE | 删除 |

### 配置示例

```sql
CREATE TABLE cdc_table (
  id BIGINT,
  name STRING,
  op STRING  -- CDC 操作类型字段
) WITH (
  'rowkind.field' = 'op'
);
```

### 使用方式

```java
// 创建生成器
RowKindGenerator generator = RowKindGenerator.create(schema, options);

// 获取 RowKind
RowKind kind = RowKindGenerator.getRowKind(generator, row);
// 如果 generator 为 null，返回 row.getRowKind()
// 如果 generator 不为 null，从字段提取
```

## 六、InnerTableCommit 扩展配置

### 与公共接口的关系

```
TableCommit (公共 API)
    ↓
StreamTableCommit + BatchTableCommit
    ↓
InnerTableCommit (内部扩展)
    ↓
TableCommitImpl (实现)
```

### 扩展功能

| 方法 | 功能 | 说明 |
|------|------|------|
| `withOverwrite(Map)` | 覆盖写入 | 静态分区/动态分区覆盖 |
| `ignoreEmptyCommit(boolean)` | 空提交控制 | 流式默认false，批量默认true |
| `expireForEmptyCommit(boolean)` | 空提交过期 | 即使无新数据也触发过期 |
| `appendCommitCheckConflict(boolean)` | 追加冲突检测 | 追加表的冲突检测 |
| `rowIdCheckConflict(Long)` | Row ID冲突检测 | CDC场景的冲突检测 |

### 覆盖写入示例

```java
// 静态分区覆盖
commit.withOverwrite(Map.of("dt", "2024-01-01"))
      .commit(messages);

// 动态分区覆盖
commit.withOverwrite(null)
      .commit(messages);
```

## 七、序列化器版本管理

### AppendCompactTaskSerializer

- **当前版本**: Version 2
- **兼容性**: ❌ 不兼容旧版本
- **恢复策略**: 需要重启作业，不支持从 Savepoint 恢复

### MultiTableCompactionTaskSerializer

- **当前版本**: Version 1
- **与单表版本的区别**: 增加了表标识符（Identifier）
- **兼容性**: ❌ 不兼容旧版本

### 序列化格式对比

```
AppendCompactTaskSerializer (Version 2):
┌────────────────┐
│ partition      │ BinaryRow
├────────────────┤
│ num_files      │ int
├────────────────┤
│ file_1..n      │ DataFileMeta[]
└────────────────┘

MultiTableCompactionTaskSerializer (Version 1):
┌────────────────┐
│ partition      │ BinaryRow
├────────────────┤
│ num_files      │ int
├────────────────┤
│ file_1..n      │ DataFileMeta[]
├────────────────┤
│ table_id       │ Identifier  ← 多表版本增加
└────────────────┘
```

## 八、设计模式应用

### 1. 模板方法模式

```java
RowKeyExtractor (抽象基类)
├── setRecord() ✅ 已实现
├── partition() ✅ 已实现（委托给 RowPartitionKeyExtractor）
├── trimmedPrimaryKey() ✅ 已实现（委托）
└── bucket() ⚠️ 抽象方法（子类实现）
    ↓
子类只需实现 bucket() 方法
```

### 2. 策略模式

```java
KeyAndBucketExtractor (策略接口)
    ↓
不同的分桶策略实现：
├── FixedBucketRowKeyExtractor    (固定分桶)
├── DynamicBucketRowKeyExtractor  (动态分桶)
├── PostponeBucketRowKeyExtractor (延迟分桶)
└── AppendTableRowKeyExtractor    (无分桶)
```

### 3. 委托模式

```java
RowKeyExtractor
    ↓ 委托
RowPartitionKeyExtractor
    ↓ 使用
代码生成的 Projection (高性能字段提取)
```

### 4. 适配器模式

```java
ChannelComputer.transform(input, converter)
    ↓
将一种类型的计算器转换为另一种类型
例如：InternalRow → SinkRecord
```

## 九、性能优化技巧

### 1. 缓存机制

```java
// RowKeyExtractor 缓存提取结果
private BinaryRow partition;
private BinaryRow trimmedPrimaryKey;

// FixedBucketRowKeyExtractor 额外缓存
private BinaryRow reuseBucketKey;
private Integer reuseBucket;
```

### 2. 复用优化

```java
// 当分桶键 = 裁剪主键时，直接复用
if (sameBucketKeyAndTrimmedPrimaryKey) {
    return trimmedPrimaryKey();
}
```

### 3. 懒初始化

```java
// 序列化场景：extractor 标记为 transient
private transient KeyAndBucketExtractor<InternalRow> extractor;

// 首次使用时初始化
if (extractor == null) {
    extractor = new FixedBucketRowKeyExtractor(schema);
}
```

## 十、关键数据流

### 写入流程

```
InternalRow
    ↓
RowKeyExtractor.setRecord(row)
    ↓
提取: partition, bucket, primaryKey
    ↓
封装为 SinkRecord
    ↓
ChannelComputer.channel(record)
    ↓
路由到指定通道
    ↓
TableWrite.write(record)
    ↓
写入对应的分区-桶
```

### 提交流程

```
TableWrite.prepareCommit()
    ↓
生成 CommitMessage
    ↓
TableCommit.commit(messages)
    ↓
写入 Manifest
    ↓
生成 Snapshot
    ↓
触发 CommitCallback.call()
    ↓
外部系统同步/后处理
```

---

**核心价值**：
- ✅ 灵活的分桶策略，适应不同场景
- ✅ 完善的分布式支持，提高吞吐量
- ✅ 强大的回调机制，易于集成
- ✅ 精心设计的抽象，代码复用性强
