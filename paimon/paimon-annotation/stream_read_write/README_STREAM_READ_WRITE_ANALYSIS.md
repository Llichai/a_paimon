# Paimon 流读流写分析 - 文档总索引

> 本文档是 Paimon 流读流写实现的完整分析文档集，包含架构设计、时序图、代码示例和快速参考。

## 📚 文档列表

### 1. 📄 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md) - 核心分析
**内容**：详细的架构说明和时序图描述

- [概述](#paimon-流读流写分析-文档总索引) - 分层架构
- [流写（Stream Write）](#流写stream-write) - 写入详解
  - 核心类关系
  - 初始化阶段
  - 数据写入阶段
  - Checkpoint 提交阶段
  - 关键特性说明
- [流读（Stream Read）](#流读stream-read) - 读取详解
  - 核心类关系
  - 初始化和恢复阶段
  - 单次扫描循环
  - 数据读取阶段
  - Checkpoint 保存和恢复
  - 流读模式详解
- [核心交互机制](#核心交互机制) - 读写协调
  - Exactly-Once 保证机制
  - 快照（Snapshot）机制
  - 分桶（Bucket）与路由
  - 压缩（Compaction）
- [详细时序图](#详细时序图) - 场景示例
  - 完整的实时写读流程
  - 故障恢复中的重复检测

---

### 2. 📊 [PAIMON_STREAM_SEQUENCE_DIAGRAMS.md](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md) - PlantUML 时序图
**内容**：10 个高质量的 PlantUML 时序图

| # | 标题 | 说明 |
|---|-----|------|
| 1 | [流写初始化](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#1-流写初始化时序图) | 从 Builder 到 StreamTableWrite 的初始化过程 |
| 2 | [单行数据写入](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#2-单行数据写入时序图) | 单行数据经过各层组件的处理流程 |
| 3 | [Checkpoint 提交](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#3-checkpoint-提交时序图) | 从准备提交到快照创建的完整流程 |
| 4 | [流读初始化与恢复](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#4-流读初始化与恢复时序图) | 扫描器初始化和故障恢复 |
| 5 | [流读单次扫描循环](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#5-流读单次扫描循环时序图) | 首次扫描和增量扫描的完整流程 |
| 6 | [数据读取](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#6-数据读取时序图) | 从 Plan 到返回数据的读取流程 |
| 7 | [故障恢复与重复检测](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#7-故障恢复与重复检测时序图) | 故障后重复消息的检测和过滤 |
| 8 | [分桶分配](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#8-写入端分桶分配时序图) | 数据路由到不同分桶的过程 |
| 9 | [快照创建](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#9-快照创建与验证时序图) | 从 CommitMessage 到 Snapshot 的创建 |
| 10 | [端到端流程](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#10-完整端到端时序简化版) | 完整的实时读写流程 |

**使用方法**：
1. 复制任意 PlantUML 代码到 [PlantUML 在线编辑器](http://www.plantuml.com/plantuml/uml/)
2. 或使用 VS Code PlantUML 插件本地渲染
3. 导出为 PNG/SVG 用于演示或文档

---

### 3. 📖 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md) - 核心类与代码示例
**内容**：详细的类说明和实用代码示例

**流写核心类**：
- `StreamWriteBuilderImpl` - 构建器
- `StreamTableWrite` - 写入接口
- `TableWriteImpl<T>` - 实现层
- `FileStoreWrite<T>` - 底层文件写

**流读核心类**：
- `StreamTableScan` - 扫描接口
- `DataTableStreamScan` - 扫描实现
- `StartingScanner` - 起始扫描
- `FollowUpScanner` - 增量扫描
- `TableRead` - 读取接口
- `AbstractDataTableRead` - 实现基类

**代码示例**：
- [示例 1](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#示例-1-基础流写流读) - 基础流写流读
- [示例 2](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#示例-2-故障恢复) - 故障恢复
- [示例 3](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#示例-3-高级功能---谓词下推和列裁剪) - 高级功能
- [示例 4](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#示例-4-时间旅行---读取历史快照) - 时间旅行
- [示例 5](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#示例-5-flink-集成示例) - Flink 集成

**常见问题**：包含 8 个常见问题的解答

---

### 4. ⚡ [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md) - 快速参考
**内容**：快速查找和调试指南

- 最小化代码示例
- 类名速查表
- 常用方法速查
- 关键概念对照
- 配置参数
- 错误处理
- 性能调优
- 调试技巧
- 故障排查流程
- 快速检查清单

---

### 5. 🔗 [PAIMON_SOURCE_CODE_NAVIGATION.md](PAIMON_SOURCE_CODE_NAVIGATION.md) - 源码导航索引
**内容**：完整的源码文件位置导航

- 目录结构速览
- 流写核心类源码位置 + 关键行号
- 流读核心类源码位置 + 关键行号
- 关键辅助类导航
- 索引文件位置总表
- 快速查找技巧
- VS Code 快速打开
- Git 命令快速查看
- 调试技巧和断点建议

---

### 6. 📍 [PAIMON_SOURCECODE_QUICK_VIEW.md](PAIMON_SOURCECODE_QUICK_VIEW.md) - 源码关键位置速览
**内容**：按流程段落映射源码位置

- 流写全流程源码位置导航
- 流读全流程源码位置导航
- 关键交互点源码查看
- IDE 搜索技巧
- 推荐阅读顺序
- 按模块快速定位
- 常见问题的源码位置

---

## 🎯 快速导航

### 按使用场景

#### 🚀 我想快速上手

1. 阅读 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#快速启动) 的快速启动部分
2. 参考 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#代码示例) 的示例代码
3. 需要特定类的详解时，使用 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#流写核心类) 的类详解
4. **查看源码实现**：[PAIMON_SOURCECODE_QUICK_VIEW.md](PAIMON_SOURCECODE_QUICK_VIEW.md) 指引你快速找到源码位置

#### 📊 我想理解完整的架构

1. 先读 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md#概述) 的概述部分
2. 查看 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md#流写stream-write) 的流写详解
3. 查看 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md#流读stream-read) 的流读详解
4. 通过 [PAIMON_STREAM_SEQUENCE_DIAGRAMS.md](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md) 的时序图深化理解
5. **深入源码**：使用 [PAIMON_SOURCE_CODE_NAVIGATION.md](PAIMON_SOURCE_CODE_NAVIGATION.md) 快速定位源文件位置

#### 🔍 我想看时序图

1. 查看 [PAIMON_STREAM_SEQUENCE_DIAGRAMS.md](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md) 的 10 个时序图
2. 复制 PlantUML 代码到在线编辑器渲染
3. 根据需要导出为图片

#### ❓ 我需要解决问题

1. 查看 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#故障排查流程) 的故障排查流程
2. 查看 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#常见错误) 的常见错误
3. 查看 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#常见问题) 的常见问题
4. **定位问题源头**：使用 [PAIMON_SOURCECODE_QUICK_VIEW.md](PAIMON_SOURCECODE_QUICK_VIEW.md#-常见问题的源码位置) 快速查找源码位置

#### ⚙️ 我想优化性能

1. 阅读 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#性能调优建议) 的性能调优建议
2. 查看 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md#41-exactly-once-保证机制) 的相关特性说明
3. 根据场景选择合适的配置参数

## 🗺️ 核心概念地图

### 数据流向

```
应用程序
    ↓ write(row)
StreamTableWrite
    ↓
TableWriteImpl
    ├─ KeyAndBucketExtractor (提取分区/桶/主键)
    ├─ RecordExtractor (转换格式)
    └─ FileStoreWrite (底层写入)
        ├─ BucketedWriter (分桶写入)
        └─ RecordWriter (缓冲写入)
            ↓
        数据文件 (.parquet)
        Manifest 文件 (.manifest)
        快照文件 (.snapshot)
            ↓
StreamTableScan
    ├─ StartingScanner (首次扫描)
    └─ FollowUpScanner (增量扫描)
        ↓
FileStoreScan (底层扫描)
    ↓
TableRead + RecordReader
    ↓
InternalRow (数据行)
    ↓
应用程序
```

### 状态管理

```
写入端状态                        读取端状态
    ↓                              ↓
commitIdentifier (0, 1, 2...)   nextSnapshotId (1, 2, 3...)
    ↓                              ↓
prepareCommit() ←——————————→ checkpoint()
    ↓                              ↓
CommitMessage[] ←————————→ CommitMessage
    ↓                              ↓
commit() 应用变更          应用到 ReadPlan
    ↓                              ↓
创建新 Snapshot           读取 Snapshot 的数据
    ↓                              ↓
notifyCheckpointComplete() ←→ notifyCheckpointComplete()
    ↓                              ↓
清理过期文件                    清理过期快照
```

---

## 📋 文档地图

```
PAIMON_STREAM_READ_WRITE.md (主文档)
├─ 架构概述
├─ 流写详解
│  ├─ 初始化
│  ├─ 数据写入
│  ├─ Checkpoint 提交
│  └─ 关键特性
├─ 流读详解
│  ├─ 初始化和恢复
│  ├─ 扫描循环
│  ├─ 数据读取
│  └─ Checkpoint 机制
├─ 核心交互机制
│  ├─ Exactly-Once
│  ├─ 快照机制
│  ├─ 分桶与路由
│  └─ 压缩机制
└─ 详细时序图

PAIMON_STREAM_SEQUENCE_DIAGRAMS.md (时序图)
├─ 10 个 PlantUML 时序图
├─ 可视化各个流程
└─ 快速参考表

PAIMON_CORE_CLASSES_AND_EXAMPLES.md (实现详解)
├─ 流写核心类
│  ├─ StreamWriteBuilderImpl
│  ├─ StreamTableWrite
│  ├─ TableWriteImpl
│  └─ FileStoreWrite
├─ 流读核心类
│  ├─ StreamTableScan
│  ├─ DataTableStreamScan
│  ├─ TableRead
│  └─ AbstractDataTableRead
├─ 5 个完整代码示例
└─ 8 个常见问题解答

PAIMON_QUICK_REFERENCE.md (快速查找)
├─ 最小化代码示例
├─ 类名速查表
├─ 常用方法速查
├─ 配置参数
├─ 错误处理
├─ 性能调优
├─ 调试技巧
├─ 故障排查流程
└─ 快速检查清单

PAIMON_SOURCE_CODE_NAVIGATION.md (源码导航)
├─ 完整的源码目录结构
├─ 核心类源码位置表
├─ 关键行号索引
├─ 快速查找技巧
├─ IDE 快速打开
├─ Git 命令参考
└─ 调试断点建议

PAIMON_SOURCECODE_QUICK_VIEW.md (源码速览)
├─ 流写流程源码导航
├─ 流读流程源码导航
├─ 关键交互点源码查看
├─ IDE 搜索技巧
├─ 推荐阅读顺序
├─ 按模块快速定位
└─ 常见问题源码位置
```

---

## 🎓 学习路径建议

### 初级（理解基本概念）
1. 阅读 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md#概述) 的概述
2. 学习 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#快速启动) 的最小化示例
3. 查看 [PAIMON_STREAM_SEQUENCE_DIAGRAMS.md](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md#1-流写初始化时序图) 的初始化时序图
4. 运行 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#示例-1-基础流写流读) 的示例 1

**预期耗时**：2-3 小时

### 中级（掌握完整流程）
1. 深入学习 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md#流写stream-write) 的流写和 [流读详解](PAIMON_STREAM_READ_WRITE.md#流读stream-read)
2. 理解 [核心交互机制](PAIMON_STREAM_READ_WRITE.md#核心交互机制)
3. 学习所有 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md) 中的代码示例
4. 理解 [PAIMON_STREAM_SEQUENCE_DIAGRAMS.md](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md) 中的 10 个时序图
5. **查看源码实现**：使用 [PAIMON_SOURCECODE_QUICK_VIEW.md](PAIMON_SOURCECODE_QUICK_VIEW.md#-推荐阅读顺序) 的推荐阅读顺序

**预期耗时**：5-7 小时

### 高级（优化和故障排查）
1. 学习 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#性能调优建议) 的性能调优
2. 掌握 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#故障排查流程) 的故障排查
3. 理解 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#常见问题) 的常见问题
4. **深度代码分析**：使用 [PAIMON_SOURCE_CODE_NAVIGATION.md](PAIMON_SOURCE_CODE_NAVIGATION.md) 理解实现细节

**预期耗时**：8-10 小时

---

## 🔗 相关资源

### 源代码位置

**写入相关**：
- `paimon-core/src/main/java/org/apache/paimon/table/sink/`
- `paimon-core/src/main/java/org/apache/paimon/operation/`

**读取相关**：
- `paimon-core/src/main/java/org/apache/paimon/table/source/`
- `paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/`

**测试代码**：
- `paimon-core/src/test/java/org/apache/paimon/table/sink/`
- `paimon-core/src/test/java/org/apache/paimon/table/source/`

### 官方资源

- [Apache Paimon 官网](https://paimon.apache.org/)
- [GitHub 仓库](https://github.com/apache/incubator-paimon)
- [文档中心](https://paimon.apache.org/docs/master/)
- [社区讨论](https://github.com/apache/incubator-paimon/discussions)

### 相关项目

- **Flink Connector**: `paimon-flink/`
- **Spark Connector**: `paimon-spark/`
- **Trino Connector**: `paimon-trino/`
- **Java API**: `paimon-client-java/`

---

## 📝 文档维护信息

**创建日期**：2025-02-25

**包含的源文件**：
- `paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java`
- `paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableWrite.java`
- `paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java`
- `paimon-core/src/main/java/org/apache/paimon/table/sink/InnerTableWrite.java`
- `paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java`
- `paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java`
- `paimon-core/src/main/java/org/apache/paimon/table/source/AbstractDataTableRead.java`
- `paimon-core/src/main/java/org/apache/paimon/operation/FileStoreWrite.java`
- `paimon-core/src/main/java/org/apache/paimon/operation/FileStoreScan.java`

**版本覆盖**：Apache Paimon 0.4.0+

---

## 🤝 如何使用这些文档

### 方式 1：顺序阅读
1. 从 [PAIMON_STREAM_READ_WRITE.md](PAIMON_STREAM_READ_WRITE.md) 开始
2. 然后阅读 [PAIMON_STREAM_SEQUENCE_DIAGRAMS.md](PAIMON_STREAM_SEQUENCE_DIAGRAMS.md)
3. 参考 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md)
4. 最后查阅 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md)

### 方式 2：问题驱动
1. 遇到问题时，查看 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#常见错误) 的常见错误
2. 进行故障排查，参考 [PAIMON_QUICK_REFERENCE.md](PAIMON_QUICK_REFERENCE.md#故障排查流程)
3. 需要代码示例时，查阅 [PAIMON_CORE_CLASSES_AND_EXAMPLES.md](PAIMON_CORE_CLASSES_AND_EXAMPLES.md#代码示例)

### 方式 3：概念查询
1. 使用各文档的目录快速定位
2. 使用本文档的导航链接跳转
3. 使用快速参考表查找相关内容

---

## 📞 反馈与改进

如有以下建议，欢迎反馈：

- 📚 文档内容不准确或需要补充
- 🐛 代码示例中发现问题
- 💡 有更好的解释方式或示例
- 🤔 文档不够清晰需要优化
- 📊 时序图需要调整或增加

---

**祝您使用 Apache Paimon 愉快！** 🚀

