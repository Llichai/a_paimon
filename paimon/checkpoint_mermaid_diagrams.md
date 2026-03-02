# Paimon Checkpoint 延迟 - Mermaid 时序图可视化

## 1. Primary Key 表 (MergeTree) Checkpoint 完整流程

```mermaid
sequenceDiagram
    participant FK as Flink Checkpoint
    participant STCMT as StreamTableCommit
    participant TW as TableWriteImpl
    participant AFSW as AbstractFileStoreWrite
    participant MTW as MergeTreeWriter
    participant CM as CompactManager
    participant CT as CompactTask

    FK->>STCMT: commit(checkpointId)
    activate STCMT

    STCMT->>TW: prepareCommit(waitCompaction=false, ckId)
    activate TW

    TW->>AFSW: prepareCommit(false, commitId)
    activate AFSW

    Note over AFSW: ⚠️ 嵌套循环开始 (串行处理)<br/>for each partition (P1...Pn):<br/>  for each bucket (B1...Bm):

    AFSW->>MTW: writer.prepareCommit(false)
    activate MTW

    MTW->>MTW: flushWriteBuffer(false, false)
    activate MTW

    Note over MTW: ⚠️ 第1个检查点<br/>shouldWaitForLatestCompaction()<br/>SortedRun数 > 4?

    MTW->>CM: shouldWaitForLatestCompaction()
    CM-->>MTW: true (文件堆积)
    Note over MTW: 强制设置:<br/>waitForLatestCompaction = true

    MTW->>CM: trySyncLatestCompaction(true)
    activate CM

    CM->>CM: getCompactionResult(blocking=true)

    Note over CT: ⏸️ 阻塞等待压缩<br/>(60-300s)
    CM->>CT: 等待 CompactTask 完成
    CT->>CT: k-way merge 多个文件

    Note over CT: 执行时间取决于:<br/>• 文件数量<br/>• 单个文件大小<br/>• 压缩策略

    CT-->>CM: 压缩完成，返回结果

    CM->>MTW: 结果返回
    deactivate CM

    MTW->>CM: triggerCompaction(false)
    Note over CM: 可能再次触发压缩

    deactivate MTW

    Note over MTW: ⚠️ 第2个检查点<br/>shouldWaitForPreparingCheckpoint()<br/>SortedRun数 > 5?

    MTW->>CM: shouldWaitForPreparingCheckpoint()
    CM-->>MTW: true (仍然太多文件)
    Note over MTW: 强制设置:<br/>waitCompaction = true

    MTW->>CM: trySyncLatestCompaction(true)
    activate CM

    CM->>CM: getCompactionResult(blocking=true)

    Note over CT: ⏸️ 再次阻塞等待<br/>(可能有新的压缩任务)<br/>(60-300s)
    CM->>CT: 等待可能的新压缩任务
    CT->>CT: 继续 k-way merge

    CT-->>CM: 完成

    CM->>MTW: 结果返回
    deactivate CM

    MTW->>MTW: drainIncrement()
    MTW-->>AFSW: CommitIncrement
    deactivate MTW

    Note over AFSW: ⚠️ 开始下一个循环<br/>for next partition/bucket

    AFSW->>AFSW: Writer 清理检查
    Note over AFSW: !compactNotCompleted()<br/>可能再次触发压缩

    AFSW-->>TW: List&lt;CommitMessage&gt;
    deactivate AFSW

    TW-->>STCMT: 提交消息列表
    deactivate TW

    STCMT-->>FK: Checkpoint 完成
    deactivate STCMT

    Note over FK: 总时间: 1s (正常)<br/>或 5-10分钟 (初始化后)
```

---

## 2. Append-Only 表 Checkpoint 完整流程

```mermaid
sequenceDiagram
    participant FK as Flink Checkpoint
    participant STCMT as StreamTableCommit
    participant TW as TableWriteImpl
    participant AFSW as AbstractFileStoreWrite
    participant AOW as AppendOnlyWriter
    participant CM as CompactManager
    participant CT as CompactTask

    FK->>STCMT: commit(checkpointId)
    activate STCMT

    STCMT->>TW: prepareCommit(waitCompaction=false, ckId)
    activate TW

    TW->>AFSW: prepareCommit(false, commitId)
    activate AFSW

    Note over AFSW: ⚠️ 嵌套循环开始 (串行处理)<br/>for each partition (P1...Pn): # 7 个分区<br/>  for each bucket (B1...Bm):     # 4 个 bucket<br/>    总共 28 个 writer 逐个处理

    loop 对每个 Partition/Bucket
        AFSW->>AOW: writer.prepareCommit(false)
        activate AOW

        AOW->>AOW: flush(false, false)
        activate AOW

        Note over AOW: ⚠️ 创建新文件<br/>sinkWriter.flush()

        AOW->>CM: addNewFile(file)

        AOW->>CM: trySyncLatestCompaction(false)
        activate CM

        Note over CM: 如果有待处理的压缩:<br/>getCompactionResult(blocking=false)<br/>可能立即返回或等待

        CM-->>AOW: 压缩结果 (如果有)
        deactivate CM

        AOW->>CM: triggerCompaction(false)

        deactivate AOW

        AOW->>CM: trySyncLatestCompaction(false || forceCompact)
        activate CM

        Note over CT: 如果 forceCompact=true<br/>或之前有未完成压缩:<br/>⏸️ 可能阻塞等待<br/>(30-60s)
        CM->>CT: 等待压缩完成
        CT->>CT: k-way merge (合并小文件)

        CT-->>CM: 完成
        CM-->>AOW: 结果
        deactivate CM

        AOW->>AOW: drainIncrement()

        Note over AOW: ⚠️ 返回到 AbstractFileStoreWrite<br/>进行 Writer 清理检查

        deactivate AOW

        AFSW->>AFSW: Writer 清理检查
        activate AFSW

        Note over AFSW: if (committable.isEmpty())<br/>  if (writerCleanChecker.apply())<br/>    调用: !compactNotCompleted()

        AFSW->>AOW: compactNotCompleted()
        activate AOW

        Note over AOW: ⚠️ 额外压缩触发!<br/>triggerCompaction(false)

        AOW->>CM: triggerCompaction(false)

        Note over CM: 可能产生新的压缩任务<br/>即使之前说不再有压缩了!

        CM-->>AOW: void

        AOW->>CM: compactNotCompleted()
        CM-->>AOW: boolean

        AOW-->>AFSW: boolean
        deactivate AOW

        deactivate AFSW

        AFSW-->>AOW: void (writer close 或保留)
        deactivate AOW

        Note over AFSW: ✓ 当前 partition/bucket 完成<br/>⚠️ 继续下一个循环 (串行!)
    end

    AFSW-->>TW: List&lt;CommitMessage&gt;
    deactivate AFSW

    TW-->>STCMT: 提交消息列表
    deactivate TW

    STCMT-->>FK: Checkpoint 完成
    deactivate STCMT

    Note over FK: 总时间: 1s (正常)<br/>或 5-10分钟 (初始化后)<br/>N×M 个 writer 串行执行 ⚠️
```

---

## 3. 恢复后的文件堆积导致的级联压缩

```mermaid
graph TD
    A["Task 失败/重启"] -->|恢复快照| B["WriteRestore.restoreFiles"]

    B -->|加载所有文件| C["每个 Partition/Bucket 初始化"]

    C --> D1["P1,B1: 10个文件"]
    C --> D2["P1,B2: 8个文件"]
    C --> D3["P2,B1: 12个文件"]
    C --> D4["P2,B2: 9个文件"]

    D1 --> E["Checkpoint 准备"]
    D2 --> E
    D3 --> E
    D4 --> E

    E --> F["P1,B1: 压缩 10个文件"]
    E --> G["P1,B2: 压缩 8个文件"]
    E --> H["P2,B1: 压缩 12个文件"]
    E --> I["P2,B2: 压缩 9个文件"]

    F -->|60s| J["⏸️ 第1个Checkpoint"]
    G -->|50s| J
    H -->|70s| J
    I -->|55s| J

    J -->|等待所有完成| K["总时间: 235s ≈ 4分钟"]

    K --> L["第2个Checkpoint"]
    L -->|由于仍有待处理| M["再次触发压缩"]
    M -->|累计等待| N["总时间: 5-10分钟"]

    N --> O["第3/4 Checkpoint"]
    O -->|文件逐渐减少| P["时间逐渐恢复到 1-2秒"]

    style A fill:#ff6b6b
    style J fill:#ff6b6b
    style N fill:#ff6b6b
    style K fill:#ffa94d
    style P fill:#51cf66
```

---

## 4. 时间成本分解 (初始化后)

```mermaid
graph LR
    Total["总时间: 300s<br/>(5分钟)"]

    subgraph MergeTree["MergeTree 表额外成本"]
        Wait1["shouldWait<br/>LatestCompaction<br/>60s"]
        Wait2["shouldWait<br/>PreparingCheckpoint<br/>120s"]
    end

    subgraph Append["Append-Only 表"]
        Serial["串行处理<br/>8×35s<br/>= 280s"]
        Extra["compactNotCompleted<br/>额外触发<br/>= 80s"]
    end

    subgraph Common["共同成本"]
        Compress["压缩本身<br/>(k-way merge)<br/>= 200s"]
    end

    Total --> MergeTree
    Total --> Append
    Total --> Common

    style Wait1 fill:#ff4757
    style Wait2 fill:#ff4757
    style Serial fill:#ffa502
    style Extra fill:#ff6348
    style Compress fill:#ff9500
```

---

## 5. 关键延迟部分代码对应

```mermaid
mindmap
  root((Checkpoint 延迟<br/>5 分钟))
    MergeTree 专有
      shouldWaitForLatestCompaction
        MergeTreeCompactManager.java:168
        条件: SortedRun > 4
        影响: flushWriteBuffer 强制等待
        时间: 60-300s
      shouldWaitForPreparingCheckpoint
        MergeTreeCompactManager.java:181
        条件: SortedRun > 5
        影响: prepareCommit 再次强制等待
        时间: 60-300s
    通用延迟
      串行处理 Partition/Bucket
        AbstractFileStoreWrite.java:405
        N×M 个 writer 逐个调用
        影响: 线性累加延迟
        时间: N×M×(30-60s)
      compactNotCompleted 额外触发
        AppendOnlyWriter.java:345
        Writer 清理时检查
        影响: 额外压缩任务
        时间: 20-60s/writer
      压缩任务本身
        k-way merge 算法
        文件数量多时成本高
        影响: 单个 writer 的 30-60s
        时间: O(N log k)
    恢复文件堆积
      WriteRestore 恢复所有文件
        加载快照中的所有文件
        影响: Level-0 堆积 10+ 个
        时间: 恢复时间 < 1s
      初始化后压缩成本陡增
        10 个文件的 merge 成本高
        影响: 每个 writer 60s
        时间: 倍数差异 150x-300x
```

---

## 6. 关键代码行号对应

| 延迟阶段 | 文件 | 行号 | 代码片段 | 影响 |
|---------|------|------|--------|------|
| **1. 第1次阻塞** | MergeTreeWriter | 550-603 | `flushWriteBuffer(W, false)` | 60-300s |
| | MergeTreeCompactManager | 168-169 | `return levels...> numSortedRunStopTrigger` | 决定强制等待 |
| | MergeTreeWriter | 553-554 | `if (shouldWait...) W = true` | 强制等待 |
| | MergeTreeWriter | 600 | `trySyncLatestCompaction(W)` | ⏸️ 第1个阻塞点 |
| **2. 第2次阻塞** | MergeTreeWriter | 634-635 | `if (shouldWaitFor...) W = true` | 强制等待 |
| | MergeTreeWriter | 639 | `trySyncLatestCompaction(W)` | ⏸️ 第2个阻塞点 |
| **3. 串行循环** | AbstractFileStoreWrite | 393-405 | 嵌套 for 循环 | N×M×(30-60s) |
| | AbstractFileStoreWrite | 405 | `increment = writer.prepareCommit(W)` | 逐个调用 |
| **4. 额外触发** | AbstractFileStoreWrite | 485 | `!writer.compactNotCompleted()` | 额外 20-60s |
| | AppendOnlyWriter | 345 | `triggerCompaction(false)` | 再次触发 |

---

## 7. 恢复过程中的变化

```mermaid
timeline
    title Checkpoint 延迟恢复过程

    section 任务状态
    失败重启 : 恢复快照 : 初始化完成
    section 文件数量
    多个小文件堆积 (10+) : 文件仍未完全压缩 : 逐渐减少
    section 单次 CK 耗时
    5-10分钟 : 4-5分钟 : 3分钟 : 2分钟 : 1分钟 : 1秒
    section 原因
    Level-0 堆积+阻塞 : 仍在压缩 : 压缩进行中 : 压缩即将完成 : 压缩完成 : 恢复正常

    section CK 序号
    1st CK : 2nd CK : 3rd CK : 4th CK : 5th CK : 6th+ CK
```
