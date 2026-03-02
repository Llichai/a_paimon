# Paimon Checkpoint 延迟 - 时序图和关键部分分析

## 1. Primary Key 表 (MergeTree) 的 Checkpoint 时序图

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                        Flink Checkpoint 触发流程                                      │
│                                                                                        │
│  Flink JobManager           TaskManager            Write Task          Compact Thread │
│         │                        │                      │                    │        │
│         │                        │                      │                    │        │
│  startCheckpoint────────────────►│                      │                    │        │
│         │                        │ prePrepareCommit    │                    │        │
│         │                        ├─────────────────────►│                    │        │
│         │                        │  prepareCommit(ck=N)│                    │        │
│         │                        │   waitCompaction=?  │                    │        │
│         │                        │  (Flink决定)        │                    │        │
│         │                        │                     ▼                    │        │
│         │                        │            ╔═══════════════════════╗    │        │
│         │                        │            ║ StreamTableWrite      ║    │        │
│         │                        │            ║ .prepareCommit()      ║    │        │
│         │                        │            ╚═════════════╤═════════╝    │        │
│         │                        │                         │               │        │
│         │                        │                         ▼               │        │
│         │                        │            ╔═══════════════════════╗    │        │
│         │                        │            ║ TableWriteImpl         ║    │        │
│         │                        │            ║ .prepareCommit()      ║    │        │
│         │                        │            ╚═════════════╤═════════╝    │        │
│         │                        │                         │               │        │
│         │                        │                         ▼               │        │
│         │                        │            ╔═══════════════════════╗    │        │
│         │                        │            ║ AbstractFileStoreWrite║    │        │
│         │                        │            ║ .prepareCommit()      ║    │        │
│         │                        │            ║ (外层循环)             ║    │        │
│         │                        │            ╚═════════════╤═════════╝    │        │
│         │                        │                         │               │        │
│         │                        │                         ├─ for each partition (P)
│         │                        │                         │   ├─ for each bucket (B)
│         │                        │                         │   │   │
│         │                        │                         │   │   ▼
│         │                        │                         │   │ ╔═══════════════════╗
│         │                        │                         │   │ ║ MergeTreeWriter   ║
│         │                        │                         │   │ ║ .prepareCommit(W?)║
│         │                        │                         │   │ ╚════════╤══════════╝
│         │                        │                         │   │          │
│         │                        │                         │   │          ▼
│         │                        │                         │   │ ╔═══════════════════════════╗
│         │                        │                         │   │ ║ flushWriteBuffer()        ║
│         │                        │                         │   │ ║ (第623行)                 ║
│         │                        │                         │   │ ╚════════╤═════════════════╝
│         │                        │                         │   │          │
│         │                        │                         │   │    ┌─────┴────────────────┐
│         │                        │                         │   │    │                      │
│         │                        │                         │   │    ▼                      │
│         │                        │                         │   │ ╔════════════════════╗  │
│         │                        │                         │   │ ║ shouldWaitFor      ║  │
│         │                        │                         │   │ ║ LatestCompaction() ║  │
│         │                        │                         │   │ ║ (第553行)          ║  │
│         │                        │                         │   │ ║ 返回 true? → 等待  ║  │
│         │                        │                         │   │ ╚════════╤═══════════╝  │
│         │                        │                         │   │          │              │
│         │                        │                         │   │          ▼              │
│         │                        │                         │   │ ╔════════════════════╗  │
│         │                        │                         │   │ ║ trySyncLatest      ║  │
│         │                        │                         │   │ ║ Compaction(W?)     ║  │
│         │                        │                         │   │ ║ (第600行)          ║  │
│         │                        │                         │   │ ║ ⏸️  BLOCKING WAIT  ║  │
│         │                        │                         │   │ ║ getCompactionResult║  │
│         │                        │                         │   │ ║ (true)→F.get()    ║  │
│         │                        │                         │   │ ╚════════╤═══════════╝  │
│         │                        │                         │   │          │              │
│         │                        │                         │   │          │ 等待压缩完成   │
│         │                        │                         │   │          ├─────────────►│ CompactTask
│         │                        │                         │   │          │   running... │ .call()
│         │                        │                         │   │          │              │
│         │                        │                         │   │          │ (60-300s)    │ k-way merge
│         │                        │                         │   │          │              │ 多个文件
│         │                        │                         │   │          │◄─────────────┤ 完成
│         │                        │                         │   │          │              │
│         │                        │                         │   │    └──────┬─────────────┘
│         │                        │                         │   │           │
│         │                        │                         │   │           ▼
│         │                        │                         │   │ ╔═══════════════════════════╗
│         │                        │                         │   │ ║ 返回到 prepareCommit()    ║
│         │                        │                         │   │ ║ 继续...                  ║
│         │                        │                         │   │ ║ (第625-643行)            ║
│         │                        │                         │   │ ╚════════╤════════════════╝
│         │                        │                         │   │          │
│         │                        │                         │   │    ┌─────┴──────────────────┐
│         │                        │                         │   │    │                        │
│         │                        │                         │   │    ▼                        │
│         │                        │                         │   │ ╔═════════════════════╗    │
│         │                        │                         │   │ ║ if (commitForce     ║    │
│         │                        │                         │   │ ║    Compact)         ║    │
│         │                        │                         │   │ ║   waitCompaction=T  ║    │
│         │                        │                         │   │ ║ (第627行)           ║    │
│         │                        │                         │   │ ╚════════╤════════════╝    │
│         │                        │                         │   │          │                │
│         │                        │                         │   │    ┌─────┴──────────────┐ │
│         │                        │                         │   │    │                    │ │
│         │                        │                         │   │    ▼                    │ │
│         │                        │                         │   │ ╔════════════════════╗ │ │
│         │                        │                         │   │ ║ shouldWaitFor      ║ │ │
│         │                        │                         │   │ ║ PreparingCheckpoint║ │ │
│         │                        │                         │   │ ║ (第634行) ⚠️ 关键 ║ │ │
│         │                        │                         │   │ ║ > numSortedRun+1?  ║ │ │
│         │                        │                         │   │ ║ YES → 强制等待！    ║ │ │
│         │                        │                         │   │ ╚════════╤═══════════╝ │ │
│         │                        │                         │   │          │             │ │
│         │                        │                         │   │          ▼             │ │
│         │                        │                         │   │ ╔════════════════════╗ │ │
│         │                        │                         │   │ ║ trySyncLatest      ║ │ │
│         │                        │                         │   │ ║ Compaction(true)   ║ │ │
│         │                        │                         │   │ ║ (第639行)          ║ │ │
│         │                        │                         │   │ ║ ⏸️  BLOCKING WAIT  ║ │ │
│         │                        │                         │   │ ║ getCompactionResult║ │ │
│         │                        │                         │   │ ║ (true)→F.get()    ║ │ │
│         │                        │                         │   │ ╚════════╤═══════════╝ │ │
│         │                        │                         │   │          │             │ │
│         │                        │                         │   │          │ 再次等待压缩完成 │ │
│         │                        │                         │   │          ├──────────────►│ 如果仍有待处理
│         │                        │                         │   │          │   running... │ 压缩任务
│         │                        │                         │   │          │              │
│         │                        │                         │   │          │ (60-300s)    │
│         │                        │                         │   │          │              │
│         │                        │                         │   │          │◄──────────────┤
│         │                        │                         │   │          │              │
│         │                        │                         │   │    └──────┬─────────────┘
│         │                        │                         │   │           │
│         │                        │                         │   │           ▼
│         │                        │                         │   │ ╔════════════════════╗
│         │                        │                         │   │ ║ drainIncrement()   ║
│         │                        │                         │   │ ║ (第642行)          ║
│         │                        │                         │   │ ╚════════╤═══════════╝
│         │                        │                         │   │          │
│         │                        │                         │   │          ▼
│         │                        │                         │   │ ╔════════════════════╗
│         │                        │                         │   │ ║ return Committed   ║
│         │                        │                         │   │ ║ Increment          ║
│         │                        │                         │   │ ╚═════════╤══════════╝
│         │                        │                         │   │          │
│         │                        │                         │   └──────────┤────────────────┐
│         │                        │                         │              │                │
│         │                        │ ⚠️ 此处开始下一个循环  (P, B+1) 或 (P+1, B=0)        │
│         │                        │                         │              │                │
│         │                        │◄────────────────────────┤──────────────┤────────────────┘
│         │                        │    提交消息列表         │              │
│         │                        │                         │              │
│         ▼                        ▼                         │              │
│ Commit Consensus                 │                         │              │
│                                  │                         │              │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Append-Only 表的 Checkpoint 时序图

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                    Append-Only 表 Checkpoint 流程                                     │
│                                                                                        │
│  Flink JobManager           TaskManager            Write Task          Compact Thread │
│         │                        │                      │                    │        │
│         │                        │                      │                    │        │
│  startCheckpoint────────────────►│                      │                    │        │
│         │                        │ prePrepareCommit    │                    │        │
│         │                        ├─────────────────────►│                    │        │
│         │                        │  prepareCommit(ck=N)│                    │        │
│         │                        │                      │                    │        │
│         │                        │                      ▼                    │        │
│         │                        │            ╔═══════════════════════╗    │        │
│         │                        │            ║ AbstractFileStoreWrite║    │        │
│         │                        │            ║ .prepareCommit()      ║    │        │
│         │                        │            ║ (外层循环)             ║    │        │
│         │                        │            ╚════════╤══════════════╝    │        │
│         │                        │                     │                   │        │
│         │                        │        ⚠️ 嵌套循环开始 (串行处理)        │        │
│         │                        │        for each partition (P1, P2...)   │        │
│         │                        │        ├─ for each bucket (B1, B2...) │        │
│         │                        │        │   │                          │        │
│         │                        │        │   ▼                          │        │
│         │                        │        │ ╔══════════════════════╗     │        │
│         │                        │        │ ║ Partition=P1         ║     │        │
│         │                        │        │ ║ Bucket=B1            ║     │        │
│         │                        │        │ ║ AppendOnlyWriter     ║     │        │
│         │                        │        │ ║ .prepareCommit(W)    ║     │        │
│         │                        │        │ ╚════════╤═════════════╝     │        │
│         │                        │        │          │                   │        │
│         │                        │        │          ▼                   │        │
│         │                        │        │ ╔══════════════════════╗     │        │
│         │                        │        │ ║ flush(false,false)   ║     │        │
│         │                        │        │ ║ (第338行)            ║     │        │
│         │                        │        │ ║ 不传 waitCompaction! ║     │        │
│         │                        │        │ ║ (第350行)            ║     │        │
│         │                        │        │ ╚════════╤═════════════╝     │        │
│         │                        │        │          │                   │        │
│         │                        │        │          ├─ sinkWriter.flush()        │
│         │                        │        │          │                   │        │
│         │                        │        │          ├─ compactManager  │        │
│         │                        │        │          │  .addNewFile()   │        │
│         │                        │        │          │                   │        │
│         │                        │        │          ├─ trySyncLatest   │        │
│         │                        │        │          │  Compaction(W?)  │        │
│         │                        │        │          │  (第355行)       │        │
│         │                        │        │          │  可能阻塞        │        │
│         │                        │        │          │                   │        │
│         │                        │        │          ├─ triggerCompaction       │
│         │                        │        │          │  (false)         │        │
│         │                        │        │          │  (第356行)       │        │
│         │                        │        │          │                   │        │
│         │                        │        │          ▼                   │        │
│         │                        │        │ ╔════════════════════════╗   │        │
│         │                        │        │ ║ trySyncLatestCompaction║   │        │
│         │                        │        │ ║ (waitCompaction||      ║   │        │
│         │                        │        │ ║  forceCompact)         ║   │        │
│         │                        │        │ ║ (第339行)              ║   │        │
│         │                        │        │ ║ ⏸️  可能阻塞等待       ║   │        │
│         │                        │        │ ║ getCompactionResult    ║   │        │
│         │                        │        │ ╚═══════╤════════════════╝   │        │
│         │                        │        │         │                    │        │
│         │                        │        │         │ 等待压缩结果        │        │
│         │                        │        │         ├──────────────────►│ 如果有压缩
│         │                        │        │         │    running...     │ CompactTask
│         │                        │        │         │                    │
│         │                        │        │         │  (30-60s)         │ k-way merge
│         │                        │        │         │  (取决于文件数)     │ 合并小文件
│         │                        │        │         │                    │
│         │                        │        │         │◄──────────────────┤
│         │                        │        │         │                    │
│         │                        │        │         ▼                    │
│         │                        │        │ ╔════════════════════════╗   │        │
│         │                        │        │ ║ drainIncrement()       ║   │        │
│         │                        │        │ ║ (第340行)              ║   │        │
│         │                        │        │ ╚════════╤═════════════╝   │        │
│         │                        │        │          │                   │        │
│         │                        │        │ ⚠️ 回到 AbstractFileStoreWrite   │        │
│         │                        │        │          │                   │        │
│         │                        │        │          ▼                   │        │
│         │                        │        │ ╔════════════════════════╗   │        │
│         │                        │        │ ║ Writer 清理检查        ║   │        │
│         │                        │        │ ║ (第428-449行)         ║   │        │
│         │                        │        │ ║ if (committable.      ║   │        │
│         │                        │        │ ║     isEmpty() &&      ║   │        │
│         │                        │        │ ║     writerClean       ║   │        │
│         │                        │        │ ║     Checker.apply())  ║   │        │
│         │                        │        │ ║   → writer.close()    ║   │        │
│         │                        │        │ ║                        ║   │        │
│         │                        │        │ ║ writerClean检查中：    ║   │        │
│         │                        │        │ ║ !writer.compact       ║   │        │
│         │                        │        │ ║  NotCompleted()  ⚠️  ║   │        │
│         │                        │        │ ╚════════╤═════════════╝   │        │
│         │                        │        │          │                   │        │
│         │                        │        │          ▼                   │        │
│         │                        │        │ ╔════════════════════════╗   │        │
│         │                        │        │ ║ compactNotCompleted()  ║   │        │
│         │                        │        │ ║ (AppendOnlyWriter      ║   │        │
│         │                        │        │ ║ .344-347)              ║   │        │
│         │                        │        │ ║                        ║   │        │
│         │                        │        │ ║ trigger Compaction    ║   │        │
│         │                        │        │ ║ (false) ⚠️ 再次触发！  ║   │        │
│         │                        │        │ ║                        ║   │        │
│         │                        │        │ ║ return compact         ║   │        │
│         │                        │        │ ║ NotCompleted()         ║   │        │
│         │                        │        │ ╚════════╤═════════════╝   │        │
│         │                        │        │          │                   │        │
│         │                        │        │          │ 可能有压缩在进行    │        │
│         │                        │        │          ├──────────────────►│ 额外压缩
│         │                        │        │          │                    │
│         │                        │        │          ▼                   │        │
│         │                        │        │ ╔════════════════════════╗   │        │
│         │                        │        │ ║ CommitMessageImpl       ║   │        │
│         │                        │        │ ║ 返回                  ║   │        │
│         │                        │        │ ╚════════╤═════════════╝   │        │
│         │                        │        │          │                   │        │
│         │                        │ ⚠️ 继续下一个循环 (P1,B2) 或 (P2,B1)   │        │
│         │                        │        │          │                   │        │
│         │                        │        │   n 次迭代循环 ⚠️ 串行       │        │
│         │                        │        │   每次 30-60s (初始化后)    │        │
│         │                        │        │                            │        │
│         │                        │        └────┬─────────────────────────────────┘
│         │                        │             │                        │        │
│         │                        │             ▼                        │        │
│         │                        │    ╔═══════════════════════╗          │        │
│         │                        │    ║ 返回List<CommitMsg>   ║          │        │
│         │                        │    ║ 所有partition/bucket  ║          │        │
│         │                        │    ║ 的提交消息            ║          │        │
│         │                        │    ╚═════════════╤═════════╝          │        │
│         │                        │                 │                     │        │
│         │◄───────────────────────┴─────────────────┤                     │        │
│    等待所有 write task 完成                        │                     │        │
│                                                    │                     │        │
│    Checkpoint 完成                                │                     │        │
│                                                    │                     │        │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 关键延迟部分详解

### 📍 **延迟点 1：flushWriteBuffer 中的强制等待（MergeTree）**

**代码位置：** `MergeTreeWriter.java` 第 550-603 行

```java
private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
        throws Exception {
    if (writeBuffer.size() > 0) {
        // ⚠️ 第 553-554 行：检查是否需要等待
        if (compactManager.shouldWaitForLatestCompaction()) {  // SortedRun 数 > 4?
            waitForLatestCompaction = true;  // 强制等待！
        }

        // ... 创建文件写入器 ...

        // ⚠️ 第 600 行：同步最新压缩（阻塞！）
        trySyncLatestCompaction(waitForLatestCompaction);
        //  │
        //  └─► compactManager.getCompactionResult(blocking=true)
        //      │
        //      └─► (如果有压缩任务) F.get() 阻塞等待 60-300s

        // 第 602 行：触发新的压缩任务
        compactManager.triggerCompaction(forcedFullCompaction);
    }
}
```

**延迟原因：**
- 当 SortedRun 数 > 4 时，即使用户没有请求等待，也会强制等待
- 恢复时可能有 10+ 个 SortedRun，导致必须等待

**时间成本：** 30-300s（取决于文件数量和大小）

---

### 📍 **延迟点 2：shouldWaitForPreparingCheckpoint 强制等待（MergeTree）**

**代码位置：** `MergeTreeWriter.java` 第 634-639 行

```java
public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
    flushWriteBuffer(waitCompaction, false);  // 第一次等待（可能）

    if (commitForceCompact) {
        waitCompaction = true;
    }

    // ⚠️ 第 634-636 行：再次检查，强制等待
    if (compactManager.shouldWaitForPreparingCheckpoint()) {  // SortedRun 数 > 5?
        waitCompaction = true;  // 强制等待！
    }

    // ⚠️ 第 639 行：第二次同步（再次阻塞！）
    trySyncLatestCompaction(waitCompaction);  // blocking=true
    //  │
    //  └─► compactManager.getCompactionResult(true)
    //      │
    //      └─► (如果仍有压缩) F.get() 再次阻塞 60-300s

    return drainIncrement();
}
```

**延迟原因：**
- 在同一次 prepareCommit 中可能阻塞两次
- 第一次在 flushWriteBuffer，第二次在这里

**时间成本：** 30-600s（两次阻塞的总和）

---

### 📍 **延迟点 3：串行处理 Partition/Bucket（两种表都有）**

**代码位置：** `AbstractFileStoreWrite.java` 第 375-405 行

```java
public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
        throws Exception {
    // ...

    // ⚠️ 第 393-405 行：嵌套循环，串行处理每个 partition/bucket
    Iterator<Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>>> partIter =
            writers.entrySet().iterator();
    while (partIter.hasNext()) {  // ⚠️ 循环 1：所有 partition（例如 7 天）
        Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partEntry =
                partIter.next();
        Iterator<Map.Entry<Integer, WriterContainer<T>>> bucketIter =
                partEntry.getValue().entrySet().iterator();
        while (bucketIter.hasNext()) {  // ⚠️ 循环 2：每个 partition 的 bucket（例如 4 个）
            WriterContainer<T> writerContainer = entry.getValue();

            // ⚠️ 第 405 行：关键！对每个 writer 逐个调用 prepareCommit
            CommitIncrement increment = writerContainer.writer.prepareCommit(waitCompaction);
            //                           └─► MergeTreeWriter.prepareCommit(W)
            //                               或 AppendOnlyWriter.prepareCommit(W)
            //                               ⏸️  可能阻塞 1-300s（取决于压缩）

            // ... 处理结果 ...

            // ⚠️ 第 428-449 行：Writer 清理检查
            if (committable.isEmpty()) {
                if (writerCleanChecker.apply(writerContainer)) {  // ⚠️ 调用检查器
                    //   │
                    //   └─► !writerContainer.writer.compactNotCompleted()
                    //       │
                    //       └─► compactManager.triggerCompaction(false)  ⚠️ 再次触发！

                    writerContainer.writer.close();
                    bucketIter.remove();
                }
            }
        }
    }
    return result;
}
```

**延迟原因：**
- N 个 partition × M 个 bucket 的 prepareCommit 是**串行执行**的
- 每个 writer 的 prepareCommit 可能触发压缩（30-60s）
- 在清理检查中还会再次触发压缩

**时间成本：**
```
正常：7 个 partition × 4 bucket × 200ms = 5.6s
初始化：7 × 4 × 30s = 840s = 14 分钟！
```

---

### 📍 **延迟点 4：compactNotCompleted() 导致的额外压缩**

**代码位置：** `AbstractFileStoreWrite.java` 第 483-485 行

```java
protected static <T> Function<WriterContainer<T>, Boolean>
        createConflictAwareWriterCleanChecker(
                String commitUser, WriteRestore restore) {

    return writerContainer ->
            writerContainer.lastModifiedCommitIdentifier < latestCommittedIdentifier
                    && !writerContainer.writer.compactNotCompleted();  // ⚠️ 关键！
}
```

**详细链路：**

```java
// AppendOnlyWriter.java 第 344-347 行
public boolean compactNotCompleted() {
    compactManager.triggerCompaction(false);  // ⚠️ 触发压缩！
    return compactManager.compactNotCompleted();  // 检查是否还有压缩
}
```

**延迟原因：**
- 在 writer 清理时检查是否有待处理的压缩
- 这会再次触发 triggerCompaction，导致额外的压缩任务
- 每个 writer 都会调用一次

**时间成本：** 每个 writer 额外 20-60s

---

## 4. 恢复时文件堆积的详细流程

```
任务失败重启
  ↓
WriteRestore.restoreFiles(partition, bucket)
  ├─ 读取最新快照
  ├─ 加载所有文件元数据
  └─ 返回 RestoreFiles

  ↓
每个 Partition 的 MergeTreeWriter / AppendOnlyWriter 初始化
  ├─ Levels.update(recoveredFiles)
  ├─ CompactManager.addNewFile(file) for each file
  └─ 重建 LSM Tree 层级结构

  ↓
恢复时的文件状态（假设 2 个 partition，每个 4 bucket）
  ├─ Partition 1, Bucket 1: 10 个 Level-0 文件（未压缩）
  ├─ Partition 1, Bucket 2: 8 个 Level-0 文件
  ├─ Partition 1, Bucket 3: 9 个 Level-0 文件
  ├─ Partition 1, Bucket 4: 7 个 Level-0 文件
  ├─ Partition 2, Bucket 1: 12 个 Level-0 文件（累积多个 ck）
  ├─ Partition 2, Bucket 2: 10 个 Level-0 文件
  ├─ Partition 2, Bucket 3: 11 个 Level-0 文件
  └─ Partition 2, Bucket 4: 9 个 Level-0 文件

  ↓
第一个 Checkpoint：每个 writer 都需要压缩大量文件
  ├─ P1,B1: 合并 10 个文件 → 60s
  ├─ P1,B2: 合并 8 个文件 → 50s
  ├─ P1,B3: 合并 9 个文件 → 55s
  ├─ P1,B4: 合并 7 个文件 → 45s
  ├─ P2,B1: 合并 12 个文件 → 70s
  ├─ P2,B2: 合并 10 个文件 → 60s
  ├─ P2,B3: 合并 11 个文件 → 65s
  └─ P2,B4: 合并 9 个文件 → 55s

  总时间 = 60+50+55+45+70+60+65+55 = 460s ≈ 7.7 分钟

  ↓
后续 Checkpoint 逐渐恢复（压缩完成，文件减少）
  ├─ 第二个 CK：4-5 分钟
  ├─ 第三个 CK：3 分钟
  └─ 第四个 CK：1-2 秒（恢复正常）
```

---

## 5. 性能影响总结表

| 场景 | Partition数 | Bucket数 | 文件堆积 | 单次成本 | 总时间 |
|------|-----------|---------|---------|---------|--------|
| **正常情况** | 7 | 4 | 2个/pb | 200ms | 5.6s |
| **初始化后** | 7 | 4 | 10个/pb | 60s | 1680s |
| **MergeTree强制等待** | 7 | 4 | 10个/pb | 60s×2 | 3360s |
| **Append-Only多pb** | 30 | 8 | 10个/pb | 60s | 14400s |

---

## 6. 关键代码文件对照表

| 延迟点 | 文件 | 行数 | 方法名 | 影响 |
|--------|------|------|--------|------|
| **MergeTree强制等待1** | MergeTreeWriter | 550-603 | flushWriteBuffer | 60-300s |
| **MergeTree强制等待2** | MergeTreeWriter | 634-639 | prepareCommit | 60-300s |
| **串行处理循环** | AbstractFileStoreWrite | 393-405 | prepareCommit | N×M×60s |
| **额外压缩触发** | AbstractFileStoreWrite | 483-485 | createConflictAware... | 20-60s |
| **compactNotCompleted** | AppendOnlyWriter | 344-347 | compactNotCompleted | 20-60s |
| **SortedRun阈值** | MergeTreeCompactManager | 168-184 | shouldWaitForLatest... | 决定是否等待 |
