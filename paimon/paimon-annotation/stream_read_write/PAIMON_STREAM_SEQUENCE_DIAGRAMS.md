# Paimon 流读流写 - PlantUML 时序图

本文档包含 PlantUML 格式的时序图，可在 PlantUML 编辑器中查看。

## 1. 流写初始化时序图

```puml
@startuml StreamWrite_Initialization
participant "Flink Application" as app
participant "StreamWriteBuilder" as builder
participant "Table" as table
participant "StreamTableWrite" as write
participant "TableWriteImpl" as twimpl
participant "FileStoreWrite" as filewrite
participant "RecordWriter" as recwriter

app -> builder: new StreamWriteBuilderImpl(table)
activate builder
builder -> builder: commitUser = UUID.randomUUID()
deactivate builder

app -> builder: withCommitUser("job-123")
activate builder
builder -> builder: this.commitUser = "job-123"
deactivate builder

app -> builder: newWrite()
activate builder
builder -> table: newWrite(commitUser)
activate table
table -> write: create StreamTableWrite
activate write

write -> write: keyAndBucketExtractor = new KeyAndBucketExtractor()
write -> write: recordExtractor = new RecordExtractor()
write -> write: rowKindGenerator = new RowKindGenerator()

write -> filewrite: initialize FileStoreWrite
activate filewrite
filewrite -> filewrite: create BucketedFileStoreWriter
filewrite -> recwriter: initialize RecordWriter for each bucket
activate recwriter
deactivate recwriter
deactivate filewrite

table --> builder: StreamTableWrite
deactivate write
deactivate table
builder --> app: StreamTableWrite
deactivate builder

@enduml
```

## 2. 单行数据写入时序图

```puml
@startuml StreamWrite_SingleRow
participant "Flink" as flink
participant "StreamTableWrite" as write
participant "TableWriteImpl" as twimpl
participant "KeyAndBucketExtractor" as extractor
participant "RecordExtractor" as recext
participant "FileStoreWrite" as filewrite
participant "RecordWriter" as recwriter
participant "Memory Buffer" as buffer

flink -> write: write(row)
activate write

write -> twimpl: write(row)
activate twimpl

twimpl -> extractor: extract(row)
activate extractor
extractor -> extractor: hashCode = hash(primary_key)
extractor -> extractor: bucket = hashCode % numBuckets
extractor -> extractor: partition = extract partition from row
extractor --> twimpl: KeyAndBucketExtraction
deactivate extractor

twimpl -> recext: toRecord(row)
activate recext
recext -> recext: convert to KeyValue or Row
recext --> twimpl: Record<T>
deactivate recext

twimpl -> filewrite: write(partition, bucket, record)
activate filewrite

filewrite -> filewrite: get RecordWriter for (partition, bucket)
filewrite -> recwriter: write(record)
activate recwriter

recwriter -> buffer: append to buffer
activate buffer
buffer -> buffer: check if buffer is full
alt buffer full
    buffer -> buffer: flush to disk
    buffer -> buffer: create temporary file
end
deactivate buffer

recwriter -> filewrite: buffer updated
deactivate recwriter

alt trigger compaction condition
    filewrite -> filewrite: submit compaction task
    filewrite -> filewrite: async task runs in background
end

filewrite --> twimpl: success
deactivate filewrite

twimpl --> write: success
deactivate twimpl

write --> flink: void
deactivate write

@enduml
```

## 3. Checkpoint 提交时序图

```puml
@startuml StreamWrite_Checkpoint
participant "Flink Checkpoint" as flink
participant "StreamTableWrite" as write
participant "TableWriteImpl" as twimpl
participant "FileStoreWrite" as filewrite
participant "StreamTableCommit" as commit
participant "Commit Executor" as executor
participant "SnapshotManager" as snapmgr

flink -> write: checkpoint triggered (checkpointId=1)
activate flink

flink -> write: prepareCommit(waitCompaction=false, commitId=1)
activate write

write -> twimpl: prepareCommit
activate twimpl

twimpl -> filewrite: flush all buffers
activate filewrite
filewrite -> filewrite: flush all RecordWriter buffers
filewrite -> filewrite: write temporary files
deactivate filewrite

alt waitCompaction=true
    twimpl -> filewrite: waitForCompaction()
    activate filewrite
    filewrite -> filewrite: wait for all background tasks
    deactivate filewrite
end

twimpl -> filewrite: getCommitIncrement()
activate filewrite
filewrite -> filewrite: collect all file changes
filewrite -> filewrite: List<CommitMessage> messages
filewrite --> twimpl: CommitMessage[]
deactivate filewrite

twimpl --> write: List<CommitMessage>
deactivate twimpl

write --> flink: CommitMessage[] messages
deactivate write

flink -> executor: submit commit task
activate executor

executor -> commit: commit(messages, checkpointId=1, commitUser="job-123")
activate commit

commit -> snapmgr: getCurrentSnapshot()
activate snapmgr
snapmgr --> commit: Snapshot{snapshotId=1}
deactivate snapmgr

commit -> commit: apply CommitMessage to snapshot
commit -> commit: snapshotId = 2
commit -> commit: commitUser = "job-123"
commit -> commit: commitIdentifier = 1
commit -> snapmgr: persist Snapshot-2
activate snapmgr
snapmgr -> snapmgr: write metadata files
deactivate snapmgr

commit --> executor: success
deactivate commit

executor --> flink: Checkpoint complete
deactivate executor
deactivate flink

@enduml
```

## 4. 流读初始化与恢复时序图

```puml
@startuml StreamRead_Initialize
participant "Flink Application" as app
participant "StreamTableScan" as scan
participant "DataTableStreamScan" as dscan
participant "StartingScanner" as startscan
participant "NextSnapshotFetcher" as fetcher
participant "SnapshotManager" as snapmgr

app -> scan: newStreamScan()
activate scan
scan -> dscan: create DataTableStreamScan()
activate dscan

dscan -> startscan: create StartingScanner by scan-mode
activate startscan
deactivate startscan

dscan -> fetcher: create NextSnapshotFetcher()
activate fetcher
deactivate fetcher

dscan --> scan: initialized
deactivate dscan
scan --> app: StreamTableScan
deactivate scan

alt first time (no checkpoint saved)
    app -> app: no restore state
else restore from checkpoint
    app -> scan: restore(savedNextSnapshotId)
    activate scan
    scan -> dscan: restore(nextSnapshotId)
    activate dscan
    dscan -> dscan: nextSnapshotId = savedNextSnapshotId
    dscan -> dscan: ready to read from this snapshot
    dscan --> scan: restored
    deactivate dscan
    scan --> app: ready to scan
    deactivate scan
end

@enduml
```

## 5. 流读单次扫描循环时序图

```puml
@startuml StreamRead_ScanLoop
participant "Flink Source" as source
participant "StreamTableScan" as scan
participant "DataTableStreamScan" as dscan
participant "StartingScanner" as startscan
participant "FollowUpScanner" as followscan
participant "NextSnapshotFetcher" as fetcher
participant "FileStoreScan" as filescan
participant "SnapshotManager" as snapmgr

loop each checkpoint interval
    source -> scan: plan()
    activate scan

    scan -> dscan: plan()
    activate dscan

    alt first scan (currentSnapshotId == null)
        dscan -> startscan: scan()
        activate startscan

        startscan -> snapmgr: getLatestSnapshot()
        activate snapmgr
        snapmgr --> startscan: Snapshot{id=1}
        deactivate snapmgr

        startscan -> filescan: scan(snapshot=1)
        activate filescan
        filescan -> filescan: read manifest files
        filescan -> filescan: get all data files
        filescan --> startscan: ReadPlan
        deactivate filescan

        startscan -> dscan: ScannedResult{plan, nextId=1}
        deactivate startscan

        dscan -> dscan: currentSnapshotId = 1

    else subsequent scans (currentSnapshotId != null)
        dscan -> followscan: scan(currentSnapshotId)
        activate followscan

        followscan -> fetcher: getNextSnapshot(currentSnapshotId)
        activate fetcher
        fetcher -> snapmgr: findSnapshot(id > currentSnapshotId)
        activate snapmgr
        snapmgr --> fetcher: Snapshot{id=2} or null
        deactivate snapmgr

        alt new snapshot found
            followscan -> filescan: scan(snapshot=2, mode=INCREMENTAL)
            activate filescan
            filescan -> filescan: read delta manifest
            filescan --> followscan: IncrementalReadPlan
            deactivate filescan

            followscan -> dscan: ScannedResult{plan, nextId=2}
            deactivate followscan

            dscan -> dscan: currentSnapshotId = 2

        else no new snapshot
            followscan --> dscan: ScannedResult{plan=null, nextId=null}
            deactivate followscan
        end
    end

    dscan --> scan: ReadPlan
    deactivate dscan

    scan --> source: Plan
    deactivate scan

    alt plan not null
        source -> source: create split readers
        source -> source: read data

        source -> scan: checkpoint()
        activate scan
        scan -> dscan: checkpoint()
        activate dscan
        dscan -> dscan: nextSnapshotId = currentSnapshotId
        dscan --> scan: nextSnapshotId
        deactivate dscan
        scan --> source: nextSnapshotId
        deactivate scan

        source -> source: save to Checkpoint

        source -> scan: notifyCheckpointComplete(nextSnapshotId)
        activate scan
        scan -> dscan: notifyCheckpointComplete(nextSnapshotId)
        activate dscan
        dscan -> dscan: can cleanup old snapshots
        dscan --> scan: success
        deactivate dscan
        scan --> source: success
        deactivate scan

    else plan is null (no new data)
        source -> source: wait for next interval
    end
end

@enduml
```

## 6. 数据读取时序图

```puml
@startuml StreamRead_DataRead
participant "Source" as source
participant "Plan" as plan
participant "TableRead" as tread
participant "AbstractDataTableRead" as dtread
participant "RecordReader" as recread
participant "FileReader" as fread
participant "Data File" as file

source -> plan: splits = plan.splits()
activate plan
plan --> source: List<Split>
deactivate plan

loop for each split
    source -> tread: createReader(split)
    activate tread

    tread -> dtread: new KeyValueTableRead(split)
    activate dtread

    dtread -> recread: create RecordReader
    activate recread

    recread -> fread: initialize file reader
    activate fread
    fread -> file: open data file
    activate file
    deactivate file
    deactivate fread

    loop read records until EOF
        source -> tread: next()
        activate tread

        tread -> dtread: next()
        activate dtread

        dtread -> recread: next()
        activate recread

        recread -> fread: next()
        activate fread

        fread -> file: read next record
        activate file
        file --> fread: data
        deactivate file

        fread --> recread: record
        deactivate fread

        alt predicate filter applied
            dtread -> dtread: evaluate predicate
            alt predicate match
                dtread -> dtread: include record
            else not match
                dtread -> dtread: skip record
            end
        end

        alt column projection applied
            dtread -> dtread: project to required columns
        end

        recread --> dtread: filtered record
        deactivate recread

        dtread --> tread: InternalRow
        deactivate dtread

        tread --> source: InternalRow
        deactivate tread

        source -> source: process row
    end

    tread -> recread: close()
    activate recread
    recread -> fread: close()
    activate fread
    fread -> file: close()
    activate file
    deactivate file
    deactivate fread
    deactivate recread
    deactivate tread
end

@enduml
```

## 7. 故障恢复与重复检测时序图

```puml
@startuml StreamRead_Write_Recovery
participant "Writer" as writer
participant "Reader" as reader
participant "Commit" as commit
participant "Table" as table
participant "SnapshotManager" as snapmgr

Note over writer,table: Normal operation - Checkpoint 1
writer -> writer: write(msg1, msg2, msg3)
writer -> writer: prepareCommit(checkpointId=1)
writer -> commit: commitMessages(msgs, checkpointId=1)
commit -> snapmgr: createSnapshot(id=2, commitId=1)
Note over snapmgr: Snapshot-2 created

reader -> reader: plan() - read Snapshot-1
reader -> reader: checkpoint() → nextSnapshotId=1
Note over reader: Save nextSnapshotId=1

Note over writer,table: Checkpoint 1 confirmed
reader -> reader: notifyCheckpointComplete(1)

Note over writer,table: FAILURE - before Checkpoint 2 confirmation
Note over writer,table: All tasks restart

writer -> writer: restore(savedState)
Note over writer: Recovered state includes commitId=1

reader -> reader: restore(nextSnapshotId=1)
Note over reader: Restored to read from Snapshot-1

Note over writer,table: Recovery processing
writer -> writer: write(msg4, msg5) - new data
writer -> writer: prepareCommit(checkpointId=2)
Note over writer: Messages = [msg1-3 (from saved state) + msg4-5 (new)]

writer -> commit: commitMessages(msgs, checkpointId=2)
activate commit

commit -> snapmgr: getCurrentSnapshot()
activate snapmgr
snapmgr --> commit: Snapshot-2 (from previous Checkpoint-1)
deactivate snapmgr

commit -> commit: Compare messages with Snapshot-2
Note over commit: Detect msg1-3 already in Snapshot-2
Note over commit: Only msg4-5 are new

commit -> commit: Apply only new messages
commit -> snapmgr: createSnapshot(id=3, commitId=2)
Note over snapmgr: Snapshot-3 created (only delta)
deactivate commit

Note over writer,table: Recovery complete
reader -> reader: plan() - read Snapshot-1+2
Note over reader: Next read will get Snapshot-3 delta
reader -> reader: checkpoint() → nextSnapshotId=2

@enduml
```

## 8. 写入端分桶分配时序图

```puml
@startuml StreamWrite_BucketAllocation
participant "Application" as app
participant "StreamTableWrite" as write
participant "TableWriteImpl" as twimpl
participant "KeyAndBucketExtractor" as extractor
participant "FileStoreWrite" as filewrite
participant "BucketedWriter" as bwriter
participant "RecordWriter" as recwriter

app -> write: write(Row{id=101, name="Alice", partition=20240101})
activate write

write -> twimpl: write(row)
activate twimpl

twimpl -> extractor: extract(row)
activate extractor
extractor -> extractor: primaryKey = row.id = 101
extractor -> extractor: bucketId = hash(101) % 16 = 5
extractor -> extractor: partition = row.partition = 20240101
extractor --> twimpl: KeyAndBucket{partition=[20240101], bucket=5}
deactivate extractor

twimpl -> filewrite: write(partition=[20240101], bucket=5, record)
activate filewrite

filewrite -> filewrite: key = (partition=[20240101], bucket=5)
filewrite -> filewrite: check if Writer exists for key
alt Writer exists in cache
    filewrite -> bwriter: get cached BucketedWriter
else Writer not exists
    filewrite -> bwriter: create new BucketedWriter
    activate bwriter
    bwriter -> bwriter: initialize StateDirectory for (partition, bucket)
    bwriter -> recwriter: create RecordWriter
    activate recwriter
    recwriter -> recwriter: allocate memory from MemoryPool
    recwriter -> recwriter: initialize WriteBuffer
    deactivate recwriter
    filewrite -> filewrite: cache BucketedWriter
    deactivate bwriter
end

filewrite -> bwriter: write(record)
activate bwriter
bwriter -> recwriter: write(record)
activate recwriter
recwriter -> recwriter: append to buffer
recwriter -> recwriter: bytesWritten += record.size()
alt buffer full or flush triggered
    recwriter -> recwriter: flush()
    recwriter -> recwriter: write file: data-1-0.parquet
end
deactivate recwriter
deactivate bwriter

filewrite --> twimpl: success
deactivate filewrite

twimpl --> write: success
deactivate twimpl

write --> app: success
deactivate write

app -> write: write(Row{id=205, name="Bob", partition=20240101})
activate write

write -> twimpl: write(row)
activate twimpl

twimpl -> extractor: extract(row)
activate extractor
extractor -> extractor: primaryKey = 205
extractor -> extractor: bucketId = hash(205) % 16 = 3
extractor --> twimpl: KeyAndBucket{partition=[20240101], bucket=3}
deactivate extractor

twimpl -> filewrite: write(partition=[20240101], bucket=3, record)
activate filewrite

filewrite -> filewrite: key = (partition=[20240101], bucket=3)
filewrite -> filewrite: different from previous bucket
filewrite -> bwriter: get or create BucketedWriter for bucket=3
filewrite -> bwriter: write(record)

filewrite --> twimpl: success
deactivate filewrite

twimpl --> write: success
deactivate twimpl

write --> app: success
deactivate write

@enduml
```

## 9. 快照创建与验证时序图

```puml
@startuml Snapshot_Creation
participant "StreamTableCommit" as commit
participant "SnapshotManager" as snapmgr
participant "ManifestList" as mlist
participant "Manifest" as manifest
participant "FileSystem" as fs

commit -> snapmgr: getCurrentSnapshot()
activate snapmgr
snapmgr -> fs: read SNAPSHOT file
fs --> snapmgr: Snapshot{id=1, timestamp=T0}
snapmgr --> commit: Snapshot-1
deactivate snapmgr

commit -> commit: Parse CommitMessage
Note over commit: commitMessage contains:
Note over commit: - new files: [file-2-0.parquet, file-2-1.parquet]
Note over commit: - deleted files: [file-1-0.parquet]

commit -> commit: update Manifest entries
Note over commit: oldManifest entries + new - deleted

commit -> mlist: create ManifestList
activate mlist
mlist -> manifest: create new Manifest with updated entries
activate manifest
manifest -> manifest: serialize entries to Avro
manifest -> fs: write MANIFEST file
fs --> manifest: file path
manifest --> mlist: MANIFEST-2 created
deactivate manifest
mlist --> commit: MANIFEST-LIST
deactivate mlist

commit -> snapmgr: createSnapshot()
activate snapmgr
snapmgr -> snapmgr: Snapshot.Builder builder
snapmgr -> snapmgr: id = previousId + 1 = 2
snapmgr -> snapmgr: timestamp = System.currentTimeMillis()
snapmgr -> snapmgr: commitUser = "job-123"
snapmgr -> snapmgr: commitIdentifier = 1
snapmgr -> snapmgr: schemaId = current schema version
snapmgr -> snapmgr: baseManifestList = MANIFEST-LIST
snapmgr -> snapmgr: deltaManifestList = [MANIFEST-2]
snapmgr -> snapmgr: snapshot = builder.build()

snapmgr -> fs: write SNAPSHOT-2 file
fs --> snapmgr: written

snapmgr -> fs: write SNAPSHOT-2.completed file
fs --> snapmgr: written

snapmgr --> commit: Snapshot-2 created
deactivate snapmgr

commit --> commit: Snapshot-2 is now visible to readers

Note over commit: Reader can now:
Note over commit: 1. Read Snapshot-2 files
Note over commit: 2. See new files added
Note over commit: 3. See deleted files removed

@enduml
```

## 10. 完整端到端时序（简化版）

```puml
@startuml EndToEnd_Simplified
participant "Flink Job" as job
participant "Writer" as writer
participant "Commit" as commit
participant "Reader" as reader
participant "Table Storage" as storage

Note over job: T0 - Start
job -> writer: new StreamWriteBuilder().newWrite()
job -> reader: new StreamTableScan()

Note over job: T1 - Data flowing
job -> writer: write(row1)
job -> writer: write(row2)
job -> writer: write(row3)

Note over job: T2 - Checkpoint 1
job -> writer: prepareCommit(checkpointId=1)
writer -> commit: commitMessages()
commit -> storage: createSnapshot-1
reader -> reader: plan() → scan Snapshot-1

Note over job: T3 - Checkpoint 1 complete
reader -> reader: checkpoint() → nextSnapshotId=1
reader -> reader: process data from Snapshot-1

job -> writer: write(row4)
job -> writer: write(row5)

Note over job: T4 - Checkpoint 2
job -> writer: prepareCommit(checkpointId=2)
writer -> commit: commitMessages()
commit -> storage: createSnapshot-2
reader -> reader: plan() → scan Snapshot-2 (delta)

Note over job: T5 - Checkpoint 2 complete
reader -> reader: checkpoint() → nextSnapshotId=2
reader -> reader: process new data

Note over job: T6 - Failure
Note over job: System crash!

Note over job: T7 - Recovery
job -> writer: restore(state)
job -> reader: restore(nextSnapshotId=1)

Note over job: T8 - Resume
writer -> commit: commitMessages(withRecoveredState)
commit -> storage: createSnapshot-3 (delta)
reader -> reader: scan from Snapshot-2

Note over job: T9 - Normal operation resumes
job -> writer: write(row6)
reader -> reader: process incremental data

@enduml
```

---

## 使用说明

1. 将上述 PlantUML 代码复制到 [PlantUML 在线编辑器](http://www.plantuml.com/plantuml/uml/)
2. 或安装本地 PlantUML 工具生成 PNG/SVG 图像
3. 推荐使用 VS Code 的 PlantUML 插件进行实时预览

## 关键概念速查

### 关键参数
- **commitIdentifier**: Checkpoint ID，用于标识每次提交
- **commitUser**: 应用 ID，用于标识流式应用
- **snapshotId**: 快照 ID，用于追踪表的版本
- **bucket**: 分桶 ID，用于数据分布

### 关键方法
- **prepareCommit(waitCompaction, commitIdentifier)**: 准备提交
- **commit(messages, commitIdentifier)**: 应用提交
- **checkpoint()**: 保存读取进度
- **restore(nextSnapshotId)**: 恢复读取进度
- **notifyCheckpointComplete()**: 清理过期数据

### 故障恢复
1. Writer 保存 state（包含 commitIdentifier）
2. Reader 保存 nextSnapshotId
3. 恢复后，根据 commitIdentifier 过滤重复消息
4. 根据 nextSnapshotId 继续读取

