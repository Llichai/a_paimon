/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MergeTree 写入器，负责写入记录并生成 CompactIncrement
 *
 * <p>该类是 Paimon LSM Tree 写入的核心实现，主要职责包括：
 * <ul>
 *   <li>管理写缓冲区（WriteBuffer），将记录写入内存
 *   <li>当缓冲区满时刷新到磁盘，生成 Level-0 文件
 *   <li>管理压缩过程，协调 CompactManager
 *   <li>根据 ChangelogProducer 配置生成 changelog 文件（INPUT 模式）
 *   <li>跟踪新文件、删除文件、changelog 文件等元数据
 * </ul>
 *
 * <p>Changelog 生成模式：
 * <ul>
 *   <li>INPUT：在 {@link #flushWriteBuffer} 时双写 changelog 文件
 *   <li>FULL_COMPACTION/LOOKUP：在压缩时由 CompactRewriter 生成 changelog
 * </ul>
 *
 * <p>文件跟踪机制：
 * <ul>
 *   <li>newFiles：新生成的数据文件（Level-0）
 *   <li>newFilesChangelog：新生成的 changelog 文件（INPUT 模式）
 *   <li>compactBefore/compactAfter：压缩前后的文件
 *   <li>compactChangelog：压缩生成的 changelog 文件（FULL_COMPACTION/LOOKUP 模式）
 * </ul>
 *
 * @see RecordWriter
 * @see MemoryOwner
 */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    // ========== 写缓冲区配置 ==========
    /** 写缓冲区是否可溢出到磁盘 */
    private final boolean writeBufferSpillable;
    /** 磁盘溢出的最大大小 */
    private final MemorySize maxDiskSize;
    /** 排序的最大扇出（merge sort 的 fan-in） */
    private final int sortMaxFan;
    /** 排序时的压缩选项 */
    private final CompressOptions sortCompression;
    /** IO 管理器，用于临时文件管理 */
    private final IOManager ioManager;

    // ========== 数据类型和比较器 ==========
    /** 键类型 */
    private final RowType keyType;
    /** 值类型 */
    private final RowType valueType;
    /** 压缩管理器，负责触发和执行压缩 */
    private final CompactManager compactManager;
    /** 键比较器 */
    private final Comparator<InternalRow> keyComparator;
    /** 合并函数，定义如何合并相同 key 的记录 */
    private final MergeFunction<KeyValue> mergeFunction;
    /** 文件写入器工厂 */
    private final KeyValueFileWriterFactory writerFactory;
    /** 提交时是否强制压缩 */
    private final boolean commitForceCompact;
    /** Changelog 生成策略（INPUT/FULL_COMPACTION/LOOKUP/NONE） */
    private final ChangelogProducer changelogProducer;
    /** 用户自定义序列号比较器 */
    @Nullable private final FieldsComparator userDefinedSeqComparator;

    // ========== 文件跟踪集合 ==========
    /** 新生成的数据文件（刷新 WriteBuffer 产生的 Level-0 文件） */
    private final LinkedHashSet<DataFileMeta> newFiles;
    /** 被删除的文件 */
    private final LinkedHashSet<DataFileMeta> deletedFiles;
    /** 新生成的 changelog 文件（INPUT 模式下刷新时产生） */
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    /** 压缩前的文件（key 是文件名，用于去重） */
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    /** 压缩后的文件 */
    private final LinkedHashSet<DataFileMeta> compactAfter;
    /** 压缩生成的 changelog 文件（FULL_COMPACTION/LOOKUP 模式） */
    private final LinkedHashSet<DataFileMeta> compactChangelog;

    /** 压缩删除文件记录 */
    @Nullable private CompactDeletionFile compactDeletionFile;

    // ========== 运行时状态 ==========
    /** 下一个序列号 */
    private long newSequenceNumber;
    /** 写缓冲区，存储待刷新的记录 */
    private WriteBuffer writeBuffer;

    /**
     * 构造 MergeTreeWriter
     *
     * @param writeBufferSpillable 写缓冲区是否可溢出到磁盘
     * @param maxDiskSize 磁盘溢出的最大大小
     * @param sortMaxFan 排序的最大扇出
     * @param sortCompression 排序时的压缩选项
     * @param ioManager IO 管理器
     * @param compactManager 压缩管理器
     * @param maxSequenceNumber 当前最大序列号
     * @param keyComparator 键比较器
     * @param mergeFunction 合并函数
     * @param writerFactory 文件写入器工厂
     * @param commitForceCompact 提交时是否强制压缩
     * @param changelogProducer Changelog 生成策略
     * @param increment 恢复时的增量信息（可选）
     * @param userDefinedSeqComparator 用户自定义序列号比较器（可选）
     */
    public MergeTreeWriter(
            boolean writeBufferSpillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions sortCompression,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment,
            @Nullable FieldsComparator userDefinedSeqComparator) {
        this.writeBufferSpillable = writeBufferSpillable;
        this.maxDiskSize = maxDiskSize;
        this.sortMaxFan = sortMaxFan;
        this.sortCompression = sortCompression;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;
        this.userDefinedSeqComparator = userDefinedSeqComparator;

        // 初始化文件跟踪集合
        this.newFiles = new LinkedHashSet<>();
        this.deletedFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();

        // 恢复之前的增量信息（用于故障恢复）
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());
            increment
                    .compactIncrement()
                    .compactBefore()
                    .forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());
            updateCompactDeletionFile(increment.compactDeletionFile());
        }
    }

    /**
     * 生成新的序列号
     *
     * <p>序列号单调递增，用于标识记录的写入顺序
     *
     * @return 新的序列号
     */
    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    /**
     * 获取压缩管理器（测试可见）
     *
     * @return 压缩管理器
     */
    @VisibleForTesting
    public CompactManager compactManager() {
        return compactManager;
    }

    /**
     * 设置内存池
     *
     * <p>创建写缓冲区（WriteBuffer），使用提供的内存池管理内存
     *
     * @param memoryPool 内存段池
     */
    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        userDefinedSeqComparator,
                        memoryPool,
                        writeBufferSpillable,
                        maxDiskSize,
                        sortMaxFan,
                        sortCompression,
                        ioManager);
    }

    /**
     * 写入一条 KeyValue 记录
     *
     * <p>记录首先写入写缓冲区（WriteBuffer）：
     * <ul>
     *   <li>如果写入成功，直接返回
     *   <li>如果缓冲区满，先刷新缓冲区，然后重试写入
     *   <li>如果重试仍失败，说明单条记录太大，抛出异常
     * </ul>
     *
     * @param kv 待写入的 KeyValue 记录
     * @throws Exception 写入异常
     */
    @Override
    public void write(KeyValue kv) throws Exception {
        // 分配新的序列号
        long sequenceNumber = newSequenceNumber();
        // 尝试写入缓冲区
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        if (!success) {
            // 缓冲区满，刷新到磁盘
            flushWriteBuffer(false, false);
            // 重试写入
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
                // 单条记录太大，无法写入
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    /**
     * 触发压缩
     *
     * @param fullCompaction 是否全量压缩
     * @throws Exception 压缩异常
     */
    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    /**
     * 添加新文件到压缩管理器
     *
     * @param files 新文件列表
     */
    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    /**
     * 获取所有数据文件
     *
     * @return 数据文件集合
     */
    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    /**
     * 获取当前最大序列号
     *
     * @return 最大序列号
     */
    @Override
    public long maxSequenceNumber() {
        return newSequenceNumber - 1;
    }

    /**
     * 获取内存占用量
     *
     * @return 内存占用字节数
     */
    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    /**
     * 刷新内存数据
     *
     * <p>尝试将写缓冲区的部分数据溢出到磁盘，如果失败则完全刷新缓冲区
     *
     * @throws Exception 刷新异常
     */
    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }

    /**
     * 刷新写缓冲区，将内存中的数据写入文件
     *
     * <p>这是 INPUT 模式生成 changelog 的核心实现位置
     *
     * <p>Changelog 生成策略（基于 changelogProducer 配置）：
     * <ul>
     *   <li>INPUT 模式：双写机制，同时写入数据文件和 changelog 文件
     *   <li>FULL_COMPACTION/LOOKUP 模式：仅写入数据文件，changelog 在压缩时生成
     * </ul>
     *
     * @param waitForLatestCompaction 是否等待最新的压缩完成
     * @param forcedFullCompaction 是否强制全量压缩
     * @throws Exception 写入或压缩过程中的异常
     */
    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        if (writeBuffer.size() > 0) {
            if (compactManager.shouldWaitForLatestCompaction()) {
                waitForLatestCompaction = true;
            }

            // ========== INPUT 模式核心：创建 changelog 写入器 ==========
            // 当 changelogProducer = INPUT 时，创建专门的 changelog 文件写入器
            // 这是 INPUT 模式的关键：在刷新内存表时双写 changelog
            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    changelogProducer == ChangelogProducer.INPUT
                            ? writerFactory.createRollingChangelogFileWriter(0)  // 创建 changelog 写入器
                            : null;
            // 创建数据文件写入器（所有模式都需要）
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);

            try {
                // ========== INPUT 模式核心：双写逻辑 ==========
                // 遍历写缓冲区中的所有记录，执行合并并写入
                // - changelogWriter::write：INPUT 模式下同时写入 changelog 文件
                // - dataWriter::write：写入数据文件（所有模式）
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        changelogWriter == null ? null : changelogWriter::write, // INPUT 模式才写 changelog
                        dataWriter::write);  // 数据文件总是写入
            } finally {
                writeBuffer.clear();
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                dataWriter.close();
            }

            // ========== INPUT 模式核心：收集 changelog 文件元数据 ==========
            // 将生成的 changelog 文件添加到提交增量中
            if (changelogWriter != null) {
                newFilesChangelog.addAll(changelogWriter.result());
            }

            // 收集数据文件元数据并通知压缩管理器
            for (DataFileMeta fileMeta : dataWriter.result()) {
                newFiles.add(fileMeta);
                compactManager.addNewFile(fileMeta);
            }
        }

        // 尝试同步最新的压缩结果
        trySyncLatestCompaction(waitForLatestCompaction);
        // 触发新的压缩任务
        compactManager.triggerCompaction(forcedFullCompaction);
    }

    /**
     * 准备提交
     *
     * <p>执行以下操作：
     * <ul>
     *   <li>刷新写缓冲区，将所有内存数据写入文件
     *   <li>根据配置决定是否等待压缩完成
     *   <li>收集所有增量信息（新文件、changelog 文件等）
     *   <li>返回 CommitIncrement 供上层提交
     * </ul>
     *
     * @param waitCompaction 是否等待压缩完成
     * @return 提交增量信息
     * @throws Exception 准备异常
     */
    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        // 先刷新缓冲区
        flushWriteBuffer(waitCompaction, false);

        // 如果配置了强制压缩，必须等待
        if (commitForceCompact) {
            waitCompaction = true;
        }

        // 再次判断是否需要等待
        // 例如：在重复写入失败的情况下，Level 0 文件可能成功提交，
        // 但在压缩阶段失败并重启，这可能导致 Level 0 文件数量增加。
        // 这个等待可以避免这种情况
        if (compactManager.shouldWaitForPreparingCheckpoint()) {
            waitCompaction = true;
        }

        // 等待压缩完成
        trySyncLatestCompaction(waitCompaction);

        // 收集并清空增量信息
        return drainIncrement();
    }

    /**
     * 检查压缩是否未完成
     *
     * @return 是否有未完成的压缩任务
     */
    @Override
    public boolean compactNotCompleted() {
        compactManager.triggerCompaction(false);
        return compactManager.compactNotCompleted();
    }

    /**
     * 同步等待所有压缩完成
     *
     * @throws Exception 同步异常
     */
    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    /**
     * 收集并清空增量信息
     *
     * <p>将收集的新文件、删除文件、changelog 文件等封装成 CommitIncrement，
     * 并清空本地跟踪集合，准备下一轮提交
     *
     * @return 提交增量信息
     */
    private CommitIncrement drainIncrement() {
        // 构建数据增量（新文件、删除文件、changelog 文件）
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        new ArrayList<>(newFilesChangelog));

        // 构建压缩增量（压缩前文件、压缩后文件、压缩 changelog）
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));

        CompactDeletionFile drainDeletionFile = compactDeletionFile;

        // 清空跟踪集合
        newFiles.clear();
        deletedFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();
        compactDeletionFile = null;

        return new CommitIncrement(dataIncrement, compactIncrement, drainDeletionFile);
    }

    /**
     * 尝试同步最新的压缩结果
     *
     * @param blocking 是否阻塞等待压缩完成
     * @throws Exception 同步异常
     */
    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    /**
     * 更新压缩结果
     *
     * <p>处理压缩完成后的文件管理：
     * <ul>
     *   <li>将压缩前的文件从 compactAfter 移除或添加到 compactBefore
     *   <li>删除不再需要的中间文件
     *   <li>添加压缩后的新文件到 compactAfter
     *   <li>收集压缩生成的 changelog 文件
     * </ul>
     *
     * @param result 压缩结果
     */
    private void updateCompactResult(CompactResult result) {
        // 收集压缩后的文件名集合
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());

        // 处理压缩前的文件
        for (DataFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // 这是一个中间文件（不是新数据文件），压缩后不再需要，可以直接删除
                // 但升级文件（upgrade file）被之前和之后的 snapshot 需要，所以需要确保：
                // 1. 这个文件不是升级操作的输出
                // 2. 这个文件不是升级操作的输入
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file);
                }
            } else {
                // 记录为压缩前的文件
                compactBefore.put(file.fileName(), file);
            }
        }

        // 添加压缩后的文件
        compactAfter.addAll(result.after());
        // 收集压缩生成的 changelog 文件（FULL_COMPACTION/LOOKUP 模式）
        compactChangelog.addAll(result.changelog());

        // 更新删除文件记录
        updateCompactDeletionFile(result.deletionFile());
    }

    /**
     * 更新压缩删除文件记录
     *
     * @param newDeletionFile 新的删除文件记录
     */
    private void updateCompactDeletionFile(@Nullable CompactDeletionFile newDeletionFile) {
        if (newDeletionFile != null) {
            compactDeletionFile =
                    compactDeletionFile == null
                            ? newDeletionFile
                            : newDeletionFile.mergeOldFile(compactDeletionFile);
        }
    }

    /**
     * 关闭写入器
     *
     * <p>执行清理操作：
     * <ul>
     *   <li>取消未完成的压缩任务
     *   <li>同步等待当前压缩完成
     *   <li>删除所有临时文件（未提交的文件）
     *   <li>清理删除文件记录
     * </ul>
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void close() throws Exception {
        // 取消压缩，避免阻塞作业取消
        compactManager.cancelCompaction();
        // 同步等待
        sync();
        // 关闭压缩管理器
        compactManager.close();

        // 删除临时文件
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();
        deletedFiles.clear();

        // 删除 changelog 文件
        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file);
        }
        newFilesChangelog.clear();

        // 删除压缩后的文件（排除升级文件）
        for (DataFileMeta file : compactAfter) {
            // 升级文件被之前的 snapshot 需要，所以需要确保这个文件不是升级操作的输出
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }
        compactAfter.clear();

        // 删除压缩 changelog 文件
        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file);
        }
        compactChangelog.clear();

        // 执行删除
        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file);
        }

        // 清理删除文件记录
        if (compactDeletionFile != null) {
            compactDeletionFile.clean();
        }
    }
}
