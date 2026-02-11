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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 使用 POJO 对象实现的 {@link DataFileMeta}。
 *
 * <p>这是 DataFileMeta 接口的标准实现,使用普通的 Java 对象(POJO)来存储数据文件的所有元数据信息。
 *
 * <h2>设计特点</h2>
 * <ul>
 *   <li><b>不可变对象</b>: 所有字段都是 final 的,修改操作返回新实例</li>
 *   <li><b>完整性</b>: 存储了数据文件的所有元数据信息</li>
 *   <li><b>兼容性</b>: 支持多个版本的序列化和反序列化</li>
 *   <li><b>高效性</b>: 字段直接访问,没有额外的计算开销</li>
 * </ul>
 *
 * <h2>字段说明</h2>
 * <ul>
 *   <li><b>基础信息</b>: fileName, fileSize, rowCount</li>
 *   <li><b>键范围</b>: minKey, maxKey, keyStats</li>
 *   <li><b>值统计</b>: valueStats, valueStatsCols</li>
 *   <li><b>序列号</b>: minSequenceNumber, maxSequenceNumber</li>
 *   <li><b>层级</b>: level(LSM 树的层级)</li>
 *   <li><b>Schema</b>: schemaId(Schema 版本标识)</li>
 *   <li><b>时间</b>: creationTime(文件创建时间)</li>
 *   <li><b>删除信息</b>: deleteRowCount(删除行数,支持删除标记)</li>
 *   <li><b>索引</b>: embeddedIndex(嵌入式索引数据)</li>
 *   <li><b>扩展</b>: extraFiles, fileSource, externalPath, firstRowId, writeCols</li>
 * </ul>
 *
 * <h2>行数统计</h2>
 * <p>rowCount 表示文件中的总行数(包括添加和删除的行)。
 * <ul>
 *   <li>rowCount = addRowCount + deleteRowCount</li>
 *   <li>为了保持与旧版本的兼容性,我们不单独存储 addRowCount</li>
 *   <li>deleteRowCount 是可选的,旧版本的文件没有这个字段</li>
 * </ul>
 */
public class PojoDataFileMeta implements DataFileMeta {

    /** 文件名。 */
    private final String fileName;

    /** 文件大小(字节)。 */
    private final long fileSize;

    /** 文件中的总行数(包括添加和删除的行)。 */
    private final long rowCount;

    /** 最小键。 */
    private final BinaryRow minKey;

    /** 最大键。 */
    private final BinaryRow maxKey;

    /** 键统计信息。 */
    private final SimpleStats keyStats;

    /** 值统计信息。 */
    private final SimpleStats valueStats;

    /**
     * 最小序列号。
     *
     * <p>对于行追踪表(row-tracking table),这个值会在提交时被重新分配。
     */
    private final long minSequenceNumber;

    /**
     * 最大序列号。
     *
     * <p>对于行追踪表(row-tracking table),这个值会在提交时被重新分配。
     */
    private final long maxSequenceNumber;

    /** Schema ID,标识数据的 Schema 版本。 */
    private final long schemaId;

    /** LSM 树的层级。 */
    private final int level;

    /** 额外的关联文件列表(如索引文件、统计文件等)。 */
    private final List<String> extraFiles;

    /** 文件创建时间。 */
    private final Timestamp creationTime;

    /**
     * 删除行数(可选)。
     *
     * <p>rowCount = addRowCount + deleteRowCount。
     * 为什么不同时保存 addRowCount 和 deleteRowCount?
     * 因为在早期版本的 DataFileMeta 中,我们只保存 rowCount,
     * 为了保持兼容性,我们继续保存 rowCount,并添加 deleteRowCount 作为可选字段。
     */
    private final @Nullable Long deleteRowCount;

    /**
     * 文件索引过滤器的字节数据(可选)。
     *
     * <p>如果索引数据很小,可以直接嵌入到数据文件元数据中,避免额外的文件读取。
     */
    private final @Nullable byte[] embeddedIndex;

    /** 文件来源(可选),标识文件的生成方式。 */
    private final @Nullable FileSource fileSource;

    /** 值统计列列表(可选),指定哪些列需要收集统计信息。 */
    private final @Nullable List<String> valueStatsCols;

    /**
     * 文件的外部路径(可选)。
     *
     * <p>如果为 null,表示文件位于默认的仓库路径中。
     * 如果不为 null,表示文件位于外部存储系统中(用于外部表)。
     */
    private final @Nullable String externalPath;

    /** 首行 ID(可选),用于行级别的追踪。 */
    private final @Nullable Long firstRowId;

    /** 写入列列表(可选),记录实际写入的列。 */
    private final @Nullable List<String> writeCols;

    /**
     * 构造函数。
     *
     * <p>创建一个新的 PojoDataFileMeta 实例,包含所有可能的元数据字段。
     * 所有字段都是 final 的,创建后不可修改,确保线程安全和不可变性。
     *
     * @param fileName 文件名
     * @param fileSize 文件大小(字节)
     * @param rowCount 行数
     * @param minKey 最小键
     * @param maxKey 最大键
     * @param keyStats 键统计信息
     * @param valueStats 值统计信息
     * @param minSequenceNumber 最小序列号
     * @param maxSequenceNumber 最大序列号
     * @param schemaId Schema ID
     * @param level LSM 树的层级
     * @param extraFiles 额外的关联文件列表
     * @param creationTime 创建时间
     * @param deleteRowCount 删除行数(可选)
     * @param embeddedIndex 嵌入式索引(可选)
     * @param fileSource 文件来源(可选)
     * @param valueStatsCols 值统计列列表(可选)
     * @param externalPath 外部路径(可选)
     * @param firstRowId 首行 ID(可选)
     * @param writeCols 写入列列表(可选)
     */
    public PojoDataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryRow minKey,
            BinaryRow maxKey,
            SimpleStats keyStats,
            SimpleStats valueStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            int level,
            List<String> extraFiles,
            Timestamp creationTime,
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols,
            @Nullable String externalPath,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
        this.fileName = fileName;
        this.fileSize = fileSize;

        this.rowCount = rowCount;

        this.embeddedIndex = embeddedIndex;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStats = keyStats;
        this.valueStats = valueStats;

        this.minSequenceNumber = minSequenceNumber;
        this.maxSequenceNumber = maxSequenceNumber;
        this.level = level;
        this.schemaId = schemaId;
        this.extraFiles = Collections.unmodifiableList(extraFiles); // 创建不可变列表确保线程安全
        this.creationTime = creationTime;

        this.deleteRowCount = deleteRowCount;
        this.fileSource = fileSource;
        this.valueStatsCols = valueStatsCols;
        this.externalPath = externalPath;
        this.firstRowId = firstRowId;
        this.writeCols = writeCols;
    }

    // ==================== Getter 方法实现 ====================
    // 下面的方法都是 DataFileMeta 接口的标准实现,直接返回对应的字段值

    @Override
    public String fileName() {
        return fileName;
    }

    @Override
    public long fileSize() {
        return fileSize;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public Optional<Long> deleteRowCount() {
        return Optional.ofNullable(deleteRowCount);
    }

    @Override
    public byte[] embeddedIndex() {
        return embeddedIndex;
    }

    @Override
    public BinaryRow minKey() {
        return minKey;
    }

    @Override
    public BinaryRow maxKey() {
        return maxKey;
    }

    @Override
    public SimpleStats keyStats() {
        return keyStats;
    }

    @Override
    public SimpleStats valueStats() {
        return valueStats;
    }

    @Override
    public long minSequenceNumber() {
        return minSequenceNumber;
    }

    @Override
    public long maxSequenceNumber() {
        return maxSequenceNumber;
    }

    @Override
    public long schemaId() {
        return schemaId;
    }

    @Override
    public int level() {
        return level;
    }

    @Override
    public List<String> extraFiles() {
        return extraFiles;
    }

    @Override
    public Timestamp creationTime() {
        return creationTime;
    }

    @Override
    public long creationTimeEpochMillis() {
        // 将 Timestamp 转换为系统默认时区的毫秒时间戳
        return creationTime
                .toLocalDateTime()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    @Override
    public String fileFormat() {
        // 从文件名中提取文件格式(扩展名)
        String[] split = fileName.split("\\.");
        if (split.length == 1) {
            throw new RuntimeException("Can't find format from file: " + fileName());
        }
        // 返回最后一个点号后面的扩展名
        return split[split.length - 1];
    }

    @Override
    public Optional<String> externalPath() {
        return Optional.ofNullable(externalPath);
    }

    @Override
    public Optional<String> externalPathDir() {
        return Optional.ofNullable(externalPath).map(Path::new).map(p -> p.getParent().toString());
    }

    @Override
    public Optional<FileSource> fileSource() {
        return Optional.ofNullable(fileSource);
    }

    @Nullable
    public List<String> valueStatsCols() {
        return valueStatsCols;
    }

    @Nullable
    public Long firstRowId() {
        return firstRowId;
    }

    @Nullable
    public List<String> writeCols() {
        return writeCols;
    }

    @Override
    public PojoDataFileMeta upgrade(int newLevel) {
        // 确保新层级大于当前层级(只能向上升级)
        checkArgument(newLevel > this.level);
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                newLevel,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public PojoDataFileMeta rename(String newFileName) {
        // 如果有外部路径,同时更新外部路径的文件名
        String newExternalPath = externalPathDir().map(dir -> dir + "/" + newFileName).orElse(null);
        return new PojoDataFileMeta(
                newFileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                newExternalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public PojoDataFileMeta copyWithoutStats() {
        // 创建不包含详细统计信息的副本,用于减少内存占用
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                EMPTY_STATS, // 清空值统计信息
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                Collections.emptyList(), // 清空值统计列列表
                externalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public PojoDataFileMeta assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber) {
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public PojoDataFileMeta assignFirstRowId(long firstRowId) {
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public PojoDataFileMeta copy(List<String> newExtraFiles) {
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                newExtraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public PojoDataFileMeta newExternalPath(String newExternalPath) {
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                newExternalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public PojoDataFileMeta copy(byte[] newEmbeddedIndex) {
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                newEmbeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public RoaringBitmap32 toFileSelection(List<Range> rowRanges) {
        RoaringBitmap32 selection = null;
        if (rowRanges != null) {
            // 首行 ID 必须存在才能进行行范围转换
            if (firstRowId() == null) {
                throw new IllegalStateException(
                        "firstRowId is null, can't convert to file selection");
            }
            selection = new RoaringBitmap32();
            // 计算文件的行 ID 范围
            long start = firstRowId();
            long end = start + rowCount() - 1;

            Range fileRange = new Range(start, end);

            // 找出期望范围与文件范围的交集
            List<Range> result = new ArrayList<>();
            for (Range expected : rowRanges) {
                Range intersection = Range.intersection(fileRange, expected);
                if (intersection != null) {
                    result.add(intersection);
                }
            }

            // 如果交集恰好等于整个文件范围,返回 null 表示选择整个文件
            if (result.size() == 1 && result.get(0).equals(fileRange)) {
                return null;
            }

            // 将行范围转换为相对于文件起始位置的位图
            for (Range range : result) {
                for (long rowId = range.from; rowId <= range.to; rowId++) {
                    selection.add((int) (rowId - start)); // 相对位置
                }
            }
        }
        return selection;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DataFileMeta)) {
            return false;
        }
        DataFileMeta that = (DataFileMeta) o;
        return Objects.equals(fileName, that.fileName())
                && fileSize == that.fileSize()
                && rowCount == that.rowCount()
                && Arrays.equals(embeddedIndex, that.embeddedIndex())
                && Objects.equals(minKey, that.minKey())
                && Objects.equals(maxKey, that.maxKey())
                && Objects.equals(keyStats, that.keyStats())
                && Objects.equals(valueStats, that.valueStats())
                && minSequenceNumber == that.minSequenceNumber()
                && maxSequenceNumber == that.maxSequenceNumber()
                && schemaId == that.schemaId()
                && level == that.level()
                && Objects.equals(extraFiles, that.extraFiles())
                && Objects.equals(creationTime, that.creationTime())
                && Objects.equals(deleteRowCount, that.deleteRowCount().orElse(null))
                && Objects.equals(fileSource, that.fileSource().orElse(null))
                && Objects.equals(valueStatsCols, that.valueStatsCols())
                && Objects.equals(externalPath, that.externalPath().orElse(null))
                && Objects.equals(firstRowId, that.firstRowId())
                && Objects.equals(writeCols, that.writeCols());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                rowCount,
                Arrays.hashCode(embeddedIndex),
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    @Override
    public String toString() {
        return String.format(
                "{fileName: %s, fileSize: %d, rowCount: %d, embeddedIndex: %s, "
                        + "minKey: %s, maxKey: %s, keyStats: %s, valueStats: %s, "
                        + "minSequenceNumber: %d, maxSequenceNumber: %d, "
                        + "schemaId: %d, level: %d, extraFiles: %s, creationTime: %s, "
                        + "deleteRowCount: %d, fileSource: %s, valueStatsCols: %s, externalPath: %s, firstRowId: %s, writeCols: %s}",
                fileName,
                fileSize,
                rowCount,
                Arrays.toString(embeddedIndex),
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }
}
