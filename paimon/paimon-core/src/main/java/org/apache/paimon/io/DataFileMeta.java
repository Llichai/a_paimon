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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * 数据文件的元数据接口 - Paimon 存储引擎的核心类之一
 *
 * <p>这个接口用于描述和管理数据文件的完整元数据信息。每个数据文件都对应一个 DataFileMeta 对象，
 * 包含文件的物理属性、统计信息、键值范围、序列号、索引等关键信息。
 *
 * <p><b>核心功能：</b>
 * <ul>
 *   <li><b>文件标识</b>：文件名、文件大小、存储路径等物理属性
 *   <li><b>数据统计</b>：行数、删除行数、键值统计信息(最小值/最大值/空值数)
 *   <li><b>键范围</b>：最小键和最大键，用于数据过滤和范围查询
 *   <li><b>序列号</b>：最小序列号和最大序列号，用于版本控制和增量读取
 *   <li><b>层级信息</b>：LSM 树的层级，用于合并策略
 *   <li><b>索引信息</b>：嵌入式索引数据，支持快速数据定位
 *   <li><b>扩展文件</b>：额外的关联文件(如 Bloom Filter、列索引等)
 * </ul>
 *
 * <p><b>版本演进历史：</b>
 * <ul>
 *   <li><b>v08</b>：基础版本，包含：
 *       <ul>
 *           <li>文件名、大小、行数
 *           <li>最小键、最大键
 *           <li>键统计、值统计
 *           <li>最小/最大序列号
 *           <li>Schema ID
 *           <li>**层级信息（level）**
 *       </ul>
 *   <li><b>v09</b>：增加：额外文件列表、创建时间
 *   <li><b>v10</b>：增加：删除行数（支持删除标记）
 *   <li><b>v12</b>：增加：嵌入式索引、文件来源
 *   <li><b>v14（FirstRowId）</b>：增加：首行 ID、写入列信息、值统计列信息
 *   <li><b>当前版本</b>：增加：外部路径（支持外部表）
 * </ul>
 *
 * <p><b>重要用途：</b>
 *
 * <ol>
 *   <li><b>文件写入</b>
 *       <ul>
 *           <li>写入器生成文件后创建对应的元数据
 *           <li>存储文件的统计信息供后续使用
 *       </ul>
 *   <li><b>文件读取</b>
 *       <ul>
 *           <li>读取器根据元数据选择和过滤文件
 *           <li>使用键范围（minKey/maxKey）过滤不需要的文件
 *           <li>使用行数和删除行数估算实际数据量
 *       </ul>
 *   <li><b>合并操作</b>
 *       <ul>
 *           <li>合并策略根据元数据选择合并文件
 *           <li>使用文件大小（fileSize）判断是否需要升级
 *           <li>使用序列号确保正确的合并顺序
 *       </ul>
 *   <li><b>快照管理</b>
 *       <ul>
 *           <li>快照通过元数据追踪文件变更
 *           <li>生成 Manifest 文件记录元数据
 *       </ul>
 *   <li><b>数据过期处理</b>
 *       <ul>
 *           <li>根据创建时间过期旧数据
 *           <li>根据序列号判断数据的新旧程度
 *       </ul>
 * </ul>
 *
 * <p><b>使用场景示例：</b>
 * <pre>{@code
 * // 场景 1：压缩策略
 * if (file.fileSize() < minFileSize) {
 *     // 小文件需要合并
 *     candidates.add(file);
 * }
 *
 * // 场景 2：读取优化
 * if (keyComparator.compare(file.maxKey(), queryKey) < 0) {
 *     // 文件的最大键小于查询键，跳过此文件
 *     continue;
 * }
 *
 * // 场景 3：版本控制
 * if (file.minSequenceNumber() > currentSequenceNumber) {
 *     // 文件中的所有数据都是更新的，优先读取
 * }
 *
 * // 场景 4：数据统计
 * long totalRows = files.stream().mapToLong(DataFileMeta::rowCount).sum();
 * long deletedRows = files.stream().mapToLong(DataFileMeta::deleteRowCount).sum();
 * double liveDataRatio = (totalRows - deletedRows) / (double) totalRows;
 * }</pre>
 *
 * <p><b>设计模式：</b>
 * <ul>
 *   <li>接口定义：使用接口而不是具体类，支持多种实现
 *   <li>工厂方法：提供静态工厂方法简化对象创建
 *   <li>不可变对象：所有修改操作返回新实例（通过序列化重新创建）
 *   <li>序列化支持：完整的序列化和反序列化机制用于持久化
 * </ul>
 *
 * <p><b>关键字段（SCHEMA）：</b>
 * <ul>
 *   <li>_FILE_NAME（String）：文件的唯一标识
 *   <li>_FILE_SIZE（Long）：文件大小（字节）
 *   <li>_ROW_COUNT（Long）：文件中的行数
 *   <li>_MIN_KEY / _MAX_KEY（BinaryRow）：键范围，用于范围查询优化
 *   <li>_KEY_STATS / _VALUE_STATS（SimpleStats）：键值统计信息
 *   <li>_MIN_SEQUENCE_NUMBER / _MAX_SEQUENCE_NUMBER（Long）：版本号范围
 *   <li>_SCHEMA_ID（Long）：表的 Schema 版本
 *   <li><b>_LEVEL（Int）：LSM Tree 的层级号，核心字段</b>
 *   <li>_EXTRA_FILES（String[]）：关联的额外文件（如索引、统计）
 *   <li>_CREATION_TIME（Timestamp）：文件创建时间
 *   <li>_DELETE_ROW_COUNT（Long）：删除行数（可选）
 *   <li>其他：嵌入式索引、文件来源等
 * </ul>
 *
 * @since 0.9.0
 * @see Levels LSM Tree 层级管理器，使用 DataFileMeta 管理文件
 * @see CompactStrategy 压缩策略，根据 DataFileMeta 进行决策
 * @see DataFileMeta08Serializer 数据文件元数据的序列化器
 */
@Public
public interface DataFileMeta {

    /**
     * 数据文件元数据的行类型 Schema
     *
     * <p>定义了所有字段的类型和结构，用于序列化和反序列化。
     *
     * <p><b>字段详解（按序列）：</b>
     * <ol>
     *   <li><b>_FILE_NAME (String)</b> - 文件名，索引 0
     *       <ul>
     *           <li>唯一标识文件在存储系统中的位置
     *           <li>通常是相对路径或完整路径
     *           <li>示例：2024-02-13/part-0/file-123.parquet
     *       </ul>
     *   <li><b>_FILE_SIZE (BigInt)</b> - 文件大小，索引 1
     *       <ul>
     *           <li>以字节为单位，不包含索引等辅助数据
     *           <li>用于压缩策略决策（大文件直接升级，小文件需要合并）
     *           <li>影响计算：空间放大、写放大估算
     *       </ul>
     *   <li><b>_ROW_COUNT (BigInt)</b> - 行数，索引 2
     *       <ul>
     *           <li>文件中的逻辑行数（删除记录也计入）
     *           <li>用于数据量估算、查询规划
     *           <li>与 _DELETE_ROW_COUNT 配合计算实际行数
     *       </ul>
     *   <li><b>_MIN_KEY (Bytes)</b> - 最小键，索引 3
     *       <ul>
     *           <li>文件中最小的键（按键比较器）
     *           <li>用于范围查询的快速过滤：if (queryKey < minKey) skip
     *           <li>用于区间分区（IntervalPartition）判断文件是否有重叠
     *       </ul>
     *   <li><b>_MAX_KEY (Bytes)</b> - 最大键，索引 4
     *       <ul>
     *           <li>文件中最大的键（按键比较器）
     *           <li>用于范围查询的快速过滤：if (queryKey > maxKey) skip
     *           <li>用于 Level-1~N 的有序性验证
     *       </ul>
     *   <li><b>_KEY_STATS (SimpleStats)</b> - 键统计信息，索引 5
     *       <ul>
     *           <li>包含键列的统计：最小值、最大值、空值数
     *           <li>用于谓词下推优化
     *       </ul>
     *   <li><b>_VALUE_STATS (SimpleStats)</b> - 值统计信息，索引 6
     *       <ul>
     *           <li>包含值列的统计信息
     *           <li>用于查询优化和数据分析
     *       </ul>
     *   <li><b>_MIN_SEQUENCE_NUMBER (BigInt)</b> - 最小序列号，索引 7
     *       <ul>
     *           <li>文件中最小的序列号
     *           <li>用于版本控制：判断文件中的数据新旧程度
     *           <li>用于增量读取：确定起始序列号
     *       </ul>
     *   <li><b>_MAX_SEQUENCE_NUMBER (BigInt)</b> - 最大序列号，索引 8
     *       <ul>
     *           <li>文件中最大的序列号
     *           <li>Level-0 文件按此字段降序排列（最新数据优先）
     *           <li>用于判断是否应该优先读取此文件
     *       </ul>
     *   <li><b>_SCHEMA_ID (BigInt)</b> - Schema ID，索引 9
     *       <ul>
     *           <li>表的 Schema 版本号
     *           <li>表结构演变时增加
     *           <li>读取文件时需要与当前 Schema 匹配
     *       </ul>
     *   <li><b>_LEVEL (Int)</b> - LSM 层级，索引 10 【核心字段】
     *       <ul>
     *           <li>文件在 LSM Tree 中的层级
     *           <li>0 = Level-0（最新，键可能重叠）
     *           <li>1+ = Level-1~N（较旧，键不重叠）
     *           <li>用于压缩策略判断
     *           <li>用于读取时确定查询顺序
     *       </ul>
     *   <li><b>_EXTRA_FILES (Array&lt;String&gt;)</b> - 额外文件列表，索引 11
     *       <ul>
     *           <li>关联的额外文件，如索引、统计等
     *           <li>示例：[bloom-filter-123, col-stats-456]
     *       </ul>
     *   <li><b>_CREATION_TIME (Timestamp)</b> - 创建时间，索引 12
     *       <ul>
     *           <li>文件创建时的时间戳
     *           <li>用于数据过期策略（TTL）
     *           <li>用于 Level-0 文件的排序（当序列号相同时）
     *       </ul>
     *   <li><b>_DELETE_ROW_COUNT (BigInt)</b> - 删除行数，索引 13【可选】
     *       <ul>
     *           <li>文件中标记为删除的行数
     *           <li>实际行数 = _ROW_COUNT - _DELETE_ROW_COUNT
     *           <li>值为 null 表示此文件没有删除记录
     *           <li>用于评估数据质量和是否需要重写
     *       </ul>
     *   <li><b>_EMBEDDED_FILE_INDEX (Bytes)</b> - 嵌入式索引，索引 14【可选】
     *       <ul>
     *           <li>文件内部的索引数据（如行组索引）
     *           <li>用于快速定位数据块
     *       </ul>
     *   <li><b>_FILE_SOURCE (TinyInt)</b> - 文件来源，索引 15【可选】
     *       <ul>
     *           <li>标识文件的来源：写入、压缩等
     *           <li>类型：FileSource enum
     *       </ul>
     *   <li><b>_VALUE_STATS_COLS (Array&lt;String&gt;)</b> - 值统计列，索引 16【可选】
     *       <ul>
     *           <li>记录了哪些列被统计
     *           <li>用于验证统计信息的有效性
     *       </ul>
     *   <li><b>_EXTERNAL_PATH (String)</b> - 外部路径，索引 17【可选】
     *       <ul>
     *           <li>支持外部表场景（数据存储在 Paimon 外部）
     *           <li>示例：s3://bucket/external-data
     *       </ul>
     *   <li><b>_FIRST_ROW_ID (BigInt)</b> - 首行 ID，索引 18【可选】
     *       <ul>
     *           <li>文件中第一行的 ID（用于变更追踪）
     *           <li>支持 CDC (Change Data Capture) 场景
     *       </ul>
     *   <li><b>_WRITE_COLS (Array&lt;String&gt;)</b> - 写入列，索引 19【可选】
     *       <ul>
     *           <li>记录文件中实际写入的列
     *           <li>支持部分列写入和列演变
     *       </ul>
     * </ol>
     *
     * <p><b>设计说明：</b>
     * <ul>
     *   <li>字段顺序很重要，反序列化时严格按照索引匹配
     *   <li>可选字段（marked as nullable）可以为 null，反序列化时需要处理
     *   <li>字段增加时应该添加到末尾，保持向后兼容性
     *   <li>更新 Schema 时需要对应增加版本号 (v08, v09, ...)
     * </ul>
     */
    RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_FILE_NAME", newStringType(false)), // 文件名
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)), // 文件大小(字节)
                            new DataField(2, "_ROW_COUNT", new BigIntType(false)), // 行数
                            new DataField(3, "_MIN_KEY", newBytesType(false)), // 最小键
                            new DataField(4, "_MAX_KEY", newBytesType(false)), // 最大键
                            new DataField(5, "_KEY_STATS", SimpleStats.SCHEMA), // 键统计信息
                            new DataField(6, "_VALUE_STATS", SimpleStats.SCHEMA), // 值统计信息
                            new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)), // 最小序列号
                            new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)), // 最大序列号
                            new DataField(9, "_SCHEMA_ID", new BigIntType(false)), // Schema ID
                            new DataField(10, "_LEVEL", new IntType(false)), // LSM 层级【核心字段】
                            new DataField(
                                    11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))), // 额外文件列表
                            new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()), // 创建时间
                            new DataField(13, "_DELETE_ROW_COUNT", new BigIntType(true)), // 删除行数(可选)
                            new DataField(14, "_EMBEDDED_FILE_INDEX", newBytesType(true)), // 嵌入式索引(可选)
                            new DataField(15, "_FILE_SOURCE", new TinyIntType(true)), // 文件来源(可选)
                            new DataField(
                                    16,
                                    "_VALUE_STATS_COLS",
                                    DataTypes.ARRAY(DataTypes.STRING().notNull())), // 值统计列(可选)
                            new DataField(17, "_EXTERNAL_PATH", newStringType(true)), // 外部路径(可选)
                            new DataField(18, "_FIRST_ROW_ID", new BigIntType(true)), // 首行 ID(可选)
                            new DataField(
                                    19, "_WRITE_COLS", new ArrayType(true, newStringType(false))))); // 写入列(可选)

    /** 空的最小键,用于 Append 表(无主键表)。 */
    BinaryRow EMPTY_MIN_KEY = EMPTY_ROW;

    /** 空的最大键,用于 Append 表(无主键表)。 */
    BinaryRow EMPTY_MAX_KEY = EMPTY_ROW;

    /** 虚拟层级,用于 Append 表或不使用层级的场景。 */
    int DUMMY_LEVEL = 0;

    /**
     * 为 Append 表创建数据文件元数据。
     *
     * <p>Append 表是只追加数据的表,没有主键,因此不需要键范围和键统计信息。
     * 这个工厂方法简化了 Append 表场景下的元数据创建。
     *
     * @param fileName 文件名
     * @param fileSize 文件大小(字节)
     * @param rowCount 行数
     * @param rowStats 行统计信息(最小值、最大值、空值数等)
     * @param minSequenceNumber 最小序列号
     * @param maxSequenceNumber 最大序列号
     * @param schemaId Schema ID,标识数据的 Schema 版本
     * @param extraFiles 额外的关联文件列表(如索引文件、统计文件等)
     * @param embeddedIndex 嵌入式索引数据(可选),用于快速数据定位
     * @param fileSource 文件来源(可选),标识文件的生成方式
     * @param valueStatsCols 值统计列列表(可选),指定哪些列需要收集统计信息
     * @param externalPath 外部路径(可选),用于外部表
     * @param firstRowId 首行 ID(可选),用于行级别的追踪
     * @param writeCols 写入列列表(可选),记录实际写入的列
     * @return 创建的数据文件元数据对象
     */
    static DataFileMeta forAppend(
            String fileName,
            long fileSize,
            long rowCount,
            SimpleStats rowStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols,
            @Nullable String externalPath,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
        return new PojoDataFileMeta(
                fileName,
                fileSize,
                rowCount,
                EMPTY_MIN_KEY, // Append 表没有主键,使用空键
                EMPTY_MAX_KEY, // Append 表没有主键,使用空键
                EMPTY_STATS, // Append 表没有键统计信息
                rowStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                DUMMY_LEVEL, // Append 表不使用层级
                extraFiles,
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(), // 使用当前时间作为创建时间
                0L, // Append 表没有删除行数
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    /**
     * 创建数据文件元数据(包含额外文件列表)。
     *
     * <p>这是完整版本的工厂方法,包含所有可能的元数据字段。适用于主键表(Primary Key Table)的场景,
     * 支持键范围、键统计、层级等高级特性。创建时间自动设置为当前时间。
     *
     * @param fileName 文件名
     * @param fileSize 文件大小(字节)
     * @param rowCount 行数
     * @param minKey 最小键,用于数据过滤和范围查询
     * @param maxKey 最大键,用于数据过滤和范围查询
     * @param keyStats 键统计信息(最小值、最大值、空值数等)
     * @param valueStats 值统计信息(最小值、最大值、空值数等)
     * @param minSequenceNumber 最小序列号
     * @param maxSequenceNumber 最大序列号
     * @param schemaId Schema ID,标识数据的 Schema 版本
     * @param level LSM 树的层级,0 表示最新层
     * @param extraFiles 额外的关联文件列表(如索引文件、统计文件等)
     * @param deleteRowCount 删除行数(可选),用于统计删除标记的数量
     * @param embeddedIndex 嵌入式索引数据(可选),用于快速数据定位
     * @param fileSource 文件来源(可选),标识文件的生成方式
     * @param valueStatsCols 值统计列列表(可选),指定哪些列需要收集统计信息
     * @param externalPath 外部路径(可选),用于外部表
     * @param firstRowId 首行 ID(可选),用于行级别的追踪
     * @param writeCols 写入列列表(可选),记录实际写入的列
     * @return 创建的数据文件元数据对象
     */
    static DataFileMeta create(
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
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols,
            @Nullable String externalPath,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
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
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(), // 使用当前时间
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    /**
     * 创建数据文件元数据(不包含额外文件列表)。
     *
     * <p>这是简化版本的工厂方法,适用于不需要额外文件的场景。创建时间自动设置为当前时间,
     * 额外文件列表为空,外部路径为 null。
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
     * @param deleteRowCount 删除行数(可选)
     * @param embeddedIndex 嵌入式索引(可选)
     * @param fileSource 文件来源(可选)
     * @param valueStatsCols 值统计列列表(可选)
     * @param firstRowId 首行 ID(可选)
     * @param writeCols 写入列列表(可选)
     * @return 创建的数据文件元数据对象
     */
    static DataFileMeta create(
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
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
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
                Collections.emptyList(), // 不包含额外文件
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(), // 使用当前时间
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                null, // 不设置外部路径
                firstRowId,
                writeCols);
    }

    /**
     * 创建数据文件元数据(指定创建时间)。
     *
     * <p>这是最完整的工厂方法,允许显式指定创建时间。这在从快照恢复或者从其他系统导入数据时非常有用。
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
     * @param creationTime 创建时间(显式指定)
     * @param deleteRowCount 删除行数(可选)
     * @param embeddedIndex 嵌入式索引(可选)
     * @param fileSource 文件来源(可选)
     * @param valueStatsCols 值统计列列表(可选)
     * @param externalPath 外部路径(可选)
     * @param firstRowId 首行 ID(可选)
     * @param writeCols 写入列列表(可选)
     * @return 创建的数据文件元数据对象
     */
    static DataFileMeta create(
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

    /** 获取文件名。 */
    String fileName();

    /** 获取文件大小(字节)。 */
    long fileSize();

    /** 获取行数。 */
    long rowCount();

    /** 获取删除行数(可选),用于统计删除标记的数量。 */
    Optional<Long> deleteRowCount();

    /** 获取嵌入式索引数据(可选),用于快速数据定位。如果没有嵌入式索引,返回空数组。 */
    byte[] embeddedIndex();

    /** 获取最小键,用于数据过滤和范围查询。对于 Append 表返回 EMPTY_ROW。 */
    BinaryRow minKey();

    /** 获取最大键,用于数据过滤和范围查询。对于 Append 表返回 EMPTY_ROW。 */
    BinaryRow maxKey();

    /** 获取键统计信息(最小值、最大值、空值数等)。对于 Append 表返回 EMPTY_STATS。 */
    SimpleStats keyStats();

    /** 获取值统计信息(最小值、最大值、空值数等)。 */
    SimpleStats valueStats();

    /** 获取最小序列号,用于版本控制和增量读取。 */
    long minSequenceNumber();

    /** 获取最大序列号,用于版本控制和增量读取。 */
    long maxSequenceNumber();

    /** 获取 Schema ID,标识数据的 Schema 版本。 */
    long schemaId();

    /** 获取 LSM 树的层级,0 表示最新层。对于 Append 表返回 DUMMY_LEVEL。 */
    int level();

    /** 获取额外的关联文件列表(如索引文件、统计文件等)。 */
    List<String> extraFiles();

    /** 获取创建时间。 */
    Timestamp creationTime();

    /** 获取创建时间的毫秒时间戳。 */
    long creationTimeEpochMillis();

    /** 获取文件格式(从文件名扩展名推断)。 */
    String fileFormat();

    /** 获取外部路径(可选),用于外部表。 */
    Optional<String> externalPath();

    /** 获取外部路径的目录部分(可选),用于外部表。 */
    Optional<String> externalPathDir();

    /** 获取文件来源(可选),标识文件的生成方式。 */
    Optional<FileSource> fileSource();

    /**
     * 获取值统计列列表(可选),指定哪些列需要收集统计信息。
     *
     * <p>如果为 null,表示收集所有列的统计信息。
     */
    @Nullable
    List<String> valueStatsCols();

    /**
     * 获取首行 ID(可选),用于行级别的追踪。
     *
     * <p>首行 ID 是文件中第一行数据的全局唯一标识符,用于支持行级别的追踪和定位。
     */
    @Nullable
    Long firstRowId();

    /**
     * 获取非空的首行 ID。
     *
     * <p>如果首行 ID 为 null,抛出异常。这个方法用于必须使用首行 ID 的场景。
     *
     * @return 首行 ID
     * @throws IllegalArgumentException 如果首行 ID 为 null
     */
    default long nonNullFirstRowId() {
        Long firstRowId = firstRowId();
        checkArgument(firstRowId != null, "First row id of '%s' should not be null.", fileName());
        return firstRowId;
    }

    /**
     * 获取写入列列表(可选),记录实际写入的列。
     *
     * <p>这个信息用于支持部分列更新等高级特性,记录哪些列被实际写入到文件中。
     */
    @Nullable
    List<String> writeCols();

    /**
     * 将文件升级到新的层级。
     *
     * <p>在 LSM 树的合并过程中,文件会从低层级移动到高层级。这个方法创建一个新的元数据对象,
     * 只修改层级字段,其他字段保持不变。
     *
     * @param newLevel 新的层级
     * @return 新的数据文件元数据对象
     */
    DataFileMeta upgrade(int newLevel);

    /**
     * 重命名文件。
     *
     * <p>创建一个新的元数据对象,使用新的文件名,其他字段保持不变。
     * 这个方法用于文件移动、重命名等场景。
     *
     * @param newFileName 新的文件名
     * @return 新的数据文件元数据对象
     */
    DataFileMeta rename(String newFileName);

    /**
     * 创建不包含统计信息的副本。
     *
     * <p>在某些场景下(如文件列表缓存),不需要详细的统计信息,可以使用这个方法创建轻量级的副本,
     * 减少内存占用。键统计和值统计都会被清空。
     *
     * @return 不包含统计信息的新元数据对象
     */
    DataFileMeta copyWithoutStats();

    /**
     * 分配新的序列号范围。
     *
     * <p>在某些场景下需要修改文件的序列号范围,例如在快照恢复、数据迁移等场景。
     * 这个方法创建一个新的元数据对象,使用新的序列号范围,其他字段保持不变。
     *
     * @param minSequenceNumber 新的最小序列号
     * @param maxSequenceNumber 新的最大序列号
     * @return 新的数据文件元数据对象
     */
    DataFileMeta assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber);

    /**
     * 分配首行 ID。
     *
     * <p>为文件分配首行 ID,用于支持行级别的追踪和定位。
     *
     * @param firstRowId 首行 ID
     * @return 新的数据文件元数据对象
     */
    DataFileMeta assignFirstRowId(long firstRowId);

    /**
     * 收集文件及其相关的所有文件路径。
     *
     * <p>除了主数据文件外,还包括所有额外的关联文件(如索引文件、统计文件等)。
     * 这个方法用于文件清理、文件移动等场景。
     *
     * @param pathFactory 文件路径工厂,用于生成完整的文件路径
     * @return 所有文件路径的列表
     */
    default List<Path> collectFiles(DataFilePathFactory pathFactory) {
        List<Path> paths = new ArrayList<>();
        paths.add(pathFactory.toPath(this)); // 主数据文件
        extraFiles().forEach(f -> paths.add(pathFactory.toAlignedPath(f, this))); // 额外文件
        return paths;
    }

    /**
     * 创建包含新额外文件列表的副本。
     *
     * <p>更新额外文件列表,其他字段保持不变。这个方法用于添加或修改关联的索引文件、统计文件等。
     *
     * @param newExtraFiles 新的额外文件列表
     * @return 新的数据文件元数据对象
     */
    DataFileMeta copy(List<String> newExtraFiles);

    /**
     * 设置新的外部路径。
     *
     * <p>为外部表设置或修改外部路径。
     *
     * @param newExternalPath 新的外部路径
     * @return 新的数据文件元数据对象
     */
    DataFileMeta newExternalPath(String newExternalPath);

    /**
     * 创建包含新嵌入式索引的副本。
     *
     * <p>更新嵌入式索引数据,其他字段保持不变。这个方法用于添加或修改文件的嵌入式索引。
     *
     * @param newEmbeddedIndex 新的嵌入式索引数据
     * @return 新的数据文件元数据对象
     */
    DataFileMeta copy(byte[] newEmbeddedIndex);

    /**
     * 根据索引范围创建文件选择位图。
     *
     * <p>将索引范围列表转换为 RoaringBitmap,用于高效的文件选择和过滤。
     * RoaringBitmap 是一种压缩位图,可以高效地表示稀疏的整数集合。
     *
     * @param indices 索引范围列表
     * @return 文件选择位图
     */
    RoaringBitmap32 toFileSelection(List<Range> indices);

    /**
     * 获取文件列表中的最大序列号。
     *
     * <p>这是一个工具方法,用于快速查找一组文件中的最大序列号。
     * 如果文件列表为空,返回 -1。
     *
     * @param fileMetas 文件元数据列表
     * @return 最大序列号,如果列表为空返回 -1
     */
    static long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }
}
