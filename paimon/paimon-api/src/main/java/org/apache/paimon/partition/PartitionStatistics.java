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

package org.apache.paimon.partition;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * 分区统计信息类。
 *
 * <p>该类封装了分区的统计信息,包括:
 * <ul>
 *   <li>分区规格(spec): 分区键值对
 *   <li>记录数(recordCount): 分区中的记录总数
 *   <li>文件大小(fileSizeInBytes): 分区文件的总字节数
 *   <li>文件数量(fileCount): 分区中的文件总数
 *   <li>最后文件创建时间(lastFileCreationTime): 最新文件的创建时间戳
 *   <li>总桶数(totalBuckets): 分区的总桶数
 * </ul>
 *
 * <p>注意:字段值可能为负数,表示某些数据已被删除(增量统计)。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建分区统计信息
 * Map<String, String> spec = new HashMap<>();
 * spec.put("date", "2024-01-01");
 * spec.put("region", "cn");
 *
 * PartitionStatistics stats = new PartitionStatistics(
 *     spec,
 *     1000L,      // recordCount
 *     10485760L,  // fileSizeInBytes (10MB)
 *     5L,         // fileCount
 *     System.currentTimeMillis(),  // lastFileCreationTime
 *     4           // totalBuckets
 * );
 *
 * // 访问统计信息
 * long records = stats.recordCount();
 * long size = stats.fileSizeInBytes();
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class PartitionStatistics implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 分区规格字段名 */
    public static final String FIELD_SPEC = "spec";
    /** 记录数字段名 */
    public static final String FIELD_RECORD_COUNT = "recordCount";
    /** 文件大小(字节)字段名 */
    public static final String FIELD_FILE_SIZE_IN_BYTES = "fileSizeInBytes";
    /** 文件数量字段名 */
    public static final String FIELD_FILE_COUNT = "fileCount";
    /** 最后文件创建时间字段名 */
    public static final String FIELD_LAST_FILE_CREATION_TIME = "lastFileCreationTime";
    /** 总桶数字段名 */
    public static final String FIELD_TOTAL_BUCKETS = "totalBuckets";

    /** 分区规格,键值对形式 */
    @JsonProperty(FIELD_SPEC)
    protected final Map<String, String> spec;

    /** 记录数,可能为负数表示数据被删除 */
    @JsonProperty(FIELD_RECORD_COUNT)
    protected final long recordCount;

    /** 文件大小(字节),可能为负数表示数据被删除 */
    @JsonProperty(FIELD_FILE_SIZE_IN_BYTES)
    protected final long fileSizeInBytes;

    /** 文件数量,可能为负数表示数据被删除 */
    @JsonProperty(FIELD_FILE_COUNT)
    protected final long fileCount;

    /** 最后文件创建时间戳 */
    @JsonProperty(FIELD_LAST_FILE_CREATION_TIME)
    protected final long lastFileCreationTime;

    /**
     * 总桶数。
     * 如果序列化数据中不存在此字段(例如来自旧版本的 Paimon),则默认为 0。
     */
    @JsonProperty(FIELD_TOTAL_BUCKETS)
    protected final int totalBuckets;

    /**
     * 构造分区统计信息对象。
     *
     * @param spec 分区规格,键值对形式
     * @param recordCount 记录数
     * @param fileSizeInBytes 文件大小(字节)
     * @param fileCount 文件数量
     * @param lastFileCreationTime 最后文件创建时间戳
     * @param totalBuckets 总桶数
     */
    @JsonCreator
    public PartitionStatistics(
            @JsonProperty(FIELD_SPEC) Map<String, String> spec,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime,
            @JsonProperty(FIELD_TOTAL_BUCKETS) int totalBuckets) {
        this.spec = spec;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
        this.totalBuckets = totalBuckets;
    }

    /**
     * 获取分区规格。
     *
     * @return 分区规格 Map
     */
    @JsonGetter(FIELD_SPEC)
    public Map<String, String> spec() {
        return spec;
    }

    /**
     * 获取记录数。
     *
     * @return 记录数,可能为负数
     */
    @JsonGetter(FIELD_RECORD_COUNT)
    public long recordCount() {
        return recordCount;
    }

    /**
     * 获取文件大小(字节)。
     *
     * @return 文件大小,可能为负数
     */
    @JsonGetter(FIELD_FILE_SIZE_IN_BYTES)
    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    /**
     * 获取文件数量。
     *
     * @return 文件数量,可能为负数
     */
    @JsonGetter(FIELD_FILE_COUNT)
    public long fileCount() {
        return fileCount;
    }

    /**
     * 获取最后文件创建时间。
     *
     * @return 最后文件创建时间戳(毫秒)
     */
    @JsonGetter(FIELD_LAST_FILE_CREATION_TIME)
    public long lastFileCreationTime() {
        return lastFileCreationTime;
    }

    /**
     * 获取总桶数。
     *
     * @return 总桶数
     */
    @JsonGetter(FIELD_TOTAL_BUCKETS)
    public int totalBuckets() {
        return totalBuckets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionStatistics that = (PartitionStatistics) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && totalBuckets == that.totalBuckets
                && Objects.equals(spec, that.spec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                spec, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime, totalBuckets);
    }

    @Override
    public String toString() {
        return "{"
                + "spec="
                + spec
                + ", recordCount="
                + recordCount
                + ", fileSizeInBytes="
                + fileSizeInBytes
                + ", fileCount="
                + fileCount
                + ", lastFileCreationTime="
                + lastFileCreationTime
                + ", totalBuckets="
                + totalBuckets
                + '}';
    }
}
