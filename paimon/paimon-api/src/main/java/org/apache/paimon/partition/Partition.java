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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_BY;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_BY;

/**
 * 分区类,表示一个分区,包括统计信息和完成标志。
 *
 * <p>该类继承自 {@link PartitionStatistics},并添加了以下额外信息:
 * <ul>
 *   <li>完成标志(done): 表示分区是否已完成写入
 *   <li>创建时间(createdAt): 分区创建的时间戳
 *   <li>创建者(createdBy): 创建分区的用户/系统
 *   <li>更新时间(updatedAt): 分区最后更新的时间戳
 *   <li>更新者(updatedBy): 最后更新分区的用户/系统
 *   <li>选项(options): 分区的自定义配置选项
 * </ul>
 *
 * <h2>完成标志(Done Flag)</h2>
 * done 标志用于指示分区是否已完成数据写入。这对于流式场景很有用,
 * 可以区分正在写入的分区和已完成的分区。
 *
 * <h2>审计信息</h2>
 * 该类包含审计字段(createdAt、createdBy、updatedAt、updatedBy),
 * 用于跟踪分区的生命周期和变更历史。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建分区
 * Map<String, String> spec = new HashMap<>();
 * spec.put("date", "2024-01-01");
 * spec.put("region", "cn");
 *
 * Partition partition = new Partition(
 *     spec,
 *     1000L,      // recordCount
 *     10485760L,  // fileSizeInBytes
 *     5L,         // fileCount
 *     System.currentTimeMillis(),  // lastFileCreationTime
 *     4,          // totalBuckets
 *     true        // done
 * );
 *
 * // 检查分区是否完成
 * if (partition.done()) {
 *     System.out.println("Partition is complete");
 * }
 *
 * // 访问审计信息
 * Long createdAt = partition.createdAt();
 * String createdBy = partition.createdBy();
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class Partition extends PartitionStatistics {

    private static final long serialVersionUID = 3L;

    /** 完成标志字段名 */
    public static final String FIELD_DONE = "done";
    /** 选项字段名 */
    public static final String FIELD_OPTIONS = "options";

    /** 完成标志,表示分区是否已完成写入 */
    @JsonProperty(FIELD_DONE)
    private final boolean done;

    /** 创建时间戳(毫秒),可能为 null */
    @JsonProperty(FIELD_CREATED_AT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long createdAt;

    /** 创建者标识,可能为 null */
    @JsonProperty(FIELD_CREATED_BY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final String createdBy;

    /** 更新时间戳(毫秒),可能为 null */
    @JsonProperty(FIELD_UPDATED_AT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long updatedAt;

    /** 更新者标识,可能为 null */
    @JsonProperty(FIELD_UPDATED_BY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final String updatedBy;

    /** 分区的自定义配置选项,可能为 null */
    @JsonProperty(FIELD_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Map<String, String> options;

    /**
     * 构造分区对象(完整构造函数)。
     *
     * @param spec 分区规格
     * @param recordCount 记录数
     * @param fileSizeInBytes 文件大小(字节)
     * @param fileCount 文件数量
     * @param lastFileCreationTime 最后文件创建时间
     * @param totalBuckets 总桶数
     * @param done 完成标志
     * @param createdAt 创建时间戳,可以为 null
     * @param createdBy 创建者,可以为 null
     * @param updatedAt 更新时间戳,可以为 null
     * @param updatedBy 更新者,可以为 null
     * @param options 自定义配置选项,可以为 null
     */
    @JsonCreator
    public Partition(
            @JsonProperty(FIELD_SPEC) Map<String, String> spec,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime,
            @JsonProperty(FIELD_TOTAL_BUCKETS) int totalBuckets,
            @JsonProperty(FIELD_DONE) boolean done,
            @JsonProperty(FIELD_CREATED_AT) @Nullable Long createdAt,
            @JsonProperty(FIELD_CREATED_BY) @Nullable String createdBy,
            @JsonProperty(FIELD_UPDATED_AT) @Nullable Long updatedAt,
            @JsonProperty(FIELD_UPDATED_BY) @Nullable String updatedBy,
            @JsonProperty(FIELD_OPTIONS) @Nullable Map<String, String> options) {
        super(spec, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime, totalBuckets);
        this.done = done;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.updatedAt = updatedAt;
        this.updatedBy = updatedBy;
        this.options = options;
    }

    /**
     * 构造分区对象(简化构造函数)。
     *
     * <p>审计信息和选项字段将被设置为 null。
     *
     * @param spec 分区规格
     * @param recordCount 记录数
     * @param fileSizeInBytes 文件大小(字节)
     * @param fileCount 文件数量
     * @param lastFileCreationTime 最后文件创建时间
     * @param totalBuckets 总桶数
     * @param done 完成标志
     */
    public Partition(
            Map<String, String> spec,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime,
            int totalBuckets,
            boolean done) {
        this(
                spec,
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                totalBuckets,
                done,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * 获取完成标志。
     *
     * @return 如果分区已完成写入返回 true,否则返回 false
     */
    @JsonGetter(FIELD_DONE)
    public boolean done() {
        return done;
    }

    /**
     * 获取创建时间戳。
     *
     * @return 创建时间戳(毫秒),可能为 null
     */
    @Nullable
    @JsonGetter(FIELD_CREATED_AT)
    public Long createdAt() {
        return createdAt;
    }

    /**
     * 获取创建者标识。
     *
     * @return 创建者标识,可能为 null
     */
    @Nullable
    @JsonGetter(FIELD_CREATED_BY)
    public String createdBy() {
        return createdBy;
    }

    /**
     * 获取更新时间戳。
     *
     * @return 更新时间戳(毫秒),可能为 null
     */
    @Nullable
    @JsonGetter(FIELD_UPDATED_AT)
    public Long updatedAt() {
        return updatedAt;
    }

    /**
     * 获取更新者标识。
     *
     * @return 更新者标识,可能为 null
     */
    @Nullable
    @JsonGetter(FIELD_UPDATED_BY)
    public String updatedBy() {
        return updatedBy;
    }

    /**
     * 获取自定义配置选项。
     *
     * @return 配置选项 Map,可能为 null
     */
    @Nullable
    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Partition partition = (Partition) o;
        return done == partition.done
                && Objects.equals(createdAt, partition.createdAt)
                && Objects.equals(createdBy, partition.createdBy)
                && Objects.equals(updatedAt, partition.updatedAt)
                && Objects.equals(updatedBy, partition.updatedBy)
                && Objects.equals(options, partition.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(), done, createdAt, createdBy, updatedAt, updatedBy, options);
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
                + ", done="
                + done
                + ", createdAt="
                + createdAt
                + ", createdBy="
                + createdBy
                + ", updatedAt="
                + updatedAt
                + ", updatedBy="
                + updatedBy
                + ", options="
                + options
                + '}';
    }
}
