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

package org.apache.paimon.table;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * 表快照，包含表的基本统计信息。
 *
 * <p>TableSnapshot 封装了一个 Paimon 表在某个时间点的完整状态信息，包括:
 * <ul>
 *   <li>快照元数据 ({@link Snapshot})
 *   <li>记录数统计
 *   <li>文件大小统计
 *   <li>文件数量统计
 *   <li>最后文件创建时间
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * TableSnapshot snapshot = new TableSnapshot(
 *     snapshot,           // 快照对象
 *     1000000L,          // 记录数
 *     1073741824L,       // 文件大小（字节）
 *     100L,              // 文件数量
 *     System.currentTimeMillis()  // 最后文件创建时间
 * );
 *
 * long recordCount = snapshot.recordCount();
 * long fileSize = snapshot.fileSizeInBytes();
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class TableSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_SNAPSHOT = "snapshot";
    public static final String FIELD_RECORD_COUNT = "recordCount";
    public static final String FIELD_FILE_SIZE_IN_BYTES = "fileSizeInBytes";
    public static final String FIELD_FILE_COUNT = "fileCount";
    public static final String FIELD_LAST_FILE_CREATION_TIME = "lastFileCreationTime";

    /** 快照元数据。 */
    @JsonProperty(FIELD_SNAPSHOT)
    private final Snapshot snapshot;

    /** 记录总数。 */
    @JsonProperty(FIELD_RECORD_COUNT)
    private final long recordCount;

    /** 文件总大小（字节）。 */
    @JsonProperty(FIELD_FILE_SIZE_IN_BYTES)
    private final long fileSizeInBytes;

    /** 文件总数。 */
    @JsonProperty(FIELD_FILE_COUNT)
    private final long fileCount;

    /** 最后一个文件的创建时间（毫秒时间戳）。 */
    @JsonProperty(FIELD_LAST_FILE_CREATION_TIME)
    private final long lastFileCreationTime;

    /**
     * 构造 TableSnapshot。
     *
     * @param snapshot 快照元数据
     * @param recordCount 记录总数
     * @param fileSizeInBytes 文件总大小（字节）
     * @param fileCount 文件总数
     * @param lastFileCreationTime 最后一个文件的创建时间
     */
    @JsonCreator
    public TableSnapshot(
            @JsonProperty(FIELD_SNAPSHOT) Snapshot snapshot,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime) {
        this.snapshot = snapshot;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
    }

    /**
     * 获取快照元数据。
     *
     * @return 快照对象
     */
    @JsonGetter(FIELD_SNAPSHOT)
    public Snapshot snapshot() {
        return snapshot;
    }

    /**
     * 获取记录总数。
     *
     * @return 记录数
     */
    @JsonGetter(FIELD_RECORD_COUNT)
    public long recordCount() {
        return recordCount;
    }

    /**
     * 获取文件总大小。
     *
     * @return 文件大小（字节）
     */
    @JsonGetter(FIELD_FILE_SIZE_IN_BYTES)
    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    /**
     * 获取文件总数。
     *
     * @return 文件数量
     */
    @JsonGetter(FIELD_FILE_COUNT)
    public long fileCount() {
        return fileCount;
    }

    /**
     * 获取最后一个文件的创建时间。
     *
     * @return 创建时间（毫秒时间戳）
     */
    @JsonGetter(FIELD_LAST_FILE_CREATION_TIME)
    public long lastFileCreationTime() {
        return lastFileCreationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSnapshot that = (TableSnapshot) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && Objects.equals(snapshot, that.snapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                snapshot, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
    }

    @Override
    public String toString() {
        return "{"
                + "snapshot="
                + snapshot
                + ", recordCount="
                + recordCount
                + ", fileSizeInBytes="
                + fileSizeInBytes
                + ", fileCount="
                + fileCount
                + ", lastFileCreationTime="
                + lastFileCreationTime
                + '}';
    }
}
