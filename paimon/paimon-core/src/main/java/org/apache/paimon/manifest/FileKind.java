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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.Public;

/**
 * 文件类型枚举
 *
 * <p>FileKind 表示 {@link ManifestEntry} 中记录的文件操作类型。
 *
 * <p>两种类型：
 * <ul>
 *   <li>ADD：新增文件 - 在提交时添加的数据文件
 *   <li>DELETE：删除文件 - 在提交时删除的数据文件（逻辑删除）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>提交阶段：{@link FileStoreCommit} 根据文件操作生成 ADD/DELETE 类型的 ManifestEntry
 *   <li>扫描阶段：{@link FileStoreScan} 根据 FileKind 过滤有效文件
 *   <li>过期阶段：{@link SnapshotDeletion} 处理 DELETE 类型的文件
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 新增文件
 * ManifestEntry addEntry = new PojoManifestEntry(
 *     FileKind.ADD,              // 新增
 *     partition,
 *     bucket,
 *     totalBuckets,
 *     dataFileMeta
 * );
 *
 * // 删除文件
 * ManifestEntry deleteEntry = new PojoManifestEntry(
 *     FileKind.DELETE,           // 删除
 *     partition,
 *     bucket,
 *     totalBuckets,
 *     dataFileMeta
 * );
 * }</pre>
 *
 * @since 0.9.0
 */
@Public
public enum FileKind {
    /** 新增文件 - 在提交时添加的数据文件 */
    ADD((byte) 0),

    /** 删除文件 - 在提交时删除的数据文件（逻辑删除） */
    DELETE((byte) 1);

    private final byte value;

    FileKind(byte value) {
        this.value = value;
    }

    /** 转换为字节值，用于序列化 */
    public byte toByteValue() {
        return value;
    }

    /** 从字节值反序列化为 FileKind */
    public static FileKind fromByteValue(byte value) {
        switch (value) {
            case 0:
                return ADD;
            case 1:
                return DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for value kind.");
        }
    }
}
