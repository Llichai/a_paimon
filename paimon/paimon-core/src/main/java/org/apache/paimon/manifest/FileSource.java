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

/**
 * 文件来源枚举
 *
 * <p>FileSource 表示数据文件的产生来源，用于区分文件是来自新数据还是 Compaction。
 *
 * <p>两种来源：
 * <ul>
 *   <li>APPEND：来自新输入 - 直接写入的新数据文件
 *   <li>COMPACT：来自 Compaction - 合并多个旧文件产生的新文件
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入阶段：{@link RecordWriter} 标记文件来源
 *   <li>Compaction 阶段：{@link MergeTreeWriter} 标记合并产生的文件为 COMPACT
 *   <li>统计分析：用于区分不同来源的文件数量和大小
 * </ul>
 *
 * <p>与 FileKind 的区别：
 * <ul>
 *   <li>FileKind：表示文件操作（ADD/DELETE）
 *   <li>FileSource：表示文件来源（APPEND/COMPACT）
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 新写入的数据文件
 * DataFileMeta appendFile = DataFileMeta.builder()
 *     .fileSource(FileSource.APPEND)    // 来自 Append
 *     .build();
 *
 * // Compaction 产生的文件
 * DataFileMeta compactFile = DataFileMeta.builder()
 *     .fileSource(FileSource.COMPACT)   // 来自 Compaction
 *     .build();
 * }</pre>
 */
public enum FileSource {

    /** 来自新输入 - 直接写入的新数据文件 */
    APPEND((byte) 0),

    /** 来自 Compaction - 合并多个旧文件产生的新文件 */
    COMPACT((byte) 1);

    private final byte value;

    FileSource(byte value) {
        this.value = value;
    }

    /** 转换为字节值，用于序列化 */
    public byte toByteValue() {
        return value;
    }

    /** 从字节值反序列化为 FileSource */
    public static FileSource fromByteValue(byte value) {
        switch (value) {
            case 0:
                return APPEND;
            case 1:
                return COMPACT;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for value kind.");
        }
    }
}
