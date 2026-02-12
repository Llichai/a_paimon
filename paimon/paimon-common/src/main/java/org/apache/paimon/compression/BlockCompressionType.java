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

package org.apache.paimon.compression;

/**
 * 块压缩类型枚举。
 *
 * <p>定义了 Paimon 支持的所有块压缩算法类型。每种压缩类型都有一个持久化 ID,
 * 用于在存储中标识压缩算法。
 *
 * <p>支持的压缩算法:
 * <ul>
 *   <li>NONE(0) - 不压缩</li>
 *   <li>ZSTD(1) - Zstandard 压缩,提供高压缩率</li>
 *   <li>LZ4(2) - LZ4 压缩,提供高压缩速度</li>
 *   <li>LZO(3) - LZO 压缩,平衡压缩率和速度</li>
 * </ul>
 */
public enum BlockCompressionType {
    /** 不压缩 */
    NONE(0),
    /** Zstandard 压缩算法 */
    ZSTD(1),
    /** LZ4 压缩算法 */
    LZ4(2),
    /** LZO 压缩算法 */
    LZO(3);

    /** 持久化 ID,用于在存储中标识压缩类型 */
    private final int persistentId;

    BlockCompressionType(int persistentId) {
        this.persistentId = persistentId;
    }

    /**
     * 获取持久化 ID。
     *
     * @return 压缩类型的持久化 ID
     */
    public int persistentId() {
        return this.persistentId;
    }

    /**
     * 根据持久化 ID 获取压缩类型。
     *
     * @param persistentId 持久化 ID
     * @return 对应的压缩类型
     * @throws IllegalArgumentException 如果持久化 ID 未知
     */
    public static BlockCompressionType getCompressionTypeByPersistentId(int persistentId) {
        BlockCompressionType[] types = values();
        for (BlockCompressionType type : types) {
            if (type.persistentId == persistentId) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown persistentId " + persistentId);
    }
}
