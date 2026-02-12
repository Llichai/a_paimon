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

package org.apache.paimon.sst;

/**
 * 块对齐类型枚举。
 *
 * <p>定义 SST 文件中数据块的对齐方式:
 * <ul>
 *   <li>{@link #ALIGNED} - 对齐的块,按固定边界对齐以提高读取效率
 *   <li>{@link #UNALIGNED} - 非对齐的块,紧凑存储以节省空间
 * </ul>
 */
public enum BlockAlignedType {
    /** 对齐的块 */
    ALIGNED((byte) 0),

    /** 非对齐的块 */
    UNALIGNED((byte) 1);

    /** 类型的字节表示 */
    private final byte b;

    BlockAlignedType(byte b) {
        this.b = b;
    }

    /**
     * 转换为字节表示。
     *
     * @return 字节值
     */
    public byte toByte() {
        return b;
    }

    /**
     * 从字节值创建对齐类型。
     *
     * @param b 字节值
     * @return 对应的对齐类型
     * @throws IllegalStateException 如果字节值非法
     */
    public static BlockAlignedType fromByte(byte b) {
        for (BlockAlignedType type : BlockAlignedType.values()) {
            if (type.toByte() == b) {
                return type;
            }
        }
        throw new IllegalStateException("Illegal block aligned type: " + b);
    }
}
