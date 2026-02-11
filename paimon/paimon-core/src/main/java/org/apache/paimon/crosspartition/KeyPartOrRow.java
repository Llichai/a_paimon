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

package org.apache.paimon.crosspartition;

/**
 * 记录类型枚举
 *
 * <p>标识记录是仅包含主键和分区，还是完整的行数据。
 *
 * <p>使用场景：
 * <ul>
 *   <li>KEY_PART：Bootstrap 阶段的记录，仅包含主键和分区字段
 *   <li>ROW：完整的数据记录，包含所有字段
 * </ul>
 *
 * <p>序列化：
 * <ul>
 *   <li>KEY_PART -> 0
 *   <li>ROW -> 1
 * </ul>
 */
public enum KeyPartOrRow {
    /** 仅包含主键和分区字段 */
    KEY_PART,

    /** 完整的行记录 */
    ROW;

    /**
     * 转换为字节值（用于序列化）
     *
     * @return 字节表示（0 或 1）
     */
    public byte toByteValue() {
        switch (this) {
            case KEY_PART:
                return 0;
            case ROW:
                return 1;
            default:
                throw new UnsupportedOperationException("Unsupported value: " + this);
        }
    }

    /**
     * 从字节值反序列化
     *
     * @param value 字节值
     * @return 对应的枚举值
     * @throws UnsupportedOperationException 如果字节值无效
     */
    public static KeyPartOrRow fromByteValue(byte value) {
        switch (value) {
            case 0:
                return KEY_PART;
            case 1:
                return ROW;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for row kind.");
        }
    }
}
