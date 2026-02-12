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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * 列举行在变更日志(Changelog)中可以描述的所有变更类型。
 *
 * <p>RowKind 是 Paimon 变更数据捕获(CDC)系统的核心概念,用于标记每一行数据的变更类型。
 * 它支持完整的 CDC 语义,可以准确表示数据的插入、更新和删除操作。
 *
 * <p>设计特点:
 * <ul>
 *     <li><b>完整的 CDC 语义</b>: 支持 INSERT、UPDATE_BEFORE、UPDATE_AFTER、DELETE 四种操作</li>
 *     <li><b>高效的序列化</b>: 每个 RowKind 都有一个 byte 值表示,用于紧凑的序列化</li>
 *     <li><b>短字符串表示</b>: 提供简洁的字符串表示(如 "+I"、"-D"),便于日志和调试</li>
 *     <li><b>稳定的编码</b>: byte 值在不同 JVM 之间保持稳定,确保序列化兼容性</li>
 * </ul>
 *
 * <p>RowKind 的四种类型:
 * <ul>
 *     <li><b>INSERT (+I)</b>: 插入新行</li>
 *     <li><b>UPDATE_BEFORE (-U)</b>: 更新操作中的旧值(撤回)</li>
 *     <li><b>UPDATE_AFTER (+U)</b>: 更新操作中的新值</li>
 *     <li><b>DELETE (-D)</b>: 删除行</li>
 * </ul>
 *
 * <p>UPDATE 操作的两种模式:
 * <ul>
 *     <li><b>非幂等更新</b>: 需要同时发送 UPDATE_BEFORE 和 UPDATE_AFTER,
 *         先撤回旧值,再发送新值。适用于无法通过主键唯一标识的行</li>
 *     <li><b>幂等更新</b>: 只发送 UPDATE_AFTER,依赖主键进行 upsert。
 *         适用于有主键的表,可以减少一半的数据量</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 检查是否是插入或更新后的数据
 * if (rowKind.isAdd()) {
 *     // 处理新数据
 * }
 *
 * // 检查是否是删除或更新前的数据(需要撤回)
 * if (rowKind.isRetract()) {
 *     // 处理撤回数据
 * }
 *
 * // 序列化和反序列化
 * byte value = rowKind.toByteValue();
 * RowKind restored = RowKind.fromByteValue(value);
 * }</pre>
 *
 * <p>注意事项:
 * <p>枚举的 hashCode() 在不同 JVM 之间不稳定,因此序列化时应使用 {@link #toByteValue()}
 * 获取稳定的 byte 值表示,而不是直接序列化枚举对象。
 *
 * @since 0.4.0
 */
@Public
public enum RowKind {

    // 注意: 枚举的 hashCode 在不同 JVM 之间不稳定,序列化时应使用 toByteValue() 方法。

    /** 插入操作,表示新增一行数据。短字符串表示: "+I",byte 值: 0 */
    INSERT("+I", (byte) 0),

    /**
     * 更新操作中的旧值(更新前的内容)。
     *
     * <p>此类型应该与 {@link #UPDATE_AFTER} 一起出现,用于建模需要先撤回旧行的更新操作。
     * 这种方式适用于非幂等更新,即无法通过唯一键标识的行的更新。
     *
     * <p>短字符串表示: "-U",byte 值: 1
     */
    UPDATE_BEFORE("-U", (byte) 1),

    /**
     * 更新操作中的新值(更新后的内容)。
     *
     * <p>此类型可以与 {@link #UPDATE_BEFORE} 一起出现,用于建模需要先撤回旧行的更新操作。
     * 或者,它可以单独描述一个幂等更新,即可以通过唯一键标识的行的更新(upsert 语义)。
     *
     * <p>短字符串表示: "+U",byte 值: 2
     */
    UPDATE_AFTER("+U", (byte) 2),

    /** 删除操作,表示删除一行数据。短字符串表示: "-D",byte 值: 3 */
    DELETE("-D", (byte) 3);

    /** 该 RowKind 的短字符串表示 */
    private final String shortString;

    /** 该 RowKind 的 byte 值表示,用于序列化 */
    private final byte value;

    /**
     * 使用给定的短字符串和 byte 值创建 {@link RowKind} 枚举。
     *
     * @param shortString RowKind 的短字符串表示
     * @param value RowKind 的 byte 值表示
     */
    RowKind(String shortString, byte value) {
        this.shortString = shortString;
        this.value = value;
    }

    /**
     * 返回该 {@link RowKind} 的短字符串表示形式。
     *
     * <p>短字符串格式紧凑,便于在日志、控制台输出和调试信息中使用。
     *
     * <ul>
     *   <li>"+I" 表示 {@link #INSERT}
     *   <li>"-U" 表示 {@link #UPDATE_BEFORE}
     *   <li>"+U" 表示 {@link #UPDATE_AFTER}
     *   <li>"-D" 表示 {@link #DELETE}
     * </ul>
     *
     * <p>短字符串的符号含义:
     * <ul>
     *     <li>"+" 前缀表示添加(INSERT 或 UPDATE_AFTER),数据的正贡献</li>
     *     <li>"-" 前缀表示撤回(UPDATE_BEFORE 或 DELETE),数据的负贡献</li>
     * </ul>
     *
     * @return 短字符串表示
     */
    public String shortString() {
        return shortString;
    }

    /**
     * 返回该 {@link RowKind} 的 byte 值表示形式。
     *
     * <p>byte 值用于序列化和反序列化,提供稳定的跨 JVM 表示。
     * 相比枚举的 ordinal() 或 hashCode(),byte 值保证在不同版本和 JVM 之间保持一致。
     *
     * <p>byte 值映射:
     * <ul>
     *   <li>0 表示 {@link #INSERT}
     *   <li>1 表示 {@link #UPDATE_BEFORE}
     *   <li>2 表示 {@link #UPDATE_AFTER}
     *   <li>3 表示 {@link #DELETE}
     * </ul>
     *
     * @return byte 值表示
     */
    public byte toByteValue() {
        return value;
    }

    /**
     * 判断该 RowKind 是否表示撤回操作。
     *
     * <p>撤回操作包括 {@link #UPDATE_BEFORE} 和 {@link #DELETE},
     * 它们表示数据的"负贡献",需要从结果中减去。
     *
     * <p>使用场景:
     * <ul>
     *     <li>聚合计算: 撤回操作需要减去对应的统计值</li>
     *     <li>去重计算: 撤回操作需要移除重复数据</li>
     *     <li>物化视图: 撤回操作需要更新或删除视图中的数据</li>
     * </ul>
     *
     * @return 如果是 UPDATE_BEFORE 或 DELETE 则返回 true,否则返回 false
     */
    public boolean isRetract() {
        return this == RowKind.UPDATE_BEFORE || this == RowKind.DELETE;
    }

    /**
     * 判断该 RowKind 是否表示添加操作。
     *
     * <p>添加操作包括 {@link #INSERT} 和 {@link #UPDATE_AFTER},
     * 它们表示数据的"正贡献",需要加入到结果中。
     *
     * <p>使用场景:
     * <ul>
     *     <li>聚合计算: 添加操作需要累加对应的统计值</li>
     *     <li>去重计算: 添加操作需要记录新的唯一值</li>
     *     <li>物化视图: 添加操作需要插入或更新视图中的数据</li>
     * </ul>
     *
     * @return 如果是 INSERT 或 UPDATE_AFTER 则返回 true,否则返回 false
     */
    public boolean isAdd() {
        return this == RowKind.INSERT || this == RowKind.UPDATE_AFTER;
    }

    /**
     * 根据给定的 byte 值创建 {@link RowKind}。
     *
     * <p>每个 {@link RowKind} 都有一个 byte 值表示,此方法用于反序列化。
     *
     * @param value byte 值,取值范围 0-3
     * @return 对应的 RowKind 枚举值
     * @throws UnsupportedOperationException 如果 byte 值不在 0-3 范围内
     * @see #toByteValue() 获取 byte 值和 RowKind 的映射关系
     */
    public static RowKind fromByteValue(byte value) {
        switch (value) {
            case 0:
                return INSERT;
            case 1:
                return UPDATE_BEFORE;
            case 2:
                return UPDATE_AFTER;
            case 3:
                return DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for row kind.");
        }
    }

    /**
     * 根据给定的短字符串创建 {@link RowKind}。
     *
     * <p>短字符串解析是大小写不敏感的,"+i" 和 "+I" 都会被识别为 INSERT。
     *
     * @param value 短字符串表示,如 "+I"、"-U"、"+U"、"-D"
     * @return 对应的 RowKind 枚举值
     * @throws UnsupportedOperationException 如果短字符串无法识别
     * @see #shortString() 获取短字符串和 RowKind 的映射关系
     */
    public static RowKind fromShortString(String value) {
        switch (value.toUpperCase()) {
            case "+I":
                return INSERT;
            case "-U":
                return UPDATE_BEFORE;
            case "+U":
                return UPDATE_AFTER;
            case "-D":
                return DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported short string '" + value + "' for row kind.");
        }
    }
}
