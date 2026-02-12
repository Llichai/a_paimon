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

package org.apache.paimon.predicate;

import org.apache.paimon.types.DataType;

import java.util.List;

/**
 * 一元叶子函数,用于测试单个字段值,不需要常量值参数。
 *
 * <p>该抽象类继承自 {@link LeafFunction},专门用于不需要常量值的谓词操作。
 * 典型的一元函数包括 NULL 检查操作,如 IS NULL 和 IS NOT NULL。
 *
 * <h2>一元函数特点</h2>
 * <ul>
 *   <li>不需要常量值列表(literals)参数,只测试字段本身的属性</li>
 *   <li>测试方法签名简化,不包含 literals 参数</li>
 *   <li>主要用于判断字段的状态或属性,而不是与其他值比较</li>
 * </ul>
 *
 * <h2>主要实现类</h2>
 * <ul>
 *   <li>{@link IsNull}: 判断字段值是否为 NULL</li>
 *   <li>{@link IsNotNull}: 判断字段值是否不为 NULL</li>
 * </ul>
 *
 * <h2>方法适配</h2>
 * 该类实现了 {@link LeafFunction} 的通用方法,并将其适配为简化的一元方法:
 * <ul>
 *   <li>{@link #test(DataType, Object, List)} 调用 {@link #test(DataType, Object)}</li>
 *   <li>{@link #test(DataType, long, Object, Object, Long, List)} 调用
 *       {@link #test(DataType, long, Object, Object, Long)}</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // IS NULL 谓词
 * LeafPredicate isNullPredicate = new LeafPredicate(
 *     IsNull.INSTANCE,
 *     DataTypes.STRING(),
 *     0,
 *     "email",
 *     Collections.emptyList()  // 不需要常量值
 * );
 *
 * // IS NOT NULL 谓词
 * LeafPredicate isNotNullPredicate = new LeafPredicate(
 *     IsNotNull.INSTANCE,
 *     DataTypes.INT(),
 *     1,
 *     "age",
 *     Collections.emptyList()  // 不需要常量值
 * );
 * }</pre>
 *
 * @see LeafFunction 叶子函数基类
 * @see IsNull IS NULL 函数实现
 * @see IsNotNull IS NOT NULL 函数实现
 */
public abstract class LeafUnaryFunction extends LeafFunction {

    /** 序列化版本号 */
    private static final long serialVersionUID = 1L;

    /**
     * 基于实际字段值进行精确测试(简化的一元形式)。
     *
     * <p>该方法测试字段值本身的属性,不需要与常量值比较。
     * 例如,IS NULL 函数只需判断 value 是否为 null。
     *
     * @param type 字段的数据类型
     * @param value 字段值(可能为 null)
     * @return 如果字段值满足条件返回 true,否则返回 false
     */
    public abstract boolean test(DataType type, Object value);

    /**
     * 基于统计信息进行快速过滤测试(简化的一元形式)。
     *
     * <p>该方法利用字段的统计信息来判断是否可能存在满足条件的数据。
     * 例如,IS NULL 函数会检查 nullCount 是否大于 0。
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数
     * @param min 字段的最小值(可能为 null)
     * @param max 字段的最大值(可能为 null)
     * @param nullCount 字段的空值计数(可能为 null)
     * @return true 表示可能包含满足条件的数据;false 表示确定不包含满足条件的数据
     */
    public abstract boolean test(
            DataType type, long rowCount, Object min, Object max, Long nullCount);

    /**
     * 基于实际字段值进行精确测试(实现 LeafFunction 接口)。
     *
     * <p>该方法是适配器方法,将带 literals 参数的接口适配为简化的一元方法。
     * literals 参数被忽略,因为一元函数不需要常量值。
     *
     * @param type 字段的数据类型
     * @param value 字段值(可能为 null)
     * @param literals 常量值列表(被忽略)
     * @return 如果字段值满足条件返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object value, List<Object> literals) {
        return test(type, value);
    }

    /**
     * 基于统计信息进行快速过滤测试(实现 LeafFunction 接口)。
     *
     * <p>该方法是适配器方法,将带 literals 参数的接口适配为简化的一元方法。
     * literals 参数被忽略,因为一元函数不需要常量值。
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数
     * @param min 字段的最小值(可能为 null)
     * @param max 字段的最大值(可能为 null)
     * @param nullCount 字段的空值计数(可能为 null)
     * @param literals 常量值列表(被忽略)
     * @return true 表示可能包含满足条件的数据;false 表示确定不包含满足条件的数据
     */
    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        return test(type, rowCount, min, max, nullCount);
    }
}
