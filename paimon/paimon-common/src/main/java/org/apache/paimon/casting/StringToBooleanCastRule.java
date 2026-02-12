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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.BinaryStringUtils;

/**
 * 字符串类型到 BOOLEAN 类型的转换规则。
 *
 * <p>支持将字符串类型({@link DataTypeFamily#CHARACTER_STRING})转换为 {@link DataTypeRoot#BOOLEAN}。
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>使用 {@link BinaryStringUtils#toBoolean} 方法执行转换
 *   <li>转换过程不区分大小写
 * </ul>
 *
 * <p>支持的字符串格式:
 *
 * <ul>
 *   <li>"true", "TRUE", "True" → true
 *   <li>"false", "FALSE", "False" → false
 *   <li>"1" → true
 *   <li>"0" → false
 *   <li>"t", "T" → true
 *   <li>"f", "F" → false
 *   <li>"yes", "YES", "Yes" → true
 *   <li>"no", "NO", "No" → false
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 标准布尔字符串
 * "true" → true
 * "false" → false
 * "TRUE" → true
 * "FALSE" → false
 *
 * // 数值字符串
 * "1" → true
 * "0" → false
 *
 * // 简写形式
 * "t" → true
 * "f" → false
 *
 * // 无效格式会抛出异常
 * "yes" → 可能抛出异常(取决于实现)
 * "invalid" → 抛出异常
 * "" → 抛出异常
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>空字符串或无效格式会抛出异常
 *   <li>前导和尾随空格可能需要先被去除
 *   <li>符合 SQL:2016 标准的 CAST 语义
 *   <li>这是一种显式类型转换,通常不会自动发生
 * </ul>
 */
class StringToBooleanCastRule extends AbstractCastRule<BinaryString, Boolean> {

    /** 单例实例 */
    static final StringToBooleanCastRule INSTANCE = new StringToBooleanCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: 字符串类型族
     *   <li>目标类型: BOOLEAN
     * </ul>
     */
    private StringToBooleanCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.BOOLEAN)
                        .build());
    }

    /**
     * 创建字符串到 BOOLEAN 的转换执行器。
     *
     * <p>使用 {@link BinaryStringUtils#toBoolean} 方法解析字符串,该方法会处理多种格式的布尔字符串。
     *
     * @param inputType 输入字符串类型
     * @param targetType 目标 BOOLEAN 类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<BinaryString, Boolean> create(DataType inputType, DataType targetType) {
        return BinaryStringUtils::toBoolean;
    }
}
