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

import static java.lang.Math.min;

/**
 * 比较工具类。
 *
 * <p>提供用于比较谓词字面量值的工具方法。
 * 支持多种数据类型的比较操作,包括实现Comparable接口的类型和字节数组。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>通用比较 - 处理实现Comparable接口的类型
 *   <li>字节数组比较 - 按字典序比较byte[]
 *   <li>类型安全 - 基于DataType进行类型感知的比较
 *   <li>异常处理 - 对不支持的类型抛出异常
 * </ul>
 *
 * <h2>支持的类型</h2>
 * <ul>
 *   <li>基本类型包装类 - Integer, Long, Double, Float等
 *   <li>字符串 - String, BinaryString
 *   <li>时间类型 - Date, Timestamp, LocalDateTime等
 *   <li>数值类型 - Decimal, BigDecimal
 *   <li>字节数组 - byte[]
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 比较整数
 * DataType intType = DataTypes.INT();
 * int result1 = CompareUtils.compareLiteral(intType, 10, 20);
 * // result1 < 0 (10 < 20)
 *
 * // 2. 比较字符串
 * DataType stringType = DataTypes.STRING();
 * BinaryString str1 = BinaryString.fromString("apple");
 * BinaryString str2 = BinaryString.fromString("banana");
 * int result2 = CompareUtils.compareLiteral(stringType, str1, str2);
 * // result2 < 0 ("apple" < "banana")
 *
 * // 3. 比较字节数组
 * DataType binaryType = DataTypes.BINARY(10);
 * byte[] bytes1 = new byte[]{1, 2, 3};
 * byte[] bytes2 = new byte[]{1, 2, 4};
 * int result3 = CompareUtils.compareLiteral(binaryType, bytes1, bytes2);
 * // result3 < 0 (字典序比较)
 *
 * // 4. 比较时间戳
 * DataType timestampType = DataTypes.TIMESTAMP();
 * Timestamp ts1 = Timestamp.fromEpochMillis(1000);
 * Timestamp ts2 = Timestamp.fromEpochMillis(2000);
 * int result4 = CompareUtils.compareLiteral(timestampType, ts1, ts2);
 * // result4 < 0 (ts1 < ts2)
 * }</pre>
 *
 * <h2>返回值说明</h2>
 * <p>比较结果遵循Comparable接口的约定:
 * <ul>
 *   <li>负数 - v1 < v2
 *   <li>零 - v1 == v2
 *   <li>正数 - v1 > v2
 * </ul>
 *
 * <h2>字节数组比较</h2>
 * <p>字节数组按字典序比较:
 * <ul>
 *   <li>逐字节比较 - 从第一个字节开始依次比较
 *   <li>长度处理 - 如果前缀相同,较短的数组被认为较小
 *   <li>无符号比较 - 将字节视为无符号值进行比较
 * </ul>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>谓词评估 - 在比较谓词中比较字段值和字面量
 *   <li>范围检查 - 检查值是否在指定范围内
 *   <li>排序 - 对谓词字面量进行排序
 *   <li>IN谓词优化 - 对IN列表中的值进行排序以优化查找
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>类型检查 - 使用instanceof进行运行时类型检查
 *   <li>泛型擦除 - 需要进行类型转换
 *   <li>字节数组 - 对大型字节数组的比较可能较慢
 * </ul>
 *
 * @see Comparable
 * @see NullFalseLeafBinaryFunction
 * @see LeafPredicate
 */
public class CompareUtils {

    /** 私有构造函数,防止实例化工具类。 */
    private CompareUtils() {}

    /**
     * 比较两个字面量值。
     *
     * <p>根据值的运行时类型选择合适的比较方法:
     * <ul>
     *   <li>如果实现了Comparable接口,使用compareTo方法
     *   <li>如果是byte[],使用字节数组比较
     *   <li>其他类型抛出RuntimeException
     * </ul>
     *
     * @param type 值的数据类型
     * @param v1 第一个值
     * @param v2 第二个值
     * @return 负数(v1 < v2)、零(v1 == v2)或正数(v1 > v2)
     * @throws RuntimeException 如果类型不支持比较
     */
    public static int compareLiteral(DataType type, Object v1, Object v2) {
        if (v1 instanceof Comparable) {
            return ((Comparable<Object>) v1).compareTo(v2);
        } else if (v1 instanceof byte[]) {
            return compare((byte[]) v1, (byte[]) v2);
        } else {
            throw new RuntimeException("Unsupported type: " + type);
        }
    }

    /**
     * 比较两个字节数组。
     *
     * <p>按字典序比较两个字节数组:
     * <ol>
     *   <li>逐字节比较,直到找到不同的字节或到达某个数组的末尾
     *   <li>如果找到不同的字节,返回差值
     *   <li>如果前缀相同,返回长度差值(较短的数组较小)
     * </ol>
     *
     * @param first 第一个字节数组
     * @param second 第二个字节数组
     * @return 负数(first < second)、零(first == second)或正数(first > second)
     */
    private static int compare(byte[] first, byte[] second) {
        for (int x = 0; x < min(first.length, second.length); x++) {
            int cmp = first[x] - second[x];
            if (cmp != 0) {
                return cmp;
            }
        }
        return first.length - second.length;
    }
}
