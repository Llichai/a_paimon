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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.apache.paimon.data.Decimal.fromBigDecimal;
import static org.apache.paimon.data.Decimal.fromUnscaledLong;

/**
 * {@link Decimal} 的工具类。
 *
 * <p>提供 Decimal 类型的各种操作,包括:
 * <ul>
 *   <li>算术运算(加法、减法)
 *   <li>类型转换(转整型、转布尔、转 double等)
 *   <li>精度和标度处理
 * </ul>
 */
public class DecimalUtils {

    /** 紧凑表示的最大精度 */
    static final int MAX_COMPACT_PRECISION = 18;

    /** 10的幂次方预计算表,用于快速计算 */
    static final long[] POW10 = new long[MAX_COMPACT_PRECISION + 1];

    static {
        POW10[0] = 1;
        for (int i = 1; i < POW10.length; i++) {
            POW10[i] = 10 * POW10[i - 1];
        }
    }

    /**
     * 将 Decimal 转换为 double 值。
     *
     * @param decimal Decimal 对象
     * @return double 值
     */
    public static double doubleValue(Decimal decimal) {
        if (decimal.isCompact()) {
            return ((double) decimal.toUnscaledLong()) / POW10[decimal.scale()];
        } else {
            return decimal.toBigDecimal().doubleValue();
        }
    }

    /**
     * 两个 Decimal 相加。
     *
     * <p>如果两个值都是紧凑格式且标度相同,使用快速的 long 加法;
     * 否则使用 BigDecimal 加法。
     *
     * @param v1 第一个加数
     * @param v2 第二个加数
     * @param precision 结果的精度
     * @param scale 结果的标度
     * @return 相加结果
     */
    public static Decimal add(Decimal v1, Decimal v2, int precision, int scale) {
        if (v1.isCompact()
                && v2.isCompact()
                && v1.scale() == v2.scale()
                && Decimal.isCompact(precision)) {
            assert scale == v1.scale(); // no need to rescale
            try {
                long ls =
                        Math.addExact(v1.toUnscaledLong(), v2.toUnscaledLong()); // checks overflow
                return Decimal.fromUnscaledLong(ls, precision, scale);
            } catch (ArithmeticException e) {
                // overflow, fall through
            }
        }
        BigDecimal bd = v1.toBigDecimal().add(v2.toBigDecimal());
        return fromBigDecimal(bd, precision, scale);
    }

    /**
     * 两个 Decimal 相减。
     *
     * <p>如果两个值都是紧凑格式且标度相同,使用快速的 long 减法;
     * 否则使用 BigDecimal 减法。
     *
     * @param v1 被减数
     * @param v2 减数
     * @param precision 结果的精度
     * @param scale 结果的标度
     * @return 相减结果
     */
    public static Decimal subtract(Decimal v1, Decimal v2, int precision, int scale) {
        if (v1.isCompact()
                && v2.isCompact()
                && v1.scale() == v2.scale()
                && Decimal.isCompact(precision)) {
            assert scale == v1.scale(); // no need to rescale
            try {
                long ls =
                        Math.subtractExact(
                                v1.toUnscaledLong(), v2.toUnscaledLong()); // checks overflow
                return fromUnscaledLong(ls, precision, scale);
            } catch (ArithmeticException e) {
                // overflow, fall through
            }
        }
        BigDecimal bd = v1.toBigDecimal().subtract(v2.toBigDecimal());
        return fromBigDecimal(bd, precision, scale);
    }

    /**
     * 将 Decimal 转换为整数。
     *
     * <p>向下取整。这与 float=>int 的行为一致,
     * 也与 SQLServer 和 Spark 的行为一致。
     *
     * @param dec Decimal 值
     * @return long 整数值
     */
    public static long castToIntegral(Decimal dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    /**
     * 将 Decimal 转换为指定精度和标度的 Decimal。
     *
     * @param dec 原 Decimal 值
     * @param precision 目标精度
     * @param scale 目标标度
     * @return 转换后的 Decimal
     */
    public static Decimal castToDecimal(Decimal dec, int precision, int scale) {
        return fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    /**
     * 从 Decimal 转换为指定精度和标度的 Decimal。
     *
     * @param dec 原 Decimal 值
     * @param precision 目标精度
     * @param scale 目标标度
     * @return 转换后的 Decimal
     */
    public static Decimal castFrom(Decimal dec, int precision, int scale) {
        return fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    /**
     * 从字符串转换为 Decimal。
     *
     * @param string 字符串
     * @param precision 目标精度
     * @param scale 目标标度
     * @return Decimal 值
     */
    public static Decimal castFrom(String string, int precision, int scale) {
        return fromBigDecimal(new BigDecimal(string), precision, scale);
    }

    /**
     * 从 BinaryString 转换为 Decimal。
     *
     * @param string 二进制字符串
     * @param precision 目标精度
     * @param scale 目标标度
     * @return Decimal 值
     */
    public static Decimal castFrom(BinaryString string, int precision, int scale) {
        return castFrom(string.toString(), precision, scale);
    }

    /**
     * 从 double 转换为 Decimal。
     *
     * @param val double 值
     * @param p 精度
     * @param s 标度
     * @return Decimal 值
     */
    public static Decimal castFrom(double val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    /**
     * 从 long 转换为 Decimal。
     *
     * @param val long 值
     * @param p 精度
     * @param s 标度
     * @return Decimal 值
     */
    public static Decimal castFrom(long val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    /**
     * 将 Decimal 转换为布尔值。
     *
     * @param dec Decimal 值
     * @return 如果不等于0则返回 true
     */
    public static boolean castToBoolean(Decimal dec) {
        return dec.toBigDecimal().compareTo(BigDecimal.ZERO) != 0;
    }
}
