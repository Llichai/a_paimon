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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DecimalType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static org.apache.paimon.types.DecimalType.MAX_COMPACT_PRECISION;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * {@link DecimalType} 的内部数据结构实现。
 *
 * <p>此数据结构是不可变的,对于足够小的值,可能以紧凑表示(作为 long 值)存储十进制值,
 * 以提高性能和减少内存占用。
 *
 * <p><b>存储策略:</b>
 * Decimal 采用两种存储格式:
 * <ul>
 *   <li><b>紧凑格式</b>(precision <= {@value DecimalType#MAX_COMPACT_PRECISION}):
 *       使用 long 存储未缩放的值,高效且内存紧凑。
 *       例如: 123.45(precision=5, scale=2) 存储为 long 值 12345
 *   </li>
 *   <li><b>非紧凑格式</b>(precision > {@value DecimalType#MAX_COMPACT_PRECISION}):
 *       使用 {@link BigDecimal} 存储完整精度的值。
 *       用于处理超过 long 范围的大数值
 *   </li>
 * </ul>
 *
 * <p><b>精度和标度:</b>
 * <ul>
 *   <li>精度(precision): 数值的总位数(不包括符号和小数点)</li>
 *   <li>标度(scale): 小数点后的位数</li>
 * </ul>
 * 例如: 123.45 的 precision=5, scale=2
 *
 * <p><b>字段语义:</b>
 * <ul>
 *   <li>{@code precision} 和 {@code scale}: 表示 SQL decimal 类型的精度和标度</li>
 *   <li>{@code decimalVal}: 如果设置,表示完整的十进制值(非紧凑格式)</li>
 *   <li>{@code longVal}: 在紧凑格式中,实际值 = longVal / (10^scale)</li>
 * </ul>
 *
 * <p><b>存储格式选择规则:</b>
 * <pre>
 * if (precision > MAX_COMPACT_PRECISION) {
 *     使用 decimalVal 存储,longVal 未定义
 * } else {
 *     使用 (longVal, scale) 存储,decimalVal 可能被缓存
 * }
 * </pre>
 *
 * <p>注意:(precision, scale) 必须正确设置。此类保证了 Decimal 值的不可变性,
 * 所有算术操作都会创建新的 Decimal 实例。
 *
 * <p>使用场景:
 * <ul>
 *   <li>金融计算:需要精确的十进制运算</li>
 *   <li>货币金额:避免浮点数精度问题</li>
 *   <li>科学计算:需要任意精度的数值</li>
 *   <li>数据库交互:对应 SQL DECIMAL/NUMERIC 类型</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class Decimal implements Comparable<Decimal>, Serializable {

    private static final long serialVersionUID = 1L;

    /** Long 类型能表示的最大十进制位数。(1e18 < Long.MaxValue < 1e19) */
    static final int MAX_LONG_DIGITS = 18;

    /** 预计算的 10 的幂次方数组,用于快速计算。POW10[i] = 10^i */
    static final long[] POW10 = new long[MAX_COMPACT_PRECISION + 1];

    static {
        POW10[0] = 1;
        for (int i = 1; i < POW10.length; i++) {
            POW10[i] = 10 * POW10[i - 1];
        }
    }

    // 字段语义说明:
    //  - `precision` 和 `scale` 表示 SQL decimal 类型的精度和标度
    //  - 如果设置了 `decimalVal`,它表示完整的十进制值
    //  - 否则,十进制值为 longVal/(10^scale)。
    //
    // 注意:(precision, scale) 必须正确。
    // 如果 precision > MAX_COMPACT_PRECISION,
    //   `decimalVal` 表示值,`longVal` 未定义
    // 否则,(longVal, scale) 表示值
    //   `decimalVal` 可能被设置和缓存

    /** 精度:未缩放值的位数。 */
    final int precision;
    /** 标度:小数点后的位数。 */
    final int scale;

    /** 紧凑格式下的长整型值。 */
    final long longVal;
    /** 非紧凑格式下的 BigDecimal 值,也用作紧凑格式的缓存。 */
    BigDecimal decimalVal;

    /**
     * 内部构造函数,不执行任何健全性检查。
     *
     * @param precision 精度
     * @param scale 标度
     * @param longVal 紧凑格式的长整型值
     * @param decimalVal BigDecimal 值
     */
    Decimal(int precision, int scale, long longVal, BigDecimal decimalVal) {
        this.precision = precision;
        this.scale = scale;
        this.longVal = longVal;
        this.decimalVal = decimalVal;
    }

    // ------------------------------------------------------------------------------------------
    // 公共接口
    // ------------------------------------------------------------------------------------------

    /**
     * 返回此 {@link Decimal} 的<i>精度</i>。
     *
     * <p>精度是未缩放值的位数。
     * 例如: 123.45 的精度为 5。
     *
     * @return 精度
     */
    public int precision() {
        return precision;
    }

    /**
     * 返回此 {@link Decimal} 的<i>标度</i>。
     *
     * <p>标度是小数点后的位数。
     * 例如: 123.45 的标度为 2。
     *
     * @return 标度
     */
    public int scale() {
        return scale;
    }

    /** Converts this {@link Decimal} into an instance of {@link BigDecimal}. */
    public BigDecimal toBigDecimal() {
        BigDecimal bd = decimalVal;
        if (bd == null) {
            decimalVal = bd = BigDecimal.valueOf(longVal, scale);
        }
        return bd;
    }

    /**
     * Returns a long describing the <i>unscaled value</i> of this {@link Decimal}.
     *
     * @throws ArithmeticException if this {@link Decimal} does not exactly fit in a long.
     */
    public long toUnscaledLong() {
        if (isCompact()) {
            return longVal;
        } else {
            return toBigDecimal().unscaledValue().longValueExact();
        }
    }

    /**
     * Returns a byte array describing the <i>unscaled value</i> of this {@link Decimal}.
     *
     * @return the unscaled byte array of this {@link Decimal}.
     */
    public byte[] toUnscaledBytes() {
        return toBigDecimal().unscaledValue().toByteArray();
    }

    /** Returns whether the decimal value is small enough to be stored in a long. */
    public boolean isCompact() {
        return precision <= MAX_COMPACT_PRECISION;
    }

    /** Returns a copy of this {@link Decimal} object. */
    public Decimal copy() {
        return new Decimal(precision, scale, longVal, decimalVal);
    }

    @Override
    public int hashCode() {
        return toBigDecimal().hashCode();
    }

    @Override
    public int compareTo(@Nonnull Decimal that) {
        if (this.isCompact() && that.isCompact() && this.scale == that.scale) {
            return Long.compare(this.longVal, that.longVal);
        }
        return this.toBigDecimal().compareTo(that.toBigDecimal());
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Decimal)) {
            return false;
        }
        Decimal that = (Decimal) o;
        return this.compareTo(that) == 0;
    }

    @Override
    public String toString() {
        return toBigDecimal().toPlainString();
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link Decimal} from a {@link BigDecimal} and the given precision and
     * scale.
     *
     * <p>The returned decimal value may be rounded to have the desired scale. The precision will be
     * checked. If the precision overflows, null will be returned.
     */
    public static @Nullable Decimal fromBigDecimal(BigDecimal bd, int precision, int scale) {
        bd = bd.setScale(scale, RoundingMode.HALF_UP);
        if (bd.precision() > precision) {
            return null;
        }

        long longVal = -1;
        if (precision <= MAX_COMPACT_PRECISION) {
            longVal = bd.movePointRight(scale).longValueExact();
        }
        return new Decimal(precision, scale, longVal, bd);
    }

    /**
     * Creates an instance of {@link Decimal} from an unscaled long value and the given precision
     * and scale.
     */
    public static Decimal fromUnscaledLong(long unscaledLong, int precision, int scale) {
        checkArgument(precision > 0 && precision <= MAX_LONG_DIGITS);
        return new Decimal(precision, scale, unscaledLong, null);
    }

    /**
     * Creates an instance of {@link Decimal} from an unscaled byte array value and the given
     * precision and scale.
     */
    public static Decimal fromUnscaledBytes(byte[] unscaledBytes, int precision, int scale) {
        BigDecimal bd = new BigDecimal(new BigInteger(unscaledBytes), scale);
        return fromBigDecimal(bd, precision, scale);
    }

    /**
     * Creates an instance of {@link Decimal} for a zero value with the given precision and scale.
     *
     * <p>The precision will be checked. If the precision overflows, null will be returned.
     */
    public static @Nullable Decimal zero(int precision, int scale) {
        if (precision <= MAX_COMPACT_PRECISION) {
            return new Decimal(precision, scale, 0, null);
        } else {
            return fromBigDecimal(BigDecimal.ZERO, precision, scale);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    /** Returns whether the decimal value is small enough to be stored in a long. */
    public static boolean isCompact(int precision) {
        return precision <= MAX_COMPACT_PRECISION;
    }
}
