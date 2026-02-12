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

import java.util.Objects;

/**
 * 固定精度和小数位数的十进制数数据类型。
 *
 * <p>该类型对应 SQL 标准中的 DECIMAL/NUMERIC 类型,用于精确存储小数值,
 * 常用于金融计算、货币金额等需要精确运算的场景。
 *
 * <p><b>精度(Precision):</b> 表示数字的总位数,范围是 1-38。
 * <p><b>小数位(Scale):</b> 表示小数点后的位数,范围是 0-精度值。
 *
 * <p>例如: DECIMAL(10, 2) 可以存储最大值 99999999.99,总共10位数字,其中2位小数。
 *
 * <p><b>存储优化:</b> 当精度 <= 18 时,使用紧凑模式(8字节的long类型存储);
 * 当精度 > 18 时,使用完整模式(16字节存储)。
 *
 * @since 0.4.0
 */
@Public
public class DecimalType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 紧凑存储模式的最大精度,该精度范围内可使用 long 类型存储。 */
    public static final int MAX_COMPACT_PRECISION = 18;

    /** 最小精度值。 */
    public static final int MIN_PRECISION = 1;

    /** 最大精度值,支持最多 38 位数字。 */
    public static final int MAX_PRECISION = 38;

    /** 默认精度值。 */
    public static final int DEFAULT_PRECISION = 10;

    /** 最小小数位数值。 */
    public static final int MIN_SCALE = 0;

    /** 默认小数位数值。 */
    public static final int DEFAULT_SCALE = 0;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "DECIMAL(%d, %d)";

    /** 精度,表示总位数。 */
    private final int precision;

    /** 小数位数,表示小数点后的位数。 */
    private final int scale;

    /**
     * 构造一个 Decimal 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param precision 精度(总位数),范围 1-38
     * @param scale 小数位数(小数点后的位数),范围 0-precision
     * @throws IllegalArgumentException 如果精度或小数位数超出有效范围
     */
    public DecimalType(boolean isNullable, int precision, int scale) {
        super(isNullable, DataTypeRoot.DECIMAL);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Decimal precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        if (scale < MIN_SCALE || scale > precision) {
            throw new IllegalArgumentException(
                    String.format(
                            "Decimal scale must be between %d and the precision %d (both inclusive).",
                            MIN_SCALE, precision));
        }
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * 构造一个默认允许 null 的 Decimal 类型实例。
     *
     * @param precision 精度(总位数)
     * @param scale 小数位数(小数点后的位数)
     */
    public DecimalType(int precision, int scale) {
        this(true, precision, scale);
    }

    /**
     * 构造一个指定精度、默认小数位数(0)的 Decimal 类型实例。
     *
     * @param precision 精度(总位数)
     */
    public DecimalType(int precision) {
        this(precision, DEFAULT_SCALE);
    }

    /**
     * 构造一个默认精度(10)和默认小数位数(0)的 Decimal 类型实例。
     */
    public DecimalType() {
        this(DEFAULT_PRECISION);
    }

    /**
     * 获取精度(总位数)。
     *
     * @return 精度值
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * 获取小数位数(小数点后的位数)。
     *
     * @return 小数位数值
     */
    public int getScale() {
        return scale;
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * <p>精度 <= 18 时返回 8 字节(紧凑模式),否则返回 16 字节。
     *
     * @return 8 或 16 字节
     */
    @Override
    public int defaultSize() {
        return isCompact(precision) ? 8 : 16;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 DecimalType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new DecimalType(isNullable, precision, scale);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "DECIMAL(精度, 小数位数)" 或 "DECIMAL(精度, 小数位数) NOT NULL"
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT, precision, scale);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DecimalType that = (DecimalType) o;
        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, scale);
    }

    /**
     * 接受访问者访问,实现访问者模式。
     *
     * @param visitor 数据类型访问者
     * @param <R> 访问结果类型
     * @return 访问结果
     */
    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    /**
     * 判断给定精度的十进制数是否可以使用紧凑模式存储。
     *
     * <p>当精度 <= 18 时,可以使用 Java 的 long 类型(8字节)存储,
     * 否则需要使用更大的存储空间(16字节)。
     *
     * @param precision 精度值
     * @return 如果可以使用紧凑模式则返回 true
     */
    public static boolean isCompact(int precision) {
        return precision <= MAX_COMPACT_PRECISION;
    }
}
