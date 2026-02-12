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
 * 时间戳数据类型(不含时区),由 {@code 年-月-日 时:分:秒[.小数秒]} 组成,支持最高纳秒精度,
 * 取值范围从 {@code 0000-01-01 00:00:00.000000000} 到 {@code 9999-12-31 23:59:59.999999999}。
 *
 * <p>与 SQL 标准相比,不支持闰秒(23:59:60 和 23:59:61),语义更接近 {@link java.time.LocalDateTime}。
 *
 * <p><b>精度范围:</b> 0-9,默认为 6。精度表示小数秒的位数。
 *
 * <p><b>使用场景:</b> 适用于:
 * <ul>
 *   <li>不需要时区信息的时间戳</li>
 *   <li>与特定时区无关的时间点记录</li>
 *   <li>本地时间的存储和处理</li>
 * </ul>
 *
 * <p>如果需要处理时区,请使用 {@link LocalZonedTimestampType}。
 *
 * @see LocalZonedTimestampType
 * @since 0.4.0
 */
@Public
public class TimestampType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 最小精度(小数秒位数)。 */
    public static final int MIN_PRECISION = 0;

    /** 最大精度(小数秒位数),支持到纳秒级别。 */
    public static final int MAX_PRECISION = 9;

    /** 默认精度(小数秒位数),微秒级别。 */
    public static final int DEFAULT_PRECISION = 6;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "TIMESTAMP(%d)";

    /** 小数秒的精度(位数)。 */
    private final int precision;

    /**
     * 构造一个 Timestamp 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param precision 小数秒的精度,范围 0-9
     * @throws IllegalArgumentException 如果精度超出有效范围
     */
    public TimestampType(boolean isNullable, int precision) {
        super(isNullable, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Timestamp precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.precision = precision;
    }

    /**
     * 构造一个默认允许 null 的 Timestamp 类型实例。
     *
     * @param precision 小数秒的精度
     */
    public TimestampType(int precision) {
        this(true, precision);
    }

    /**
     * 构造一个默认精度(6,微秒级别)的 Timestamp 类型实例。
     */
    public TimestampType() {
        this(DEFAULT_PRECISION);
    }

    /**
     * 获取小数秒的精度。
     *
     * @return 精度值(0-9)
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * 返回该类型的默认存储大小。
     *
     * @return 8 字节
     */
    @Override
    public int defaultSize() {
        return 8;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 TimestampType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new TimestampType(isNullable, precision);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "TIMESTAMP(精度)" 或 "TIMESTAMP(精度) NOT NULL"
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT, precision);
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
        TimestampType that = (TimestampType) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
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
}
