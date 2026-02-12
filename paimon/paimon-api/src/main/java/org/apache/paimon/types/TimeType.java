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
 * 时间数据类型(不含时区),由 {@code 时:分:秒[.小数秒]} 组成,支持最高纳秒精度,
 * 取值范围从 {@code 00:00:00.000000000} 到 {@code 23:59:59.999999999}。
 *
 * <p>与 SQL 标准相比,不支持闰秒(23:59:60 和 23:59:61),语义更接近 {@link java.time.LocalTime}。
 * 不提供带时区的时间类型。
 *
 * <p><b>内部表示:</b>
 * <ul>
 *   <li>与 {@code int} 之间的转换表示当天的毫秒数</li>
 *   <li>与 {@code long} 之间的转换表示当天的纳秒数</li>
 * </ul>
 *
 * <p><b>精度范围:</b> 0-9,默认为 0。精度表示小数秒的位数。
 *
 * <p><b>使用场景:</b> 适用于:
 * <ul>
 *   <li>营业时间、作息时间等一天内的时间点</li>
 *   <li>定时任务的触发时间</li>
 *   <li>时间段的开始和结束时间</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class TimeType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 最小精度(小数秒位数)。 */
    public static final int MIN_PRECISION = 0;

    /** 最大精度(小数秒位数),支持到纳秒级别。 */
    public static final int MAX_PRECISION = 9;

    /** 默认精度(小数秒位数)。 */
    public static final int DEFAULT_PRECISION = 0;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "TIME(%d)";

    /** 小数秒的精度(位数)。 */
    private final int precision;

    /**
     * 构造一个 Time 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param precision 小数秒的精度,范围 0-9
     * @throws IllegalArgumentException 如果精度超出有效范围
     */
    public TimeType(boolean isNullable, int precision) {
        super(isNullable, DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Time precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.precision = precision;
    }

    /**
     * 构造一个默认允许 null 的 Time 类型实例。
     *
     * @param precision 小数秒的精度
     */
    public TimeType(int precision) {
        this(true, precision);
    }

    /**
     * 构造一个默认精度(0)的 Time 类型实例。
     */
    public TimeType() {
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
     * @return 4 字节
     */
    @Override
    public int defaultSize() {
        return 4;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 TimeType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new TimeType(isNullable, precision);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "TIME(精度)" 或 "TIME(精度) NOT NULL"
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
        TimeType timeType = (TimeType) o;
        return precision == timeType.precision;
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
