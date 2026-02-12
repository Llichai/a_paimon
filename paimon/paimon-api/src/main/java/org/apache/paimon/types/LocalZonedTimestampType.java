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
 * 带本地时区的时间戳数据类型,由 {@code 年-月-日 时:分:秒[.小数秒] 时区} 组成,
 * 支持最高纳秒精度,取值范围从 {@code 0000-01-01 00:00:00.000000000 +14:59} 到
 * {@code 9999-12-31 23:59:59.999999999 -14:59}。
 *
 * <p>不支持闰秒(23:59:60 和 23:59:61),语义更接近 {@link java.time.OffsetDateTime}。
 *
 * <p>该类型填补了无时区和强制时区时间戳类型之间的空白,允许根据配置的会话时区
 * 来解释 UTC 时间戳。与 {@code int} 之间的转换表示自 epoch 以来的秒数,
 * 与 {@code long} 之间的转换表示自 epoch 以来的毫秒数。
 *
 * <p><b>精度范围:</b> 0-9,默认为 6。精度表示小数秒的位数。
 *
 * <p><b>时区处理:</b> 该类型会根据当前会话的时区自动转换时间戳,适用于需要
 * 跨时区处理时间的场景。内部以 UTC 时间存储,显示时按会话时区转换。
 *
 * <p><b>使用场景:</b> 适用于:
 * <ul>
 *   <li>需要跨时区的应用程序</li>
 *   <li>全球性业务的时间记录</li>
 *   <li>需要根据用户时区显示时间的场景</li>
 * </ul>
 *
 * @see TimestampType
 * @since 0.4.0
 */
@Public
public final class LocalZonedTimestampType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 最小精度(小数秒位数)。 */
    public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

    /** 最大精度(小数秒位数),支持到纳秒级别。 */
    public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

    /** 默认精度(小数秒位数),微秒级别。 */
    public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "TIMESTAMP(%d) WITH LOCAL TIME ZONE";

    /** 小数秒的精度(位数)。 */
    private final int precision;

    /**
     * 构造一个带本地时区的 Timestamp 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param precision 小数秒的精度,范围 0-9
     * @throws IllegalArgumentException 如果精度超出有效范围
     */
    public LocalZonedTimestampType(boolean isNullable, int precision) {
        super(isNullable, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Timestamp with local time zone precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.precision = precision;
    }

    /**
     * 构造一个默认允许 null 的带本地时区的 Timestamp 类型实例。
     *
     * @param precision 小数秒的精度
     */
    public LocalZonedTimestampType(int precision) {
        this(true, precision);
    }

    /**
     * 构造一个默认精度(6,微秒级别)的带本地时区的 Timestamp 类型实例。
     */
    public LocalZonedTimestampType() {
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
     * @return 新的 LocalZonedTimestampType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new LocalZonedTimestampType(isNullable, precision);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "TIMESTAMP(精度) WITH LOCAL TIME ZONE" 或带 NOT NULL 后缀
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
        LocalZonedTimestampType that = (LocalZonedTimestampType) o;
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
