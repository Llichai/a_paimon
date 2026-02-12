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
import org.apache.paimon.types.LocalZonedTimestampType;

import java.io.Serializable;
import java.time.Instant;

import static org.apache.paimon.data.Timestamp.MICROS_PER_MILLIS;
import static org.apache.paimon.data.Timestamp.NANOS_PER_MICROS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 本地时区时间戳。
 *
 * <p>表示 {@link LocalZonedTimestampType} 类型数据的内部数据结构。
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li><b>不可变性</b>:时间戳对象一旦创建,值不可修改</li>
 *   <li><b>高精度</b>:支持纳秒级精度(0-999,999纳秒)</li>
 *   <li><b>紧凑存储</b>:当精度≤3时可以用long存储,否则需要long+int</li>
 *   <li><b>时区独立</b>:存储相对于UTC 1970-01-01 00:00:00的偏移量</li>
 * </ul>
 *
 * <h2>内部结构</h2>
 * <pre>
 * LocalZoneTimestamp 组成:
 * ┌─────────────────────────────────────────┐
 * │ millisecond: long                       │ ← 毫秒数(自1970-01-01 00:00:00)
 * │ nanoOfMillisecond: int                  │ ← 毫秒内的纳秒(0-999,999)
 * └─────────────────────────────────────────┘
 *
 * 时间表示:
 * 完整时间 = millisecond毫秒 + nanoOfMillisecond纳秒
 *
 * 示例: 2024-01-15 10:30:45.123456789
 *   millisecond     = 1705318245123 (到.123的毫秒数)
 *   nanoOfMillisecond = 456789 (毫秒后的456,789纳秒)
 * </pre>
 *
 * <h2>精度支持</h2>
 * <table border="1">
 *   <tr>
 *     <th>精度</th>
 *     <th>说明</th>
 *     <th>存储方式</th>
 *     <th>示例</th>
 *   </tr>
 *   <tr>
 *     <td>0</td>
 *     <td>秒级</td>
 *     <td>紧凑(long)</td>
 *     <td>2024-01-15 10:30:45</td>
 *   </tr>
 *   <tr>
 *     <td>3</td>
 *     <td>毫秒级</td>
 *     <td>紧凑(long)</td>
 *     <td>2024-01-15 10:30:45.123</td>
 *   </tr>
 *   <tr>
 *     <td>6</td>
 *     <td>微秒级</td>
 *     <td>完整(long+int)</td>
 *     <td>2024-01-15 10:30:45.123456</td>
 *   </tr>
 *   <tr>
 *     <td>9</td>
 *     <td>纳秒级</td>
 *     <td>完整(long+int)</td>
 *     <td>2024-01-15 10:30:45.123456789</td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建当前时间戳
 * LocalZoneTimestamp now = LocalZoneTimestamp.now();
 *
 * // 从毫秒创建
 * LocalZoneTimestamp ts1 = LocalZoneTimestamp.fromEpochMillis(1705318245123L);
 *
 * // 从毫秒+纳秒创建(高精度)
 * LocalZoneTimestamp ts2 = LocalZoneTimestamp.fromEpochMillis(
 *     1705318245123L,  // 毫秒
 *     456789           // 毫秒内的纳秒
 * );
 *
 * // 从Java时间对象创建
 * Instant instant = Instant.now();
 * LocalZoneTimestamp ts3 = LocalZoneTimestamp.fromInstant(instant);
 *
 * java.sql.Timestamp sqlTs = new java.sql.Timestamp(System.currentTimeMillis());
 * LocalZoneTimestamp ts4 = LocalZoneTimestamp.fromSQLTimestamp(sqlTs);
 *
 * // 从微秒创建
 * LocalZoneTimestamp ts5 = LocalZoneTimestamp.fromMicros(1705318245123456L);
 *
 * // 转换为Java对象
 * Instant instant2 = ts2.toInstant();
 * java.sql.Timestamp sqlTs2 = ts2.toSQLTimestamp();
 * long micros = ts2.toMicros();
 *
 * // 降低精度到毫秒
 * LocalZoneTimestamp millis = ts2.toMillisTimestamp();
 *
 * // 比较
 * int cmp = ts1.compareTo(ts2);
 *
 * // 检查是否可以紧凑存储
 * boolean compact = LocalZoneTimestamp.isCompact(3);  // true (精度≤3)
 * boolean notCompact = LocalZoneTimestamp.isCompact(6); // false (精度>3)
 * }</pre>
 *
 * <h2>与Timestamp的区别</h2>
 * <ul>
 *   <li>{@link Timestamp}:不带时区的时间戳</li>
 *   <li>{@link LocalZoneTimestamp}:带本地时区的时间戳</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>毫秒字段可以是负数(表示1970年之前的时间)</li>
 *   <li>纳秒字段范围:[0, 999,999]</li>
 *   <li>构造函数会验证纳秒范围,超出范围会抛出异常</li>
 *   <li>精度≤3时建议使用紧凑表示(仅毫秒)</li>
 * </ul>
 *
 * @since 0.9.0
 * @see LocalZonedTimestampType
 * @see Timestamp
 * @see java.time.Instant
 */
@Public
public final class LocalZoneTimestamp implements Comparable<LocalZoneTimestamp>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 存储整秒数和毫秒内的秒数。 */
    private final long millisecond;

    /** 存储毫秒内的纳秒数(0-999,999)。 */
    private final int nanoOfMillisecond;

    /**
     * 构造LocalZoneTimestamp对象。
     *
     * @param millisecond 毫秒数(自1970-01-01 00:00:00)
     * @param nanoOfMillisecond 毫秒内的纳秒数,必须在[0, 999,999]范围内
     * @throws IllegalArgumentException 如果nanoOfMillisecond超出有效范围
     */
    private LocalZoneTimestamp(long millisecond, int nanoOfMillisecond) {
        checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
        this.millisecond = millisecond;
        this.nanoOfMillisecond = nanoOfMillisecond;
    }

    /**
     * 返回自1970-01-01 00:00:00以来的毫秒数。
     *
     * @return 毫秒数
     */
    public long getMillisecond() {
        return millisecond;
    }

    /**
     * 返回毫秒内的纳秒数。
     *
     * <p>值范围是0到999,999。
     *
     * @return 毫秒内的纳秒数
     */
    public int getNanoOfMillisecond() {
        return nanoOfMillisecond;
    }

    /**
     * 将LocalZoneTimestamp转换为 {@link java.sql.Timestamp}。
     *
     * @return SQL时间戳对象
     */
    public java.sql.Timestamp toSQLTimestamp() {
        return java.sql.Timestamp.from(toInstant());
    }

    /**
     * 转换为仅包含毫秒精度的时间戳。
     *
     * <p>丢弃纳秒部分,仅保留毫秒。
     *
     * @return 毫秒精度的时间戳
     */
    public LocalZoneTimestamp toMillisTimestamp() {
        return fromEpochMillis(millisecond);
    }

    /**
     * 将LocalZoneTimestamp转换为 {@link Instant}。
     *
     * @return Instant对象
     */
    public Instant toInstant() {
        long epochSecond = millisecond / 1000;
        int milliOfSecond = (int) (millisecond % 1000);
        if (milliOfSecond < 0) {
            --epochSecond;
            milliOfSecond += 1000;
        }
        long nanoAdjustment = milliOfSecond * 1_000_000 + nanoOfMillisecond;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    /**
     * 将LocalZoneTimestamp转换为微秒。
     *
     * @return 自1970-01-01 00:00:00以来的微秒数
     */
    public long toMicros() {
        long micros = Math.multiplyExact(millisecond, MICROS_PER_MILLIS);
        return micros + nanoOfMillisecond / NANOS_PER_MICROS;
    }

    /**
     * 比较两个LocalZoneTimestamp对象。
     *
     * <p>首先比较毫秒部分,如果相等则比较纳秒部分。
     *
     * @param that 要比较的另一个时间戳
     * @return 负数表示this < that, 0表示相等, 正数表示this > that
     */
    @Override
    public int compareTo(LocalZoneTimestamp that) {
        int cmp = Long.compare(this.millisecond, that.millisecond);
        if (cmp == 0) {
            cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LocalZoneTimestamp)) {
            return false;
        }
        LocalZoneTimestamp that = (LocalZoneTimestamp) obj;
        return this.millisecond == that.millisecond
                && this.nanoOfMillisecond == that.nanoOfMillisecond;
    }

    @Override
    public String toString() {
        return toSQLTimestamp().toLocalDateTime().toString();
    }

    @Override
    public int hashCode() {
        int ret = (int) millisecond ^ (int) (millisecond >> 32);
        return 31 * ret + nanoOfMillisecond;
    }

    // ------------------------------------------------------------------------------------------
    // 构造工具方法
    // ------------------------------------------------------------------------------------------

    /**
     * 创建表示当前时间的LocalZoneTimestamp实例。
     *
     * @return 当前时间的时间戳
     */
    public static LocalZoneTimestamp now() {
        return fromInstant(Instant.now());
    }

    /**
     * 从毫秒数创建LocalZoneTimestamp实例。
     *
     * <p>毫秒内的纳秒字段将被设置为0。
     *
     * @param milliseconds 自1970-01-01 00:00:00以来的毫秒数;
     *                     负数表示1970-01-01 00:00:00之前的毫秒数
     * @return LocalZoneTimestamp实例
     */
    public static LocalZoneTimestamp fromEpochMillis(long milliseconds) {
        return new LocalZoneTimestamp(milliseconds, 0);
    }

    /**
     * 从毫秒数和毫秒内的纳秒数创建LocalZoneTimestamp实例。
     *
     * @param milliseconds 自1970-01-01 00:00:00以来的毫秒数;
     *                     负数表示1970-01-01 00:00:00之前的毫秒数
     * @param nanosOfMillisecond 毫秒内的纳秒数,范围0到999,999
     * @return LocalZoneTimestamp实例
     */
    public static LocalZoneTimestamp fromEpochMillis(long milliseconds, int nanosOfMillisecond) {
        return new LocalZoneTimestamp(milliseconds, nanosOfMillisecond);
    }

    /**
     * 从 {@link java.sql.Timestamp} 创建LocalZoneTimestamp实例。
     *
     * @param timestamp java.sql.Timestamp实例
     * @return LocalZoneTimestamp实例
     */
    public static LocalZoneTimestamp fromSQLTimestamp(java.sql.Timestamp timestamp) {
        return fromInstant(timestamp.toInstant());
    }

    /**
     * 从 {@link Instant} 创建LocalZoneTimestamp实例。
     *
     * @param instant Instant实例
     * @return LocalZoneTimestamp实例
     */
    public static LocalZoneTimestamp fromInstant(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nanoSecond = instant.getNano();

        long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
        int nanoOfMillisecond = nanoSecond % 1_000_000;

        return new LocalZoneTimestamp(millisecond, nanoOfMillisecond);
    }

    /**
     * 从微秒数创建LocalZoneTimestamp实例。
     *
     * @param micros 自1970-01-01 00:00:00以来的微秒数
     * @return LocalZoneTimestamp实例
     */
    public static LocalZoneTimestamp fromMicros(long micros) {
        long mills = Math.floorDiv(micros, MICROS_PER_MILLIS);
        long nanos = (micros - mills * MICROS_PER_MILLIS) * NANOS_PER_MICROS;
        return LocalZoneTimestamp.fromEpochMillis(mills, (int) nanos);
    }

    /**
     * 判断时间戳数据是否可以用long类型的毫秒数紧凑存储。
     *
     * <p>当精度小于等于3(毫秒级)时,可以使用紧凑存储。
     *
     * @param precision 时间戳精度
     * @return true表示可以紧凑存储, false表示需要完整存储(long+int)
     */
    public static boolean isCompact(int precision) {
        return precision <= 3;
    }
}
