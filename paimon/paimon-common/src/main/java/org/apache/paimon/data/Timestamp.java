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
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Preconditions;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * {@link TimestampType} 的内部数据结构实现。
 *
 * <p>此数据结构是不可变的,由自 {@code 1970-01-01 00:00:00} 以来的毫秒数和
 * 毫秒内的纳秒数组成。对于足够小的值,可能以紧凑表示(作为 long 值)存储。
 *
 * <p><b>时间表示:</b>
 * Timestamp 使用两个字段来表示高精度的时间戳:
 * <ul>
 *   <li>{@code millisecond}: 自 Unix 纪元(1970-01-01 00:00:00 UTC)以来的毫秒数</li>
 *   <li>{@code nanoOfMillisecond}: 毫秒内的纳秒数(0-999,999)</li>
 * </ul>
 *
 * <p><b>精度支持:</b>
 * Timestamp 最高支持纳秒精度(9位小数):
 * <pre>
 * 精度 0-3: 仅需毫秒部分,可使用紧凑格式(long)存储
 * 精度 4-6: 需要微秒精度,使用完整格式
 * 精度 7-9: 需要纳秒精度,使用完整格式
 * </pre>
 *
 * <p><b>存储格式:</b>
 * <ul>
 *   <li><b>紧凑格式</b>(precision <= 3): 仅存储毫秒数(long),nanoOfMillisecond=0
 *       <br>适用于不需要毫秒以下精度的场景</li>
 *   <li><b>完整格式</b>(precision > 3): 存储毫秒数 + 纳秒数
 *       <br>用于需要微秒或纳秒精度的场景</li>
 * </ul>
 *
 * <p><b>时间范围:</b>
 * 可以表示从公元前 29201 年到公元 30828 年的时间戳
 * (受限于 long 类型的毫秒数范围)。
 *
 * <p>注意:此类在历史上也用于表示 {@link LocalZonedTimestampType},
 * 但现在推荐使用 {@link LocalZoneTimestamp} 来表示带时区的时间戳。
 *
 * <p>使用场景:
 * <ul>
 *   <li>记录事件发生的精确时间</li>
 *   <li>日志和审计系统</li>
 *   <li>时间序列数据</li>
 *   <li>对应 SQL TIMESTAMP 类型</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class Timestamp implements Comparable<Timestamp>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 一天中的毫秒数。 */
    public static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    /** 每毫秒的微秒数。 */
    public static final long MICROS_PER_MILLIS = 1000L;

    /** 每微秒的纳秒数。 */
    public static final long NANOS_PER_MICROS = 1000L;

    /** 此字段保存自纪元以来的毫秒数(包括整秒和毫秒部分)。 */
    private final long millisecond;

    /** 此字段保存毫秒内的纳秒数(0-999,999)。 */
    private final int nanoOfMillisecond;

    /**
     * 构造一个 Timestamp 实例。
     *
     * @param millisecond 自纪元以来的毫秒数
     * @param nanoOfMillisecond 毫秒内的纳秒数,必须在 [0, 999999] 范围内
     * @throws IllegalArgumentException 如果 nanoOfMillisecond 超出范围
     */
    private Timestamp(long millisecond, int nanoOfMillisecond) {
        Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
        this.millisecond = millisecond;
        this.nanoOfMillisecond = nanoOfMillisecond;
    }

    /**
     * 返回自 {@code 1970-01-01 00:00:00} 以来的毫秒数。
     *
     * @return 毫秒数
     */
    public long getMillisecond() {
        return millisecond;
    }

    /**
     * 返回毫秒内的纳秒数。
     *
     * <p>值范围从 0 到 999,999。
     * 与 {@link #getMillisecond()} 结合使用可获得完整的纳秒精度时间戳。
     *
     * @return 毫秒内的纳秒数
     */
    public int getNanoOfMillisecond() {
        return nanoOfMillisecond;
    }

    /**
     * 将此 {@link Timestamp} 对象转换为 {@link java.sql.Timestamp}。
     *
     * @return SQL Timestamp 对象
     */
    public java.sql.Timestamp toSQLTimestamp() {
        return java.sql.Timestamp.valueOf(toLocalDateTime());
    }

    /**
     * 将此 Timestamp 转换为仅包含毫秒精度的 Timestamp。
     *
     * <p>会丢弃纳秒部分,仅保留毫秒精度。
     *
     * @return 毫秒精度的 Timestamp
     */
    public Timestamp toMillisTimestamp() {
        return fromEpochMillis(millisecond);
    }

    /**
     * 将此 {@link Timestamp} 对象转换为 {@link LocalDateTime}。
     *
     * <p>LocalDateTime 表示不带时区的日期时间。
     *
     * @return LocalDateTime 对象
     */
    public LocalDateTime toLocalDateTime() {
        int date = (int) (millisecond / MILLIS_PER_DAY);
        int time = (int) (millisecond % MILLIS_PER_DAY);
        if (time < 0) {
            --date;
            time += MILLIS_PER_DAY;
        }
        long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return LocalDateTime.of(localDate, localTime);
    }

    /**
     * Converts this {@link Timestamp} object to a {@link Instant}.
     *
     * @deprecated use {@link LocalZoneTimestamp}.
     */
    @Deprecated
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

    /** Converts this {@link Timestamp} object to micros. */
    public long toMicros() {
        long micros = Math.multiplyExact(millisecond, MICROS_PER_MILLIS);
        return micros + nanoOfMillisecond / NANOS_PER_MICROS;
    }

    @Override
    public int compareTo(Timestamp that) {
        int cmp = Long.compare(this.millisecond, that.millisecond);
        if (cmp == 0) {
            cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Timestamp)) {
            return false;
        }
        Timestamp that = (Timestamp) obj;
        return this.millisecond == that.millisecond
                && this.nanoOfMillisecond == that.nanoOfMillisecond;
    }

    @Override
    public String toString() {
        return toLocalDateTime().toString();
    }

    @Override
    public int hashCode() {
        int ret = (int) millisecond ^ (int) (millisecond >> 32);
        return 31 * ret + nanoOfMillisecond;
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /** Creates an instance of {@link Timestamp} for now. */
    public static Timestamp now() {
        return fromLocalDateTime(LocalDateTime.now());
    }

    /**
     * Creates an instance of {@link Timestamp} from milliseconds.
     *
     * <p>The nanos-of-millisecond field will be set to zero.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     */
    public static Timestamp fromEpochMillis(long milliseconds) {
        return new Timestamp(milliseconds, 0);
    }

    /**
     * Creates an instance of {@link Timestamp} from milliseconds and a nanos-of-millisecond.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     * @param nanosOfMillisecond the nanoseconds within the millisecond, from 0 to 999,999
     */
    public static Timestamp fromEpochMillis(long milliseconds, int nanosOfMillisecond) {
        return new Timestamp(milliseconds, nanosOfMillisecond);
    }

    /**
     * Creates an instance of {@link Timestamp} from an instance of {@link LocalDateTime}.
     *
     * @param dateTime an instance of {@link LocalDateTime}
     */
    public static Timestamp fromLocalDateTime(LocalDateTime dateTime) {
        long epochDay = dateTime.toLocalDate().toEpochDay();
        long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

        long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
        int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);

        return new Timestamp(millisecond, nanoOfMillisecond);
    }

    /**
     * Creates an instance of {@link Timestamp} from an instance of {@link java.sql.Timestamp}.
     *
     * @param timestamp an instance of {@link java.sql.Timestamp}
     */
    public static Timestamp fromSQLTimestamp(java.sql.Timestamp timestamp) {
        return fromLocalDateTime(timestamp.toLocalDateTime());
    }

    /**
     * Creates an instance of {@link Timestamp} from an instance of {@link Instant}.
     *
     * @param instant an instance of {@link Instant}
     * @deprecated use {@link LocalZoneTimestamp}.
     */
    @Deprecated
    public static Timestamp fromInstant(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nanoSecond = instant.getNano();

        long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
        int nanoOfMillisecond = nanoSecond % 1_000_000;

        return new Timestamp(millisecond, nanoOfMillisecond);
    }

    /** Creates an instance of {@link Timestamp} from micros. */
    public static Timestamp fromMicros(long micros) {
        long mills = Math.floorDiv(micros, MICROS_PER_MILLIS);
        long nanos = (micros - mills * MICROS_PER_MILLIS) * NANOS_PER_MICROS;
        return Timestamp.fromEpochMillis(mills, (int) nanos);
    }

    /**
     * Returns whether the timestamp data is small enough to be stored in a long of milliseconds.
     */
    public static boolean isCompact(int precision) {
        return precision <= 3;
    }
}
