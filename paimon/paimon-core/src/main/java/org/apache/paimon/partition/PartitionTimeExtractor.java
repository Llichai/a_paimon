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

package org.apache.paimon.partition;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * 时间提取器,用于从分区值中提取时间。
 *
 * <p>支持多种时间格式和自定义时间模式,用于解析分区值中的时间信息。
 */
public class PartitionTimeExtractor {

    /** 时间戳格式化器,支持日期和时间 */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
                    .optionalStart()
                    .appendLiteral(" ")
                    .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                    .optionalEnd()
                    .optionalEnd()
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.LENIENT);

    /** 日期格式化器,仅支持日期 */
    private static final DateTimeFormatter DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.LENIENT);

    /** 时间模式,用于从分区值组合时间字符串 */
    @Nullable private final String pattern;
    /** 格式化器模式字符串 */
    @Nullable private final String formatter;

    /**
     * 构造分区时间提取器。
     *
     * @param pattern 时间模式,null表示使用第一个分区值
     * @param formatter 格式化器模式,null表示使用默认格式
     */
    public PartitionTimeExtractor(@Nullable String pattern, @Nullable String formatter) {
        this.pattern = pattern;
        this.formatter = formatter;
    }

    /**
     * 从分区规格中提取时间。
     *
     * @param spec 分区规格映射
     * @return 提取的本地日期时间
     */
    public LocalDateTime extract(LinkedHashMap<String, String> spec) {
        return extract(new ArrayList<>(spec.keySet()), new ArrayList<>(spec.values()));
    }

    /**
     * 从分区键和值中提取时间。
     *
     * @param partitionKeys 分区键列表
     * @param partitionValues 分区值列表
     * @return 提取的本地日期时间
     */
    public LocalDateTime extract(List<String> partitionKeys, List<?> partitionValues) {
        String timestampString;
        if (pattern == null) {
            timestampString = partitionValues.get(0).toString();
        } else {
            timestampString = pattern;
            for (int i = 0; i < partitionKeys.size(); i++) {
                timestampString =
                        timestampString.replaceAll(
                                "\\$" + partitionKeys.get(i), partitionValues.get(i).toString());
            }
        }
        return toLocalDateTime(timestampString, this.formatter);
    }

    /**
     * 将时间字符串转换为本地日期时间。
     *
     * @param timestampString 时间字符串
     * @param formatterPattern 格式化器模式
     * @return 本地日期时间
     */
    private static LocalDateTime toLocalDateTime(
            String timestampString, @Nullable String formatterPattern) {

        if (formatterPattern == null) {
            return PartitionTimeExtractor.toLocalDateTimeDefault(timestampString);
        }
        DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern(Objects.requireNonNull(formatterPattern), Locale.ROOT);
        try {
            return LocalDateTime.parse(timestampString, Objects.requireNonNull(dateTimeFormatter));
        } catch (DateTimeParseException e) {
            return LocalDateTime.of(
                    LocalDate.parse(timestampString, Objects.requireNonNull(dateTimeFormatter)),
                    LocalTime.MIDNIGHT);
        }
    }

    /**
     * 使用默认格式将时间字符串转换为本地日期时间。
     *
     * @param timestampString 时间字符串
     * @return 本地日期时间
     */
    public static LocalDateTime toLocalDateTimeDefault(String timestampString) {
        try {
            return LocalDateTime.parse(timestampString, TIMESTAMP_FORMATTER);
        } catch (DateTimeParseException e) {
            return LocalDateTime.of(
                    LocalDate.parse(timestampString, DATE_FORMATTER), LocalTime.MIDNIGHT);
        }
    }
}
