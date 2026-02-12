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

package org.apache.paimon.options;

import org.apache.paimon.annotation.Public;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.paimon.options.MemorySize.MemoryUnit.BYTES;
import static org.apache.paimon.options.MemorySize.MemoryUnit.GIGA_BYTES;
import static org.apache.paimon.options.MemorySize.MemoryUnit.KILO_BYTES;
import static org.apache.paimon.options.MemorySize.MemoryUnit.MEGA_BYTES;
import static org.apache.paimon.options.MemorySize.MemoryUnit.TERA_BYTES;
import static org.apache.paimon.options.MemorySize.MemoryUnit.hasUnit;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 内存大小类,表示字节数,可以用不同单位查看。
 *
 * <h2>解析规则</h2>
 * 内存大小可以从文本表达式解析。如果表达式是纯数字,则值将被解释为字节。
 *
 * <h2>支持的单位</h2>
 * 为了使较大的值更紧凑,支持常见的大小后缀:
 * <ul>
 *   <li>1b 或 1bytes (字节)
 *   <li>1k 或 1kb 或 1kibibytes (解释为 kibibytes = 1024 字节)
 *   <li>1m 或 1mb 或 1mebibytes (解释为 mebibytes = 1024 kibibytes)
 *   <li>1g 或 1gb 或 1gibibytes (解释为 gibibytes = 1024 mebibytes)
 *   <li>1t 或 1tb 或 1tebibytes (解释为 tebibytes = 1024 gibibytes)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建内存大小对象
 * MemorySize size1 = MemorySize.ofMebiBytes(128);  // 128 MB
 * MemorySize size2 = MemorySize.parse("256mb");     // 256 MB
 * MemorySize size3 = new MemorySize(1024);          // 1024 bytes
 *
 * // 获取不同单位的值
 * long bytes = size1.getBytes();        // 134217728
 * int mebibytes = size1.getMebiBytes(); // 128
 *
 * // 执行计算
 * MemorySize sum = size1.add(size2);    // 384 MB
 * MemorySize doubled = size1.multiply(2); // 256 MB
 *
 * // 格式化输出
 * String str = size1.toString();                // "128 mb"
 * String readable = size1.toHumanReadableString(); // "128.000mb (134217728 bytes)"
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public class MemorySize implements java.io.Serializable, Comparable<MemorySize> {

    private static final long serialVersionUID = 1L;

    /** 零内存大小的常量 */
    public static final MemorySize ZERO = new MemorySize(0L);

    /** 最大内存大小的常量 (Long.MAX_VALUE 字节) */
    public static final MemorySize MAX_VALUE = new MemorySize(Long.MAX_VALUE);

    /** 32 KB 内存大小的常量 */
    public static final MemorySize VALUE_32_KB = MemorySize.ofKibiBytes(32);

    /** 8 MB 内存大小的常量 */
    public static final MemorySize VALUE_8_MB = MemorySize.ofMebiBytes(8);

    /** 128 MB 内存大小的常量 */
    public static final MemorySize VALUE_128_MB = MemorySize.ofMebiBytes(128);

    /** 256 MB 内存大小的常量 */
    public static final MemorySize VALUE_256_MB = MemorySize.ofMebiBytes(256);

    /** 内存单位的有序列表,用于格式化输出 */
    private static final List<MemoryUnit> ORDERED_UNITS =
            Arrays.asList(BYTES, KILO_BYTES, MEGA_BYTES, GIGA_BYTES, TERA_BYTES);

    // ------------------------------------------------------------------------

    /** 内存大小,以字节为单位 */
    private final long bytes;

    /** 缓存的 toString() 返回值 */
    private transient String stringified;

    /** 缓存的 toHumanReadableString() 返回值 */
    private transient String humanReadableStr;

    /**
     * 构造一个新的 MemorySize。
     *
     * @param bytes 大小,以字节为单位。必须大于或等于零
     */
    public MemorySize(long bytes) {
        checkArgument(bytes >= 0, "bytes must be >= 0");
        this.bytes = bytes;
    }

    /**
     * 创建以 Mebibytes (MiB) 为单位的 MemorySize。
     *
     * @param mebiBytes Mebibytes 数量 (1 MiB = 1024 KiB)
     * @return MemorySize 对象
     */
    public static MemorySize ofMebiBytes(long mebiBytes) {
        return new MemorySize(mebiBytes << 20);
    }

    /**
     * 创建以 Kibibytes (KiB) 为单位的 MemorySize。
     *
     * @param kibiBytes Kibibytes 数量 (1 KiB = 1024 bytes)
     * @return MemorySize 对象
     */
    public static MemorySize ofKibiBytes(long kibiBytes) {
        return new MemorySize(kibiBytes << 10);
    }

    /**
     * 创建以字节为单位的 MemorySize。
     *
     * @param bytes 字节数
     * @return MemorySize 对象
     */
    public static MemorySize ofBytes(long bytes) {
        return new MemorySize(bytes);
    }

    // ------------------------------------------------------------------------

    /** 获取以字节为单位的内存大小。 */
    public long getBytes() {
        return bytes;
    }

    /** 获取以 Kibibytes (= 1024 bytes) 为单位的内存大小。 */
    public long getKibiBytes() {
        return bytes >> 10;
    }

    /** 获取以 Mebibytes (= 1024 Kibibytes) 为单位的内存大小。 */
    public int getMebiBytes() {
        return (int) (bytes >> 20);
    }

    /** 获取以 Gibibytes (= 1024 Mebibytes) 为单位的内存大小。 */
    public long getGibiBytes() {
        return bytes >> 30;
    }

    /** 获取以 Tebibytes (= 1024 Gibibytes) 为单位的内存大小。 */
    public long getTebiBytes() {
        return bytes >> 40;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return (int) (bytes ^ (bytes >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == this.getClass()
                        && ((MemorySize) obj).bytes == this.bytes);
    }

    @Override
    public String toString() {
        if (stringified == null) {
            stringified = formatToString();
        }

        return stringified;
    }

    private String formatToString() {
        MemoryUnit highestIntegerUnit =
                IntStream.range(0, ORDERED_UNITS.size())
                        .sequential()
                        .filter(idx -> bytes % ORDERED_UNITS.get(idx).getMultiplier() != 0)
                        .boxed()
                        .findFirst()
                        .map(
                                idx -> {
                                    if (idx == 0) {
                                        return ORDERED_UNITS.get(0);
                                    } else {
                                        return ORDERED_UNITS.get(idx - 1);
                                    }
                                })
                        .orElse(BYTES);

        return String.format(
                "%d %s",
                bytes / highestIntegerUnit.getMultiplier(), highestIntegerUnit.getUnits()[1]);
    }

    /**
     * 返回人类可读的字符串表示形式。
     *
     * <p>格式为: "数值单位 (字节数 bytes)"
     * <p>例如: "128.000mb (134217728 bytes)"
     *
     * @return 人类可读的字符串
     */
    public String toHumanReadableString() {
        if (humanReadableStr == null) {
            humanReadableStr = formatToHumanReadableString();
        }

        return humanReadableStr;
    }

    /**
     * 格式化为人类可读的字符串。
     * 使用最高的可用单位,并显示小数位数。
     */
    private String formatToHumanReadableString() {
        MemoryUnit highestUnit =
                IntStream.range(0, ORDERED_UNITS.size())
                        .sequential()
                        .filter(idx -> bytes > ORDERED_UNITS.get(idx).getMultiplier())
                        .boxed()
                        .max(Comparator.naturalOrder())
                        .map(ORDERED_UNITS::get)
                        .orElse(BYTES);

        if (highestUnit == BYTES) {
            return String.format("%d %s", bytes, BYTES.getUnits()[1]);
        } else {
            double approximate = 1.0 * bytes / highestUnit.getMultiplier();
            return String.format(
                    Locale.ROOT,
                    "%.3f%s (%d bytes)",
                    approximate,
                    highestUnit.getUnits()[1],
                    bytes);
        }
    }

    @Override
    public int compareTo(MemorySize that) {
        return Long.compare(this.bytes, that.bytes);
    }

    // ------------------------------------------------------------------------
    //  计算操作
    // ------------------------------------------------------------------------

    /**
     * 将此内存大小与另一个内存大小相加。
     *
     * @param that 要添加的内存大小
     * @return 相加后的新 MemorySize
     * @throws ArithmeticException 如果结果溢出
     */
    public MemorySize add(MemorySize that) {
        return new MemorySize(Math.addExact(this.bytes, that.bytes));
    }

    /**
     * 从此内存大小中减去另一个内存大小。
     *
     * @param that 要减去的内存大小
     * @return 相减后的新 MemorySize
     * @throws ArithmeticException 如果结果溢出或下溢
     */
    public MemorySize subtract(MemorySize that) {
        return new MemorySize(Math.subtractExact(this.bytes, that.bytes));
    }

    /**
     * 将此内存大小乘以一个倍数。
     *
     * @param multiplier 乘数,必须 >= 0
     * @return 相乘后的新 MemorySize
     * @throws ArithmeticException 如果结果溢出
     */
    public MemorySize multiply(double multiplier) {
        checkArgument(multiplier >= 0, "multiplier must be >= 0");

        BigDecimal product =
                BigDecimal.valueOf(this.bytes).multiply(BigDecimal.valueOf(multiplier));
        if (product.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
            throw new ArithmeticException("long overflow");
        }
        return new MemorySize(product.longValue());
    }

    /**
     * 将此内存大小除以一个除数。
     *
     * @param by 除数,必须 >= 0
     * @return 相除后的新 MemorySize
     */
    public MemorySize divide(long by) {
        checkArgument(by >= 0, "divisor must be >= 0");
        return new MemorySize(bytes / by);
    }

    // ------------------------------------------------------------------------
    //  解析
    // ------------------------------------------------------------------------

    /**
     * 将给定的字符串解析为 MemorySize。
     *
     * @param text 要解析的字符串
     * @return 解析后的 MemorySize
     * @throws IllegalArgumentException 如果表达式无法解析
     */
    public static MemorySize parse(String text) throws IllegalArgumentException {
        return new MemorySize(parseBytes(text));
    }

    /**
     * 使用默认单位解析给定的字符串。
     *
     * @param text 要解析的字符串
     * @param defaultUnit 指定默认单位
     * @return 解析后的 MemorySize
     * @throws IllegalArgumentException 如果表达式无法解析
     */
    public static MemorySize parse(String text, MemoryUnit defaultUnit)
            throws IllegalArgumentException {
        if (!hasUnit(text)) {
            return parse(text + defaultUnit.getUnits()[0]);
        }

        return parse(text);
    }

    /**
     * 将给定的字符串解析为字节数。支持的表达式列在 {@link MemorySize} 下。
     *
     * @param text 要解析的字符串
     * @return 解析后的大小,以字节为单位
     * @throws IllegalArgumentException 如果表达式无法解析
     */
    public static long parseBytes(String text) throws IllegalArgumentException {
        checkNotNull(text, "text");

        final String trimmed = text.trim();
        checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

        final int len = trimmed.length();
        int pos = 0;

        char current;
        while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
            pos++;
        }

        final String number = trimmed.substring(0, pos);
        final String unit = trimmed.substring(pos).trim().toLowerCase(Locale.US);

        if (number.isEmpty()) {
            throw new NumberFormatException("text does not start with a number");
        }

        final long value;
        try {
            value = Long.parseLong(number); // 这会在溢出时抛出 NumberFormatException
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "The value '"
                            + number
                            + "' cannot be re represented as 64bit number (numeric overflow).");
        }

        final long multiplier = parseUnit(unit).map(MemoryUnit::getMultiplier).orElse(1L);
        final long result = value * multiplier;

        // 检查溢出
        if (result / multiplier != value) {
            throw new IllegalArgumentException(
                    "The value '"
                            + text
                            + "' cannot be re represented as 64bit number of bytes (numeric overflow).");
        }

        return result;
    }

    private static Optional<MemoryUnit> parseUnit(String unit) {
        if (matchesAny(unit, BYTES)) {
            return Optional.of(BYTES);
        } else if (matchesAny(unit, KILO_BYTES)) {
            return Optional.of(KILO_BYTES);
        } else if (matchesAny(unit, MEGA_BYTES)) {
            return Optional.of(MEGA_BYTES);
        } else if (matchesAny(unit, GIGA_BYTES)) {
            return Optional.of(GIGA_BYTES);
        } else if (matchesAny(unit, TERA_BYTES)) {
            return Optional.of(TERA_BYTES);
        } else if (!unit.isEmpty()) {
            throw new IllegalArgumentException(
                    "Memory size unit '"
                            + unit
                            + "' does not match any of the recognized units: "
                            + MemoryUnit.getAllUnits());
        }

        return Optional.empty();
    }

    private static boolean matchesAny(String str, MemoryUnit unit) {
        for (String s : unit.getUnits()) {
            if (s.equals(str)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 内存单位枚举,主要用于从配置文件中解析值。
     *
     * <p>为了使较大的值更紧凑,支持常见的大小后缀:
     *
     * <ul>
     *   <li>1b 或 1bytes (字节)
     *   <li>1k 或 1kb 或 1kibibytes (解释为 kibibytes = 1024 字节)
     *   <li>1m 或 1mb 或 1mebibytes (解释为 mebibytes = 1024 kibibytes)
     *   <li>1g 或 1gb 或 1gibibytes (解释为 gibibytes = 1024 mebibytes)
     *   <li>1t 或 1tb 或 1tebibytes (解释为 tebibytes = 1024 gibibytes)
     * </ul>
     */
    public enum MemoryUnit {
        /** 字节单位 */
        BYTES(new String[] {"b", "bytes"}, 1L),
        /** Kibibytes 单位 (1024 bytes) */
        KILO_BYTES(new String[] {"k", "kb", "kibibytes"}, 1024L),
        /** Mebibytes 单位 (1024 KiB) */
        MEGA_BYTES(new String[] {"m", "mb", "mebibytes"}, 1024L * 1024L),
        /** Gibibytes 单位 (1024 MiB) */
        GIGA_BYTES(new String[] {"g", "gb", "gibibytes"}, 1024L * 1024L * 1024L),
        /** Tebibytes 单位 (1024 GiB) */
        TERA_BYTES(new String[] {"t", "tb", "tebibytes"}, 1024L * 1024L * 1024L * 1024L);

        /** 该单位的字符串表示数组 */
        private final String[] units;

        /** 转换为字节的乘数 */
        private final long multiplier;

        MemoryUnit(String[] units, long multiplier) {
            this.units = units;
            this.multiplier = multiplier;
        }

        /**
         * 获取该单位的字符串表示数组。
         *
         * @return 单位字符串数组
         */
        public String[] getUnits() {
            return units;
        }

        /**
         * 获取转换为字节的乘数。
         *
         * @return 乘数值
         */
        public long getMultiplier() {
            return multiplier;
        }

        /**
         * 获取所有单位的字符串表示。
         *
         * @return 所有单位的连接字符串
         */
        public static String getAllUnits() {
            return concatenateUnits(
                    BYTES.getUnits(),
                    KILO_BYTES.getUnits(),
                    MEGA_BYTES.getUnits(),
                    GIGA_BYTES.getUnits(),
                    TERA_BYTES.getUnits());
        }

        /**
         * 检查给定的文本是否包含单位。
         *
         * @param text 要检查的文本
         * @return 如果包含单位返回 true,否则返回 false
         */
        public static boolean hasUnit(String text) {
            checkNotNull(text, "text");

            final String trimmed = text.trim();
            checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

            final int len = trimmed.length();
            int pos = 0;

            char current;
            while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
                pos++;
            }

            final String unit = trimmed.substring(pos).trim().toLowerCase(Locale.US);

            return unit.length() > 0;
        }

        /**
         * 连接多个单位数组为一个字符串。
         *
         * @param allUnits 所有单位数组
         * @return 连接后的字符串
         */
        private static String concatenateUnits(final String[]... allUnits) {
            final StringBuilder builder = new StringBuilder(128);

            for (String[] units : allUnits) {
                builder.append('(');

                for (String unit : units) {
                    builder.append(unit);
                    builder.append(" | ");
                }

                builder.setLength(builder.length() - 3);
                builder.append(") / ");
            }

            builder.setLength(builder.length() - 3);
            return builder.toString();
        }
    }
}
