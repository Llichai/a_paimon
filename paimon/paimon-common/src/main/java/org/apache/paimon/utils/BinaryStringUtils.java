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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.types.DataTypeRoot.BINARY;
import static org.apache.paimon.types.DataTypeRoot.CHAR;

/**
 * {@link BinaryString} 的工具类。
 *
 * <p>提供二进制字符串与各种数据类型之间的转换功能,包括:
 * <ul>
 *   <li>字符串到布尔值的转换
 *   <li>字符串到数值类型(int, long, short, byte, float, double)的转换
 *   <li>字符串到日期时间类型的转换
 *   <li>字符串的连接、分割等操作
 * </ul>
 */
public class BinaryStringUtils {

    /** NULL 字符串常量 */
    public static final BinaryString NULL_STRING = fromString("NULL");

    /** TRUE 字符串常量 */
    public static final BinaryString TRUE_STRING = fromString("TRUE");

    /** FALSE 字符串常量 */
    public static final BinaryString FALSE_STRING = fromString("FALSE");

    /** 空字符串数组常量 */
    public static final BinaryString[] EMPTY_STRING_ARRAY = new BinaryString[0];
    /** 表示 true 的字符串列表 */
    private static final List<BinaryString> TRUE_STRINGS =
            Stream.of("t", "true", "y", "yes", "1")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    /** 表示 false 的字符串列表 */
    private static final List<BinaryString> FALSE_STRINGS =
            Stream.of("f", "false", "n", "no", "0")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    /**
     * 从二进制字符串中获取临时字节数组。
     *
     * @param str 二进制字符串
     * @param sizeInBytes 字节大小
     * @return 字节数组
     */
    private static byte[] getTmpBytes(BinaryString str, int sizeInBytes) {
        byte[] bytes = MemorySegmentUtils.allocateReuseBytes(sizeInBytes);
        MemorySegmentUtils.copyToBytes(str.getSegments(), str.getOffset(), bytes, 0, sizeInBytes);
        return bytes;
    }

    /**
     * 将 {@link BinaryString} 解析为布尔值。
     *
     * @param str 要解析的二进制字符串
     * @return 解析后的布尔值
     * @throws RuntimeException 如果字符串无法解析为布尔值
     */
    public static boolean toBoolean(BinaryString str) {
        BinaryString lowerCase = str.toLowerCase();
        if (TRUE_STRINGS.contains(lowerCase)) {
            return true;
        }
        if (FALSE_STRINGS.contains(lowerCase)) {
            return false;
        }
        throw new RuntimeException("Cannot parse '" + str + "' as BOOLEAN.");
    }

    /**
     * 将 BinaryString 解析为 Long 值。
     *
     * <p>注意:在此方法中,我们以负数格式累积结果,如果字符串不以 '-' 开头,
     * 则在最后将其转换为正数格式。这是因为最小值的位数大于最大值,
     * 例如 Long.MAX_VALUE 是 '9223372036854775807',而 Long.MIN_VALUE 是 '-9223372036854775808'。
     *
     * <p>此代码主要复制自 Hive 中的 LazyLong.parseLong。
     *
     * @param str 要解析的二进制字符串
     * @return 解析后的 long 值
     * @throws NumberFormatException 如果字符串无法解析为 long
     */
    public static long toLong(BinaryString str) throws NumberFormatException {
        int sizeInBytes = str.getSizeInBytes();
        byte[] tmpBytes = getTmpBytes(str, sizeInBytes);
        if (sizeInBytes == 0) {
            throw numberFormatExceptionFor(str, "Input is empty.");
        }
        int i = 0;

        byte b = tmpBytes[i];
        final boolean negative = b == '-';
        if (negative || b == '+') {
            i++;
            if (sizeInBytes == 1) {
                throw numberFormatExceptionFor(str, "Input has only positive or negative symbol.");
            }
        }

        long result = 0;
        final byte separator = '.';
        final int radix = 10;
        final long stopValue = Long.MIN_VALUE / radix;
        while (i < sizeInBytes) {
            b = tmpBytes[i];
            i++;
            if (b == separator) {
                // We allow decimals and will return a truncated integral in that case.
                // Therefore we won't throw an exception here (checking the fractional
                // part happens below.)
                break;
            }

            int digit;
            if (b >= '0' && b <= '9') {
                digit = b - '0';
            } else {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }

            // We are going to process the new digit and accumulate the result. However, before
            // doing this, if the result is already smaller than the
            // stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely be smaller
            // than minValue, and we can stop.
            if (result < stopValue) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }

            result = result * radix - digit;
            // Since the previous result is less than or equal to
            // stopValue(Long.MIN_VALUE / radix), we can just use `result > 0` to check overflow.
            // If result overflows, we should stop.
            if (result > 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }

        // This is the case when we've encountered a decimal separator. The fractional
        // part will not change the number, but we will verify that the fractional part
        // is well formed.
        while (i < sizeInBytes) {
            byte currentByte = tmpBytes[i];
            if (currentByte < '0' || currentByte > '9') {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }
            i++;
        }

        if (!negative) {
            result = -result;
            if (result < 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }
        return result;
    }

    /**
     * 将 BinaryString 解析为 Int 值。
     *
     * <p>注意:在此方法中,我们以负数格式累积结果,如果字符串不以 '-' 开头,
     * 则在最后将其转换为正数格式。这是因为最小值的位数大于最大值,
     * 例如 Integer.MAX_VALUE 是 '2147483647',而 Integer.MIN_VALUE 是 '-2147483648'。
     *
     * <p>此代码主要复制自 Hive 中的 LazyInt.parseInt。
     *
     * <p>注意:此方法与 `toLong` 几乎相同,但为了性能原因,我们保持它的重复,
     * 就像 Hive 那样。
     *
     * @param str 要解析的二进制字符串
     * @return 解析后的 int 值
     * @throws NumberFormatException 如果字符串无法解析为 int
     */
    public static int toInt(BinaryString str) throws NumberFormatException {
        int sizeInBytes = str.getSizeInBytes();
        byte[] tmpBytes = getTmpBytes(str, sizeInBytes);
        if (sizeInBytes == 0) {
            throw numberFormatExceptionFor(str, "Input is empty.");
        }
        int i = 0;

        byte b = tmpBytes[i];
        final boolean negative = b == '-';
        if (negative || b == '+') {
            i++;
            if (sizeInBytes == 1) {
                throw numberFormatExceptionFor(str, "Input has only positive or negative symbol.");
            }
        }

        int result = 0;
        final byte separator = '.';
        final int radix = 10;
        final long stopValue = Integer.MIN_VALUE / radix;
        while (i < sizeInBytes) {
            b = tmpBytes[i];
            i++;
            if (b == separator) {
                // We allow decimals and will return a truncated integral in that case.
                // Therefore we won't throw an exception here (checking the fractional
                // part happens below.)
                break;
            }

            int digit;
            if (b >= '0' && b <= '9') {
                digit = b - '0';
            } else {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }

            // We are going to process the new digit and accumulate the result. However, before
            // doing this, if the result is already smaller than the
            // stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely be smaller
            // than minValue, and we can stop.
            if (result < stopValue) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }

            result = result * radix - digit;
            // Since the previous result is less than or equal to
            // stopValue(Long.MIN_VALUE / radix), we can just use `result > 0` to check overflow.
            // If result overflows, we should stop.
            if (result > 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }

        // This is the case when we've encountered a decimal separator. The fractional
        // part will not change the number, but we will verify that the fractional part
        // is well formed.
        while (i < sizeInBytes) {
            byte currentByte = tmpBytes[i];
            if (currentByte < '0' || currentByte > '9') {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }
            i++;
        }

        if (!negative) {
            result = -result;
            if (result < 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }
        return result;
    }

    /**
     * 将 BinaryString 解析为 Short 值。
     *
     * @param str 要解析的二进制字符串
     * @return 解析后的 short 值
     * @throws NumberFormatException 如果字符串无法解析为 short 或溢出
     */
    public static short toShort(BinaryString str) throws NumberFormatException {
        int intValue = toInt(str);
        short result = (short) intValue;
        if (result == intValue) {
            return result;
        }
        throw numberFormatExceptionFor(str, "Overflow.");
    }

    /**
     * 将 BinaryString 解析为 Byte 值。
     *
     * @param str 要解析的二进制字符串
     * @return 解析后的 byte 值
     * @throws NumberFormatException 如果字符串无法解析为 byte 或溢出
     */
    public static byte toByte(BinaryString str) throws NumberFormatException {
        int intValue = toInt(str);
        byte result = (byte) intValue;
        if (result == intValue) {
            return result;
        }
        throw numberFormatExceptionFor(str, "Overflow.");
    }

    /**
     * 将 BinaryString 解析为 Double 值。
     *
     * @param str 要解析的二进制字符串
     * @return 解析后的 double 值
     * @throws NumberFormatException 如果字符串无法解析为 double
     */
    public static double toDouble(BinaryString str) throws NumberFormatException {
        return Double.parseDouble(str.toString());
    }

    /**
     * 将 BinaryString 解析为 Float 值。
     *
     * @param str 要解析的二进制字符串
     * @return 解析后的 float 值
     * @throws NumberFormatException 如果字符串无法解析为 float
     */
    public static float toFloat(BinaryString str) throws NumberFormatException {
        return Float.parseFloat(str.toString());
    }

    /**
     * 创建数字格式异常。
     *
     * @param input 输入字符串
     * @param reason 异常原因
     * @return NumberFormatException 实例
     */
    private static NumberFormatException numberFormatExceptionFor(
            BinaryString input, String reason) {
        return new NumberFormatException("For input string: '" + input + "'. " + reason);
    }

    /**
     * 将 BinaryString 转换为日期(天数)。
     *
     * @param input 输入字符串
     * @return 日期值(从纪元开始的天数)
     * @throws DateTimeException 如果无法解析日期
     */
    public static int toDate(BinaryString input) throws DateTimeException {
        String str = input.toString();
        if (StringUtils.isNumeric(str)) {
            // for Integer.toString conversion
            return toInt(input);
        }
        Integer date = DateTimeUtils.parseDate(str);
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    /**
     * 将 BinaryString 转换为时间(毫秒)。
     *
     * @param input 输入字符串
     * @return 时间值(从午夜开始的毫秒数)
     * @throws DateTimeException 如果无法解析时间
     */
    public static int toTime(BinaryString input) throws DateTimeException {
        String str = input.toString();
        if (StringUtils.isNumeric(str)) {
            // for Integer.toString conversion
            return toInt(input);
        }
        Integer date = DateTimeUtils.parseTime(str);
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    /**
     * 用于 {@code CAST(x as TIMESTAMP)} 操作。
     *
     * @param input 输入字符串
     * @param precision 时间戳精度
     * @return Timestamp 实例
     * @throws DateTimeException 如果无法解析时间戳
     */
    public static Timestamp toTimestamp(BinaryString input, int precision)
            throws DateTimeException {
        if (StringUtils.isNumeric(input.toString())) {
            long epoch = toLong(input);
            return fromMillisToTimestamp(epoch, precision);
        }
        return DateTimeUtils.parseTimestampData(input.toString(), precision);
    }

    /**
     * 用于 {@code CAST(x as TIMESTAMP_LTZ)} 操作。
     *
     * @param input 输入字符串
     * @param precision 时间戳精度
     * @param timeZone 时区
     * @return Timestamp 实例
     * @throws DateTimeException 如果无法解析时间戳
     */
    public static Timestamp toTimestamp(BinaryString input, int precision, TimeZone timeZone)
            throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision, timeZone);
    }

    /**
     * 将纪元毫秒转换为具有指定精度的时间戳的辅助方法。
     *
     * @param epoch 纪元值
     * @param precision 精度(0=秒, 3=毫秒, 6=微秒, 9=纳秒)
     * @return Timestamp 实例
     */
    private static Timestamp fromMillisToTimestamp(long epoch, int precision) {
        // 根据精度从纪元计算毫秒和纳秒
        long millis;
        int nanosOfMillis;

        switch (precision) {
            case 0: // 秒
                millis = epoch * 1000;
                nanosOfMillis = 0;
                break;
            case 3: // 毫秒
                millis = epoch;
                nanosOfMillis = 0;
                break;
            case 6: // 微秒
                millis = epoch / 1000;
                nanosOfMillis = (int) ((epoch % 1000) * 1000);
                break;
            case 9: // 纳秒
                millis = epoch / 1_000_000;
                nanosOfMillis = (int) (epoch % 1_000_000);
                break;
            default:
                throw new RuntimeException("Unsupported precision: " + precision);
        }

        // 如果纳秒为负数,则减去一毫秒
        // 并向前计算纳秒偏移量
        // 因为纳秒应该始终是毫秒之上的正偏移量
        if (nanosOfMillis < 0) {
            nanosOfMillis = 1000000 + nanosOfMillis;
            millis -= 1;
        }

        return Timestamp.fromEpochMillis(millis, nanosOfMillis);
    }

    /**
     * 将字符串数据转换为字符类型(CHAR/VARCHAR)。
     *
     * @param strData 字符串数据
     * @param type 目标数据类型
     * @return 转换后的字符串
     */
    public static BinaryString toCharacterString(BinaryString strData, DataType type) {
        final boolean targetCharType = type.getTypeRoot() == CHAR;
        final int targetLength = DataTypeChecks.getLength(type);
        if (strData.numChars() > targetLength) {
            return strData.substring(0, targetLength);
        } else if (strData.numChars() < targetLength && targetCharType) {
            int padLength = targetLength - strData.numChars();
            BinaryString padString = BinaryString.blankString(padLength);
            return concat(strData, padString);
        }
        return strData;
    }

    /**
     * 将字节数组转换为二进制字符串类型(BINARY/VARBINARY)。
     *
     * @param byteArrayTerm 字节数组
     * @param type 目标数据类型
     * @return 转换后的字节数组
     */
    public static byte[] toBinaryString(byte[] byteArrayTerm, DataType type) {
        final boolean targetBinaryType = type.getTypeRoot() == BINARY;
        final int targetLength = DataTypeChecks.getLength(type);
        if (byteArrayTerm.length == targetLength) {
            return byteArrayTerm;
        }
        if (targetBinaryType) {
            return Arrays.copyOf(byteArrayTerm, targetLength);
        } else {
            if (byteArrayTerm.length <= targetLength) {
                return byteArrayTerm;
            } else {
                return Arrays.copyOf(byteArrayTerm, targetLength);
            }
        }
    }

    /**
     * 将输入字符串连接成单个字符串。
     *
     * <p>如果任何参数为 NULL,则返回 NULL。
     *
     * @param inputs 要连接的字符串数组
     * @return 连接后的字符串,如果任何输入为 null 则返回 null
     */
    public static BinaryString concat(BinaryString... inputs) {
        return concat(Arrays.asList(inputs));
    }

    /**
     * 将输入字符串连接成单个字符串。
     *
     * @param inputs 要连接的字符串可迭代对象
     * @return 连接后的字符串,如果任何输入为 null 则返回 null
     */
    public static BinaryString concat(Iterable<BinaryString> inputs) {
        // 计算结果的总长度
        int totalLength = 0;
        for (BinaryString input : inputs) {
            if (input == null) {
                return null;
            }

            totalLength += input.getSizeInBytes();
        }

        // 分配新的字节数组,并将输入逐个复制到其中
        final byte[] result = new byte[totalLength];
        int offset = 0;
        for (BinaryString input : inputs) {
            if (input != null) {
                int len = input.getSizeInBytes();
                MemorySegmentUtils.copyToBytes(
                        input.getSegments(), input.getOffset(), result, offset, len);
                offset += len;
            }
        }
        return BinaryString.fromBytes(result);
    }

    /**
     * 使用完整分隔符分割字符串,保留所有标记(包括空字符串)。
     *
     * @param str 要分割的字符串
     * @param delimiter 分隔符
     * @return 分割后的字符串数组
     */
    public static BinaryString[] splitByWholeSeparatorPreserveAllTokens(
            BinaryString str, BinaryString delimiter) {
        int sizeInBytes = str.getSizeInBytes();
        MemorySegment[] segments = str.getSegments();
        int offset = str.getOffset();

        if (sizeInBytes == 0) {
            return EMPTY_STRING_ARRAY;
        }

        if (delimiter == null || BinaryString.EMPTY_UTF8.equals(delimiter)) {
            // 以空白字符分割
            return splitByWholeSeparatorPreserveAllTokens(str, fromString(" "));
        }

        int sepSize = delimiter.getSizeInBytes();
        MemorySegment[] sepSegs = delimiter.getSegments();
        int sepOffset = delimiter.getOffset();

        final ArrayList<BinaryString> substrings = new ArrayList<>();
        int beg = 0;
        int end = 0;
        while (end < sizeInBytes) {
            end =
                    MemorySegmentUtils.find(
                                    segments,
                                    offset + beg,
                                    sizeInBytes - beg,
                                    sepSegs,
                                    sepOffset,
                                    sepSize)
                            - offset;

            if (end > -1) {
                if (end > beg) {

                    // 以下代码是可以的,因为 String.substring(beg, end)
                    // 排除了位置 'end' 处的字符
                    substrings.add(BinaryString.fromAddress(segments, offset + beg, end - beg));

                    // 设置下一次搜索的起点
                    // 以下等同于 beg = end + (separatorLength - 1) + 1,
                    // 这是正确的计算:
                } else {
                    // 我们找到了分隔符的连续出现
                    substrings.add(BinaryString.EMPTY_UTF8);
                }
                beg = end + sepSize;
            } else {
                // String.substring(beg) 从 'beg' 到字符串末尾
                substrings.add(BinaryString.fromAddress(segments, offset + beg, sizeInBytes - beg));
                end = sizeInBytes;
            }
        }

        return substrings.toArray(new BinaryString[0]);
    }
}
