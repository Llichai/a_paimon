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

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 数据格式测试工具类。
 *
 * <p>提供各种数据格式测试所需的辅助方法,包括:
 * <ul>
 *   <li>数据格式字符串化:将复杂数据结构转换为可读字符串</li>
 *   <li>测试数据生成:创建各种大小和布局的测试数据</li>
 *   <li>内存段操作:跨段分割和重组测试数据</li>
 * </ul>
 *
 * <h2>主要功能</h2>
 *
 * <h3>1. 字符串化方法</h3>
 * <ul>
 *   <li>{@link #toStringNoRowKind(InternalRow, RowType)} - 转换行数据(不含RowKind)</li>
 *   <li>{@link #toStringWithRowKind(InternalRow, RowType)} - 转换行数据(完整支持)</li>
 *   <li>{@link #getDataFieldString(Object, DataType)} - 递归转换任意类型字段</li>
 *   <li>{@link #internalRowToString(InternalRow, RowType)} - 包含RowKind的完整转换</li>
 * </ul>
 *
 * <h3>2. 测试数据生成</h3>
 * <ul>
 *   <li>{@link #get24BytesBinaryRow()} - 生成24字节的BinaryRow</li>
 *   <li>{@link #get160BytesBinaryRow()} - 生成160字节的BinaryRow</li>
 *   <li>{@link #getMultiSeg160BytesBinaryRow(BinaryRow)} - 生成跨6段的160字节BinaryRow</li>
 *   <li>{@link #getMultiSeg160BytesInOneSegRow(BinaryRow)} - 生成单段+空段的BinaryRow</li>
 * </ul>
 *
 * <h3>3. 内存操作工具</h3>
 * <ul>
 *   <li>{@link #splitBytes(byte[], int)} - 将字节数组分割到两个内存段</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 字符串化测试
 * RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
 * InternalRow row = GenericRow.of(42, BinaryString.fromString("test"));
 *
 * String str1 = DataFormatTestUtil.toStringNoRowKind(row, rowType);
 * // 输出: "42, test"
 *
 * String str2 = DataFormatTestUtil.internalRowToString(row, rowType);
 * // 输出: "+I[42, test]"
 *
 * // 2. 生成测试数据
 * BinaryRow row24 = DataFormatTestUtil.get24BytesBinaryRow();
 * BinaryRow row160 = DataFormatTestUtil.get160BytesBinaryRow();
 *
 * // 3. 生成跨段数据(用于测试跨段场景)
 * BinaryRow multiSegRow = DataFormatTestUtil.getMultiSeg160BytesBinaryRow(row160);
 *
 * // 4. 分割字节数组
 * byte[] data = "Hello, World!".getBytes();
 * MemorySegment[] segments = DataFormatTestUtil.splitBytes(data, 8);
 * }</pre>
 *
 * <h2>测试场景</h2>
 * <table border="1">
 *   <tr>
 *     <th>场景</th>
 *     <th>方法</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>单段数据</td>
 *     <td>get24BytesBinaryRow()</td>
 *     <td>小数据,全在一个段内</td>
 *   </tr>
 *   <tr>
 *     <td>单段数据</td>
 *     <td>get160BytesBinaryRow()</td>
 *     <td>大数据,全在一个段内</td>
 *   </tr>
 *   <tr>
 *     <td>跨段数据</td>
 *     <td>getMultiSeg160BytesBinaryRow()</td>
 *     <td>数据跨越6个段,测试段边界</td>
 *   </tr>
 *   <tr>
 *     <td>混合段</td>
 *     <td>getMultiSeg160BytesInOneSegRow()</td>
 *     <td>一个有效段+一个空段</td>
 *   </tr>
 * </table>
 */
public class DataFormatTestUtil {

    /**
     * 将InternalRow转换为字符串(不含RowKind)。
     *
     * <p>仅转换字段值,不包含RowKind信息。支持基本类型和数组类型。
     *
     * @param row 要转换的行
     * @param type 行的类型信息
     * @return 字符串表示,格式: "field1, field2, ..."
     */
    public static String toStringNoRowKind(InternalRow row, RowType type) {
        StringBuilder build = new StringBuilder();
        for (int i = 0; i < type.getFieldCount(); i++) {
            if (i != 0) {
                build.append(", ");
            }
            if (row.isNullAt(i)) {
                build.append("NULL");
            } else {
                InternalRow.FieldGetter fieldGetter =
                        InternalRow.createFieldGetter(type.getTypeAt(i), i);
                Object field = fieldGetter.getFieldOrNull(row);
                if (field instanceof byte[]) {
                    build.append(Arrays.toString((byte[]) field));
                } else if (field instanceof InternalArray) {
                    InternalArray internalArray = (InternalArray) field;
                    ArrayType arrayType = (ArrayType) type.getTypeAt(i);
                    InternalArray.ElementGetter elementGetter =
                            InternalArray.createElementGetter(arrayType.getElementType());
                    String[] result = new String[internalArray.size()];
                    for (int j = 0; j < internalArray.size(); j++) {
                        Object object = elementGetter.getElementOrNull(internalArray, j);
                        result[j] = null == object ? null : object.toString();
                    }
                    build.append(Arrays.toString(result));
                } else {
                    build.append(field);
                }
            }
        }
        return build.toString();
    }

    /**
     * 将InternalRow转换为字符串(支持ArrayType、RowType和MapType)。
     *
     * <p>完整支持所有复杂类型的递归转换。
     *
     * @param row 要转换的行
     * @param type 行的类型信息
     * @return 字符串表示
     */
    public static String toStringWithRowKind(InternalRow row, RowType type) {
        StringBuilder build = new StringBuilder();
        for (int i = 0; i < type.getFieldCount(); i++) {
            if (i != 0) {
                build.append(", ");
            }
            if (row.isNullAt(i)) {
                build.append("NULL");
            } else {
                InternalRow.FieldGetter fieldGetter =
                        InternalRow.createFieldGetter(type.getTypeAt(i), i);
                Object field = fieldGetter.getFieldOrNull(row);
                build.append(getDataFieldString(field, type.getTypeAt(i)));
            }
        }
        return build.toString();
    }

    /**
     * 将数据字段转换为字符串,支持ArrayType、RowType和MapType的递归转换。
     *
     * <p>这是一个通用的字符串化方法,可以处理任意嵌套的复杂类型。
     *
     * <h3>支持的类型</h3>
     * <ul>
     *   <li>byte[] - 转换为数组字符串表示</li>
     *   <li>InternalArray - 递归转换数组元素</li>
     *   <li>InternalRow - 递归转换行字段</li>
     *   <li>InternalMap - 递归转换键值对</li>
     *   <li>其他类型 - 使用toString()方法</li>
     * </ul>
     *
     * @param field 要转换的字段值
     * @param type 字段的数据类型
     * @return 字符串表示
     */
    public static String getDataFieldString(Object field, DataType type) {
        if (field instanceof byte[]) {
            return Arrays.toString((byte[]) field);
        } else if (field instanceof InternalArray) {
            InternalArray internalArray = (InternalArray) field;
            ArrayType arrayType = (ArrayType) type;
            InternalArray.ElementGetter elementGetter =
                    InternalArray.createElementGetter(arrayType.getElementType());
            String[] result = new String[internalArray.size()];
            for (int j = 0; j < internalArray.size(); j++) {
                Object object = elementGetter.getElementOrNull(internalArray, j);
                result[j] =
                        null == object
                                ? null
                                : getDataFieldString(object, arrayType.getElementType());
            }
            return Arrays.toString(result);
        } else if (field instanceof InternalRow) {
            return String.format("(%s)", toStringWithRowKind((InternalRow) field, (RowType) type));
        } else if (field instanceof InternalMap) {
            InternalMap internalMap = (InternalMap) field;
            MapType mapType = (MapType) type;

            InternalArray keyArray = internalMap.keyArray();
            InternalArray.ElementGetter keyElementGetter =
                    InternalArray.createElementGetter(mapType.getKeyType());
            InternalArray valueArray = internalMap.valueArray();
            InternalArray.ElementGetter valueElementGetter =
                    InternalArray.createElementGetter(mapType.getValueType());

            StringBuilder mapBuild = new StringBuilder();
            mapBuild.append("{");
            for (int j = 0; j < internalMap.size(); j++) {
                if (j != 0) {
                    mapBuild.append(", ");
                }
                mapBuild.append(
                                getDataFieldString(
                                        keyElementGetter.getElementOrNull(keyArray, j),
                                        mapType.getKeyType()))
                        .append("=")
                        .append(
                                getDataFieldString(
                                        valueElementGetter.getElementOrNull(valueArray, j),
                                        mapType.getValueType()));
            }
            mapBuild.append("}");
            return mapBuild.toString();
        } else {
            return field.toString();
        }
    }

    /**
     * 将InternalRow转换为完整字符串(包含RowKind)。
     *
     * <p>输出格式: RowKind[field1, field2, ...]
     * <p>例如: "+I[42, test]" 表示INSERT操作
     *
     * @param row 要转换的行
     * @param type 行的类型信息
     * @return 包含RowKind的字符串表示
     */
    public static String internalRowToString(InternalRow row, RowType type) {
        return row.getRowKind().shortString() + "[" + toStringNoRowKind(row, type) + ']';
    }

    /**
     * 生成一个24字节的BinaryRow用于测试。
     *
     * <p>布局:
     * <ul>
     *   <li>8字节头部</li>
     *   <li>2个字符串字段,每个占8字节(固定长度部分)</li>
     *   <li>每个字符串包含2个随机数字字符</li>
     * </ul>
     *
     * @return 24字节的BinaryRow
     */
    public static BinaryRow get24BytesBinaryRow() {
        // header (8 bytes) + 2 * string in fixed-length part (8 bytes each)
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(StringUtils.randomNumericString(2)));
        writer.writeString(1, BinaryString.fromString(StringUtils.randomNumericString(2)));
        writer.complete();
        return row;
    }

    /**
     * 生成一个160字节的BinaryRow用于测试。
     *
     * <p>布局:
     * <ul>
     *   <li>8字节头部</li>
     *   <li>第1个字符串:72字节(8字节固定部分 + 72字节可变部分)</li>
     *   <li>第2个字符串:64字节(8字节固定部分 + 64字节可变部分)</li>
     *   <li>总计:8 + 8 + 72 + 8 + 64 = 160字节</li>
     * </ul>
     *
     * @return 160字节的BinaryRow
     */
    public static BinaryRow get160BytesBinaryRow() {
        // header (8 bytes) +
        // 72 byte length string (8 bytes in fixed-length, 72 bytes in variable-length) +
        // 64 byte length string (8 bytes in fixed-length, 64 bytes in variable-length)
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(StringUtils.randomNumericString(72)));
        writer.writeString(1, BinaryString.fromString(StringUtils.randomNumericString(64)));
        writer.complete();
        return row;
    }

    /**
     * 生成一个跨越6个内存段的160字节BinaryRow。
     *
     * <p>将输入的160字节行数据分割存储在6个内存段中,用于测试跨段访问场景。
     *
     * <p>布局策略:
     * <ul>
     *   <li>每个段大小32字节</li>
     *   <li>数据从第一个段的偏移8开始</li>
     *   <li>数据跨越段边界,需要处理边界情况</li>
     * </ul>
     *
     * @param row160 原始的160字节BinaryRow
     * @return 跨6个段的BinaryRow,内容与输入相同
     */
    public static BinaryRow getMultiSeg160BytesBinaryRow(BinaryRow row160) {
        BinaryRow multiSegRow160 = new BinaryRow(2);
        MemorySegment[] segments = new MemorySegment[6];
        int baseOffset = 8;
        int posInSeg = baseOffset;
        int remainSize = 160;
        for (int i = 0; i < segments.length; i++) {
            segments[i] = MemorySegment.wrap(new byte[32]);
            int copy = Math.min(32 - posInSeg, remainSize);
            row160.getSegments()[0].copyTo(160 - remainSize, segments[i], posInSeg, copy);
            remainSize -= copy;
            posInSeg = 0;
        }
        multiSegRow160.pointTo(segments, baseOffset, 160);
        assertThat(multiSegRow160).isEqualTo(row160);
        return multiSegRow160;
    }

    /**
     * 生成一个由2个内存段组成的BinaryRow,但所有数据都在第一个段中。
     *
     * <p>第一个段包含完整的160字节数据(与输入相同),第二个段为空。
     * 用于测试多段但实际单段使用的场景。
     *
     * @param row160 原始的160字节BinaryRow
     * @return 包含2个段的BinaryRow,第一个段有数据,第二个段为空
     */
    public static BinaryRow getMultiSeg160BytesInOneSegRow(BinaryRow row160) {
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = row160.getSegments()[0];
        segments[1] = MemorySegment.wrap(new byte[row160.getSegments()[0].size()]);
        row160.pointTo(segments, 0, row160.getSizeInBytes());
        return row160;
    }

    /**
     * 将字节数组分割到两个内存段中。
     *
     * <p>分割策略:
     * <ul>
     *   <li>每个段大小: (bytes.length + 1) / 2 + baseOffset</li>
     *   <li>第一个段从baseOffset开始存储前半部分数据</li>
     *   <li>第二个段从0开始存储后半部分数据</li>
     * </ul>
     *
     * @param bytes 要分割的字节数组
     * @param baseOffset 第一个段的起始偏移量
     * @return 包含2个MemorySegment的数组
     */
    public static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
        int newSize = (bytes.length + 1) / 2 + baseOffset;
        MemorySegment[] ret = new MemorySegment[2];
        ret[0] = MemorySegment.wrap(new byte[newSize]);
        ret[1] = MemorySegment.wrap(new byte[newSize]);

        ret[0].put(baseOffset, bytes, 0, newSize - baseOffset);
        ret[1].put(0, bytes, newSize - baseOffset, bytes.length - (newSize - baseOffset));
        return ret;
    }

    /**
     * 用于测试泛型类型获取/设置的简单类。
     *
     * <p>包含两个字段:
     * <ul>
     *   <li>i: int类型整数</li>
     *   <li>j: double类型浮点数</li>
     * </ul>
     */
    public static class MyObj {
        /** 整数字段。 */
        public int i;
        /** 浮点数字段。 */
        public double j;

        /**
         * 构造MyObj对象。
         *
         * @param i 整数值
         * @param j 浮点数值
         */
        public MyObj(int i, double j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyObj myObj = (MyObj) o;

            return i == myObj.i && Double.compare(myObj.j, j) == 0;
        }

        @Override
        public String toString() {
            return "MyObj{" + "i=" + i + ", j=" + j + '}';
        }
    }
}
