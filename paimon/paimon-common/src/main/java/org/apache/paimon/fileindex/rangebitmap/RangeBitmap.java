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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fileindex.rangebitmap.dictionary.Dictionary;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.ChunkedDictionary;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.KeyFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;

/**
 * 范围位图索引实现。
 *
 * <p>结合字典编码和 BSI (Bit-Sliced Index) 的高级索引结构,支持任意可比较类型的范围查询。
 *
 * <h3>核心思想</h3>
 * <p>将原始值映射为连续的整数编码,然后使用 BSI 进行高效查询:
 * <pre>
 * 原始数据: ["apple", "banana", "cherry", "apple"]
 *
 * 1. 字典编码:
 *    Dictionary: {"apple": 0, "banana": 1, "cherry": 2}
 *    Encoded:    [0, 1, 2, 0]
 *
 * 2. BSI 索引:
 *    构建编码值的 BSI,支持范围查询
 *
 * 3. 查询 >= "banana":
 *    - 字典查找: "banana" -> code 1
 *    - BSI 查询: code >= 1 -> {1, 2}
 *    - 结果行号: {1, 2}
 * </pre>
 *
 * <h3>数据结构</h3>
 * <ul>
 *   <li>{@link Dictionary}: 存储唯一值到编码的映射,支持二分查找</li>
 *   <li>{@link BitSliceIndexBitmap}: 存储编码值的位切片索引</li>
 *   <li>min/max: 最小值和最大值,用于快速过滤</li>
 *   <li>cardinality: 唯一值数量</li>
 * </ul>
 *
 * <h3>文件格式</h3>
 * <pre>
 * +----------------+--------+------------+--------+
 * | Header Length  | Header | Dictionary | BSI    |
 * | 4 bytes        | 变长   | 变长       | 变长   |
 * +----------------+--------+------------+--------+
 *
 * Header 包括:
 *   - version (1 byte): 版本号
 *   - rid (4 bytes): 总行数
 *   - cardinality (4 bytes): 唯一值数量
 *   - min (变长): 最小值
 *   - max (变长): 最大值
 *   - dictionaryLength (4 bytes): 字典长度
 * </pre>
 *
 * <h3>查询优化</h3>
 * <ul>
 *   <li>min/max 过滤: 先检查查询范围是否与 [min, max] 相交</li>
 *   <li>字典查找: 使用二分查找定位编码值,时间复杂度 O(log D),D 为字典大小</li>
 *   <li>BSI 查询: 使用位运算优化范围查询,时间复杂度 O(log D)</li>
 *   <li>延迟加载: 字典和 BSI 按需加载,节省内存</li>
 * </ul>
 *
 * <h3>查询类型</h3>
 * <table border="1">
 *   <tr>
 *     <th>查询类型</th>
 *     <th>实现方式</th>
 *     <th>时间复杂度</th>
 *   </tr>
 *   <tr>
 *     <td>等值 (=)</td>
 *     <td>字典查找 + BSI.eq()</td>
 *     <td>O(log D)</td>
 *   </tr>
 *   <tr>
 *     <td>不等 (!=)</td>
 *     <td>eq() + 位图取反</td>
 *     <td>O(log D + n)</td>
 *   </tr>
 *   <tr>
 *     <td>大于 (>)</td>
 *     <td>字典查找 + BSI.gt()</td>
 *     <td>O(log D)</td>
 *   </tr>
 *   <tr>
 *     <td>范围 (BETWEEN)</td>
 *     <td>gte() AND lte()</td>
 *     <td>O(log D)</td>
 *   </tr>
 *   <tr>
 *     <td>IN</td>
 *     <td>多个 eq() 的 OR</td>
 *     <td>O(k * log D)</td>
 *   </tr>
 *   <tr>
 *     <td>TopK</td>
 *     <td>BSI.topK() + NULL 处理</td>
 *     <td>O(log D * log n)</td>
 *   </tr>
 * </table>
 *
 * <h3>NULL 值处理</h3>
 * <ul>
 *   <li>NULL 值不参与字典编码</li>
 *   <li>通过 BSI.isNotNull() 获取非 NULL 行,取反得到 NULL 行</li>
 *   <li>TopK/BottomK 支持 NULLS_FIRST 和 NULLS_LAST 语义</li>
 * </ul>
 *
 * <h3>适用场景</h3>
 * <ul>
 *   <li>字符串类型的范围查询和排序</li>
 *   <li>中低基数列(基数 < 10000)</li>
 *   <li>频繁的范围查询和 TopK 查询</li>
 *   <li>需要精确结果的场景</li>
 * </ul>
 *
 * <h3>与其他索引对比</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>RangeBitmap</th>
 *     <th>Bitmap</th>
 *     <th>BSI</th>
 *   </tr>
 *   <tr>
 *     <td>支持类型</td>
 *     <td>所有可比较类型</td>
 *     <td>所有类型</td>
 *     <td>仅数值类型</td>
 *   </tr>
 *   <tr>
 *     <td>查询类型</td>
 *     <td>等值、范围、TopK</td>
 *     <td>等值、IN</td>
 *     <td>等值、范围</td>
 *   </tr>
 *   <tr>
 *     <td>空间复杂度</td>
 *     <td>O(n * log D + D)</td>
 *     <td>O(n * D)</td>
 *     <td>O(n * log V)</td>
 *   </tr>
 * </table>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 写入索引
 * RangeBitmap.Appender appender = new RangeBitmap.Appender(keyFactory, limitSize);
 * appender.append("apple");   // row 0
 * appender.append("banana");  // row 1
 * appender.append("cherry");  // row 2
 * appender.append(null);      // row 3
 * byte[] serialized = appender.serialize();
 *
 * // 读取索引
 * RangeBitmap rangeBitmap = new RangeBitmap(inputStream, offset, keyFactory);
 *
 * // 等值查询: name = 'banana'
 * RoaringBitmap32 result1 = rangeBitmap.eq("banana");  // {1}
 *
 * // 范围查询: name >= 'banana'
 * RoaringBitmap32 result2 = rangeBitmap.gte("banana"); // {1, 2}
 *
 * // TopK 查询: ORDER BY name LIMIT 2
 * RoaringBitmap32 result3 = rangeBitmap.topK(2, NULLS_LAST, null, true); // {2, 1}
 * }</pre>
 *
 * @see Dictionary
 * @see BitSliceIndexBitmap
 * @see ChunkedDictionary
 */
/** Implementation of range-bitmap. */
public class RangeBitmap {

    /** 版本 1:当前版本。 */
    public static final int VERSION_1 = 1;

    /** 当前版本号。 */
    public static final byte CURRENT_VERSION = VERSION_1;

    /** 总行数 (包括 NULL 值)。 */
    private final int rid;

    /** 最小值 (不包括 NULL)。 */
    @Nullable private final Object min;

    /** 最大值 (不包括 NULL)。 */
    @Nullable private final Object max;

    /** 唯一值数量 (不包括 NULL)。 */
    private final int cardinality;

    /** 字典数据的偏移量。 */
    private final int dictionaryOffset;

    /** BSI 数据的偏移量。 */
    private final int bsiOffset;

    /** 可定位输入流。 */
    private final SeekableInputStream in;

    /** 键工厂,用于创建序列化/反序列化器和比较器。 */
    private final KeyFactory factory;

    /** 值比较器。 */
    private final Comparator<Object> comparator;

    /** 字典,延迟加载。 */
    private Dictionary dictionary;

    /** BSI 位图,延迟加载。 */
    private BitSliceIndexBitmap bsi;

    /**
     * 构造范围位图索引。
     *
     * <p>从输入流中读取头部信息,但不立即加载字典和 BSI 数据。
     *
     * @param in 可定位输入流
     * @param offset 索引数据的起始偏移量
     * @param factory 键工厂
     * @throws RuntimeException 如果读取失败或版本不兼容
     */
    public RangeBitmap(SeekableInputStream in, int offset, KeyFactory factory) {
        ByteBuffer headers;
        int headerLength;
        try {
            in.seek(offset);
            byte[] headerLengthInBytes = new byte[Integer.BYTES];
            IOUtils.readFully(in, headerLengthInBytes);
            headerLength = ByteBuffer.wrap(headerLengthInBytes).getInt();

            byte[] headerInBytes = new byte[headerLength];
            IOUtils.readFully(in, headerInBytes);
            headers = ByteBuffer.wrap(headerInBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KeyFactory.KeyDeserializer deserializer = factory.createDeserializer();
        byte version = headers.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException("Invalid version " + version);
        }
        this.rid = headers.getInt();
        this.cardinality = headers.getInt();
        this.min = cardinality <= 0 ? null : deserializer.deserialize(headers);
        this.max = cardinality <= 0 ? null : deserializer.deserialize(headers);
        int dictionaryLength = headers.getInt();

        this.dictionaryOffset = offset + Integer.BYTES + headerLength;
        this.bsiOffset = dictionaryOffset + dictionaryLength;

        this.in = in;
        this.factory = factory;
        this.comparator = factory.createComparator();
    }

    /**
     * 等值查询。
     *
     * <p>返回所有值等于 key 的行号。
     *
     * <h3>优化</h3>
     * <ul>
     *   <li>先检查 key 是否在 [min, max] 范围内</li>
     *   <li>如果所有值都相同,直接返回所有非 NULL 行</li>
     *   <li>否则通过字典查找编码,再查询 BSI</li>
     * </ul>
     *
     * @param key 查询值
     * @return 包含所有等于 key 的行号的位图
     */
    public RoaringBitmap32 eq(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin == 0 && compareMax == 0) {
            return isNotNull();
        } else if (compareMin < 0 || compareMax > 0) {
            return new RoaringBitmap32();
        }

        int code = getDictionary().find(key);
        if (code < 0) {
            return new RoaringBitmap32();
        }
        return getBitSliceIndexBitmap().eq(code);
    }

    /**
     * 不等查询。
     *
     * <p>返回所有值不等于 key 的行号 (不包括 NULL)。
     *
     * @param key 查询值
     * @return 包含所有不等于 key 的行号的位图
     */
    public RoaringBitmap32 neq(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }
        return not(eq(key));
    }

    /**
     * 小于等于查询。
     *
     * <p>返回所有值 <= key 的行号。
     *
     * @param key 查询值
     * @return 包含所有 <= key 的行号的位图
     */
    public RoaringBitmap32 lte(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMax >= 0) {
            return isNotNull();
        } else if (compareMin < 0) {
            return new RoaringBitmap32();
        }

        return not(gt(key));
    }

    /**
     * 小于查询。
     *
     * <p>返回所有值 < key 的行号。
     *
     * @param key 查询值
     * @return 包含所有 < key 的行号的位图
     */
    public RoaringBitmap32 lt(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMax > 0) {
            return isNotNull();
        } else if (compareMin <= 0) {
            return new RoaringBitmap32();
        }

        return not(gte(key));
    }

    /**
     * 大于等于查询。
     *
     * <p>返回所有值 >= key 的行号。
     *
     * <h3>算法</h3>
     * <ol>
     *   <li>检查 key 是否在 [min, max] 范围内</li>
     *   <li>使用字典查找 key 的编码值 code</li>
     *   <li>如果 code < 0(不存在),使用插入位置 -code-1</li>
     *   <li>调用 BSI.gte(code) 查询</li>
     * </ol>
     *
     * @param key 查询值
     * @return 包含所有 >= key 的行号的位图
     */
    public RoaringBitmap32 gte(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin <= 0) {
            return isNotNull();
        } else if (compareMax > 0) {
            return new RoaringBitmap32();
        }

        int code = getDictionary().find(key);
        return code < 0
                ? getBitSliceIndexBitmap().gte(-code - 1)
                : getBitSliceIndexBitmap().gte(code);
    }

    /**
     * 大于查询。
     *
     * <p>返回所有值 > key 的行号。
     *
     * @param key 查询值
     * @return 包含所有 > key 的行号的位图
     */
    public RoaringBitmap32 gt(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin < 0) {
            return isNotNull();
        } else if (compareMax >= 0) {
            return new RoaringBitmap32();
        }

        int code = getDictionary().find(key);
        return code < 0
                ? getBitSliceIndexBitmap().gte(-code - 1)
                : getBitSliceIndexBitmap().gt(code);
    }

    /**
     * IN 查询。
     *
     * <p>返回所有值在 keys 列表中的行号。
     *
     * @param keys 值列表
     * @return 包含所有在 keys 中的行号的位图
     */
    public RoaringBitmap32 in(List<Object> keys) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        RoaringBitmap32 bitmap = new RoaringBitmap32();
        for (Object key : keys) {
            bitmap.or(eq(key));
        }
        return bitmap;
    }

    /**
     * NOT IN 查询。
     *
     * <p>返回所有值不在 keys 列表中的行号 (不包括 NULL)。
     *
     * @param keys 值列表
     * @return 包含所有不在 keys 中的行号的位图
     */
    public RoaringBitmap32 notIn(List<Object> keys) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        return not(in(keys));
    }

    /**
     * IS NULL 查询。
     *
     * <p>返回所有 NULL 值的行号。
     *
     * @return 包含所有 NULL 行号的位图
     */
    public RoaringBitmap32 isNull() {
        return isNull(null);
    }

    /**
     * IS NOT NULL 查询。
     *
     * <p>返回所有非 NULL 值的行号。
     *
     * @return 包含所有非 NULL 行号的位图
     */
    public RoaringBitmap32 isNotNull() {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        return getBitSliceIndexBitmap().isNotNull();
    }

    private RoaringBitmap32 isNull(@Nullable RoaringBitmap32 foundSet) {
        if (cardinality <= 0) {
            return rid > 0 ? RoaringBitmap32.bitmapOf(0, rid - 1) : new RoaringBitmap32();
        }

        if (foundSet != null && foundSet.isEmpty()) {
            return foundSet;
        }

        RoaringBitmap32 bitmap = isNotNull();
        bitmap.flip(0, rid);
        if (foundSet != null) {
            bitmap.and(foundSet);
        }
        return bitmap;
    }

    public Object get(int position) {
        if (position < 0 || position >= rid) {
            return null;
        }
        Integer code = getBitSliceIndexBitmap().get(position);
        if (code == null) {
            return null;
        }
        return getDictionary().find(code);
    }

    private RoaringBitmap32 not(RoaringBitmap32 bitmap) {
        bitmap.flip(0, rid);
        bitmap.and(isNotNull());
        return bitmap;
    }

    private Dictionary getDictionary() {
        if (dictionary == null) {
            try {
                dictionary = new ChunkedDictionary(in, dictionaryOffset, factory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dictionary;
    }

    private BitSliceIndexBitmap getBitSliceIndexBitmap() {
        if (bsi == null) {
            try {
                bsi = new BitSliceIndexBitmap(in, bsiOffset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return bsi;
    }

    public RoaringBitmap32 topK(
            int k,
            SortValue.NullOrdering nullOrdering,
            @Nullable RoaringBitmap32 foundSet,
            boolean strict) {
        return fillNulls(
                k, nullOrdering, foundSet, (l, r) -> getBitSliceIndexBitmap().topK(l, r, strict));
    }

    public RoaringBitmap32 bottomK(
            int k,
            SortValue.NullOrdering nullOrdering,
            @Nullable RoaringBitmap32 foundSet,
            boolean strict) {
        return fillNulls(
                k,
                nullOrdering,
                foundSet,
                (l, r) -> getBitSliceIndexBitmap().bottomK(l, r, strict));
    }

    private RoaringBitmap32 fillNulls(
            int k,
            SortValue.NullOrdering nullOrdering,
            @Nullable RoaringBitmap32 foundSet,
            BiFunction<Integer, RoaringBitmap32, RoaringBitmap32> function) {
        if (cardinality <= 0) {
            return rid > 0 ? RoaringBitmap32.bitmapOf(0, rid - 1) : new RoaringBitmap32();
        }

        RoaringBitmap32 bitmap;
        if (NULLS_LAST.equals(nullOrdering)) {
            bitmap = function.apply(k, foundSet);
            long cardinality = bitmap.getCardinality();
            if (cardinality >= k) {
                return bitmap;
            }
            bitmap.or(isNull(foundSet).limit((int) (k - cardinality)));
        } else {
            bitmap = isNull(foundSet);
            long cardinality = bitmap.getCardinality();
            if (cardinality >= k) {
                return bitmap.limit(k);
            }
            bitmap.or(function.apply((int) (k - cardinality), foundSet));
        }
        return bitmap;
    }

    /** A Builder for {@link RangeBitmap}. */
    public static class Appender {

        private int rid;
        private final TreeMap<Object, RoaringBitmap32> bitmaps;
        private final KeyFactory factory;
        private final int limitedSerializedSizeInBytes;

        public Appender(KeyFactory factory, int limitedSerializedSizeInBytes) {
            this.rid = 0;
            this.bitmaps = new TreeMap<>(factory.createComparator());
            this.factory = factory;
            this.limitedSerializedSizeInBytes = limitedSerializedSizeInBytes;
        }

        public void append(Object key) {
            if (key != null) {
                bitmaps.computeIfAbsent(key, (x) -> new RoaringBitmap32()).add(rid);
            }
            rid++;
        }

        public byte[] serialize() {
            int code = 0;
            BitSliceIndexBitmap.Appender bsi =
                    new BitSliceIndexBitmap.Appender(0, bitmaps.size() - 1);
            ChunkedDictionary.Appender dictionary =
                    new ChunkedDictionary.Appender(factory, limitedSerializedSizeInBytes);
            for (Map.Entry<Object, RoaringBitmap32> entry : bitmaps.entrySet()) {
                Object key = entry.getKey();
                RoaringBitmap32 bitmap = entry.getValue();

                // build the dictionary
                dictionary.sortedAppend(key, code);

                // build the relationship between position and code by the bsi
                Iterator<Integer> iterator = bitmap.iterator();
                while (iterator.hasNext()) {
                    bsi.append(iterator.next(), code);
                }

                code++;
            }

            // serializer
            KeyFactory.KeySerializer serializer = factory.createSerializer();

            // min & max
            Object min = bitmaps.isEmpty() ? null : bitmaps.firstKey();
            Object max = bitmaps.isEmpty() ? null : bitmaps.lastKey();

            int headerSize = 0;
            headerSize += Byte.BYTES; // version
            headerSize += Integer.BYTES; // rid
            headerSize += Integer.BYTES; // cardinality
            headerSize += min == null ? 0 : serializer.serializedSizeInBytes(min); // min
            headerSize += max == null ? 0 : serializer.serializedSizeInBytes(max); // max
            headerSize += Integer.BYTES; // dictionary length

            // dictionary
            byte[] dictionarySerializeInBytes = dictionary.serialize();
            int dictionaryLength = dictionarySerializeInBytes.length;

            // bsi
            ByteBuffer bsiBuffer = bsi.serialize();
            int bsiLength = bsiBuffer.array().length;

            ByteBuffer buffer =
                    ByteBuffer.allocate(Integer.BYTES + headerSize + dictionaryLength + bsiLength);
            // write header length
            buffer.putInt(headerSize);

            // write header
            buffer.put(CURRENT_VERSION);
            buffer.putInt(rid);
            buffer.putInt(bitmaps.size());
            if (min != null) {
                serializer.serialize(buffer, min);
            }
            if (max != null) {
                serializer.serialize(buffer, max);
            }
            buffer.putInt(dictionaryLength);

            // write dictionary
            buffer.put(dictionarySerializeInBytes);

            // write bsi
            buffer.put(bsiBuffer.array());

            return buffer.array();
        }
    }
}
