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
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 二进制Map数据结构,用于高效存储键值对集合。
 *
 * <h2>内存布局</h2>
 * <pre>
 * 二进制格式:
 * +-------------------+-------------------+---------------------+
 * | Key Array Size    | Key BinaryArray   | Value BinaryArray  |
 * | (4 bytes)         | (variable)        | (variable)         |
 * +-------------------+-------------------+---------------------+
 *
 * 详细说明:
 * 1. Key Array Size (4字节): 键数组的字节大小
 * 2. Key BinaryArray: 所有键的二进制数组(BinaryArray格式)
 * 3. Value BinaryArray: 所有值的二进制数组(BinaryArray格式)
 * </pre>
 *
 * <h2>设计特点</h2>
 * <ul>
 *   <li>零拷贝访问: 直接在内存段上操作,避免对象创建</li>
 *   <li>紧凑存储: 使用连续内存布局,减少内存碎片</li>
 *   <li>高效序列化: 内存格式即序列化格式,无需转换</li>
 *   <li>键值分离: 键和值分别存储在两个数组中,支持独立访问</li>
 * </ul>
 *
 * <h2>实现说明</h2>
 * <p>本实现参考了Apache Spark的UnsafeMapData设计,采用类似的内存布局和访问模式。
 * 与{@link GenericMap}相比:
 * <ul>
 *   <li>BinaryMap: 适用于高性能场景,零拷贝,内存紧凑</li>
 *   <li>GenericMap: 适用于灵活操作,包装Java Map,易于使用</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建BinaryMap
 * BinaryArray keys = ...; // 键数组
 * BinaryArray values = ...; // 值数组
 * BinaryMap map = BinaryMap.valueOf(keys, values);
 *
 * // 访问map
 * int size = map.size();
 * BinaryArray keyArray = map.keyArray();
 * BinaryArray valueArray = map.valueArray();
 *
 * // 转换为Java Map
 * Map<?, ?> javaMap = map.toJavaMap(keyType, valueType);
 *
 * // 拷贝map
 * BinaryMap copy = map.copy();
 * }</pre>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>内存对齐: 使用8字节对齐,提高访问效率</li>
 *   <li>批量操作: 支持整体拷贝和哈希计算</li>
 *   <li>延迟初始化: 反序列化时延迟创建keys和values对象</li>
 *   <li>重用对象: copy方法支持对象重用,减少GC压力</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class BinaryMap extends BinarySection implements InternalMap {

    private static final long serialVersionUID = 1L;

    /** 键数组,包含map的所有键 */
    private transient BinaryArray keys;

    /** 值数组,包含map的所有值 */
    private transient BinaryArray values;

    /**
     * 创建空的BinaryMap实例。
     *
     * <p>初始化内部的键数组和值数组,准备接收数据。
     */
    public BinaryMap() {
        keys = new BinaryArray();
        values = new BinaryArray();
    }

    /**
     * 返回map的大小(键值对数量)。
     *
     * @return 键值对的数量
     */
    public int size() {
        return keys.size();
    }

    /**
     * 将此BinaryMap指向指定的内存段。
     *
     * <p>此方法解析二进制格式并初始化内部的键数组和值数组:
     * <ol>
     *   <li>从前4字节读取键数组大小</li>
     *   <li>根据大小计算值数组大小</li>
     *   <li>将keys指向键数组区域</li>
     *   <li>将values指向值数组区域</li>
     * </ol>
     *
     * <p>内存布局验证:
     * <ul>
     *   <li>键数组大小必须非负</li>
     *   <li>值数组大小必须非负</li>
     *   <li>键数组和值数组的元素数量必须相等</li>
     * </ul>
     *
     * <p>注意: 在反序列化时(BinarySection.readObject),keys和values可能未初始化,
     * 需要检查null并创建新实例。
     *
     * @param segments 内存段数组
     * @param offset 起始偏移量
     * @param sizeInBytes 总字节大小
     */
    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        // 从前4字节读取键数组的字节大小
        final int keyArrayBytes = MemorySegmentUtils.getInt(segments, offset);
        assert keyArrayBytes >= 0 : "keyArraySize (" + keyArrayBytes + ") should >= 0";
        final int valueArrayBytes = sizeInBytes - keyArrayBytes - 4;
        assert valueArrayBytes >= 0 : "valueArraySize (" + valueArrayBytes + ") should >= 0";

        // 在反序列化过程中,keys和values可能未初始化,需要检查并创建
        if (keys == null) {
            keys = new BinaryArray();
        }
        keys.pointTo(segments, offset + 4, keyArrayBytes);
        if (values == null) {
            values = new BinaryArray();
        }
        values.pointTo(segments, offset + 4 + keyArrayBytes, valueArrayBytes);

        assert keys.size() == values.size();

        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * 返回键数组。
     *
     * <p>键数组包含map中的所有键,按插入顺序排列。
     *
     * @return 键的BinaryArray
     */
    public BinaryArray keyArray() {
        return keys;
    }

    /**
     * 返回值数组。
     *
     * <p>值数组包含map中的所有值,与键数组按位置对应。
     *
     * @return 值的BinaryArray
     */
    public BinaryArray valueArray() {
        return values;
    }

    /**
     * 将BinaryMap转换为Java Map。
     *
     * <p>转换过程:
     * <ol>
     *   <li>将键数组转换为对象数组</li>
     *   <li>将值数组转换为对象数组</li>
     *   <li>创建HashMap并逐个添加键值对</li>
     * </ol>
     *
     * <p>注意: 此操作会创建新的Java对象,适用于需要以Java Map形式操作数据的场景。
     *
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     * @return Java Map实例
     */
    public Map<?, ?> toJavaMap(DataType keyType, DataType valueType) {
        Object[] keyArray = keys.toObjectArray(keyType);
        Object[] valueArray = values.toObjectArray(valueType);

        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < keyArray.length; i++) {
            map.put(keyArray[i], valueArray[i]);
        }
        return map;
    }

    /**
     * 创建此BinaryMap的拷贝。
     *
     * <p>创建新的BinaryMap实例并拷贝全部内容。
     *
     * @return 新的BinaryMap拷贝
     */
    public BinaryMap copy() {
        return copy(new BinaryMap());
    }

    /**
     * 将此BinaryMap拷贝到指定的重用实例。
     *
     * <p>拷贝过程:
     * <ol>
     *   <li>将内存段内容拷贝到新的字节数组</li>
     *   <li>包装为新的内存段</li>
     *   <li>让重用实例指向新的内存段</li>
     * </ol>
     *
     * <p>性能优化: 支持对象重用,减少GC压力。
     *
     * @param reuse 要重用的BinaryMap实例
     * @return 包含拷贝数据的BinaryMap
     */
    public BinaryMap copy(BinaryMap reuse) {
        byte[] bytes = MemorySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    /**
     * 计算此BinaryMap的哈希码。
     *
     * <p>基于整个内存段内容计算哈希值,使用按字(word)计算的高效算法。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return MemorySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    // ------------------------------------------------------------------------------------------
    // 构造工具方法
    // ------------------------------------------------------------------------------------------

    /**
     * 从键数组和值数组创建BinaryMap。
     *
     * <p>创建过程:
     * <ol>
     *   <li>验证键数组和值数组都在单个内存段中</li>
     *   <li>分配新的字节数组(4字节头 + 键数组 + 值数组)</li>
     *   <li>写入键数组大小到头部</li>
     *   <li>拷贝键数组内容</li>
     *   <li>拷贝值数组内容</li>
     *   <li>创建BinaryMap并指向新的内存段</li>
     * </ol>
     *
     * <p>限制条件: 键数组和值数组必须各自在单个内存段中(不支持跨段)。
     *
     * @param key 键的BinaryArray
     * @param value 值的BinaryArray
     * @return 新创建的BinaryMap
     * @throws IllegalArgumentException 如果键或值数组跨多个内存段
     */
    public static BinaryMap valueOf(BinaryArray key, BinaryArray value) {
        checkArgument(key.segments.length == 1 && value.getSegments().length == 1);
        byte[] bytes = new byte[4 + key.sizeInBytes + value.sizeInBytes];
        MemorySegment segment = MemorySegment.wrap(bytes);
        segment.putInt(0, key.sizeInBytes);
        key.getSegments()[0].copyTo(key.getOffset(), segment, 4, key.sizeInBytes);
        value.getSegments()[0].copyTo(
                value.getOffset(), segment, 4 + key.sizeInBytes, value.sizeInBytes);
        BinaryMap map = new BinaryMap();
        map.pointTo(segment, 0, bytes.length);
        return map;
    }
}
