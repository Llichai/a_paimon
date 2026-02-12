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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.writable.WritableTimestampVector;

import java.util.Arrays;

/**
 * 堆时间戳列向量实现类。
 *
 * <p>这个类表示一个可空的时间戳列向量，用于在列式存储中高效地存储和访问时间戳类型的数据（TIMESTAMP）。
 * 时间戳由两部分组成：毫秒部分和纳秒部分，提供了纳秒级精度。
 *
 * <h2>时间戳表示</h2>
 * <p>Paimon 的时间戳由两个组件组成：
 * <ul>
 *   <li><b>milliseconds</b>: 自 Unix 纪元（1970-01-01 00:00:00 UTC）以来的毫秒数</li>
 *   <li><b>nanoOfMilliseconds</b>: 当前毫秒内的纳秒偏移量（0-999,999）</li>
 * </ul>
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapTimestampVector 结构:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个时间戳是否为 null
 * ├─────────────────────┤
 * │ Milliseconds Array  │  毫秒部分（64位）
 * │ [1609459200000,...] │
 * ├─────────────────────┤
 * │ NanoOfMilliseconds  │  纳秒部分（32位）
 * │ [123456, 789012,...]│  范围: 0-999999
 * └─────────────────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个可以存储1000个时间戳的列向量
 * HeapTimestampVector vector = new HeapTimestampVector(1000);
 *
 * // 设置时间戳: 2021-01-01 00:00:00.123456
 * Timestamp ts = Timestamp.fromEpochMillis(1609459200000L, 123456);
 * vector.setTimestamp(0, ts);
 *
 * // 追加时间戳
 * vector.appendTimestamp(Timestamp.now());
 *
 * // 读取时间戳
 * Timestamp value = vector.getTimestamp(0, 6);  // 精度6表示微秒
 *
 * // 填充相同的时间戳
 * vector.fill(Timestamp.fromEpochMillis(0L, 0));
 *
 * // 重置向量
 * vector.reset();
 * }</pre>
 *
 * <h2>精度支持</h2>
 * <p>虽然内部总是以纳秒精度存储，但 {@link #getTimestamp(int, int)} 方法接受一个精度参数：
 * <ul>
 *   <li>精度 3: 毫秒精度</li>
 *   <li>精度 6: 微秒精度</li>
 *   <li>精度 9: 纳秒精度</li>
 * </ul>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 每个时间戳占用12字节（8字节毫秒 + 4字节纳秒）</li>
 *   <li><b>高精度</b>: 支持纳秒级精度（最高9位小数）</li>
 *   <li><b>字典压缩</b>: 对于重复值多的数据，支持字典编码</li>
 *   <li><b>访问速度</b>: O(1) 时间复杂度的随机访问</li>
 * </ul>
 *
 * <h2>线程安全性</h2>
 * 此类不是线程安全的，如果多线程访问需要外部同步。
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableTimestampVector 可写时间戳向量接口
 * @see Timestamp 时间戳数据类型
 */
public class HeapTimestampVector extends AbstractHeapVector implements WritableTimestampVector {

    private static final long serialVersionUID = 1L;

    /** 毫秒部分数组。 */
    private long[] milliseconds;

    /** 纳秒部分数组（当前毫秒内的纳秒偏移量）。 */
    private int[] nanoOfMilliseconds;

    /**
     * 构造一个堆时间戳列向量。
     *
     * @param len 向量的容量（可以存储的时间戳数量）
     */
    public HeapTimestampVector(int len) {
        super(len);
        this.milliseconds = new long[len];
        this.nanoOfMilliseconds = new int[len];
    }

    /**
     * 为堆向量预留空间。
     *
     * <p>如果需要的容量大于当前数组大小，则扩展毫秒和纳秒数组。
     *
     * @param newCapacity 新的容量大小
     */
    @Override
    void reserveForHeapVector(int newCapacity) {
        if (milliseconds.length < newCapacity) {
            milliseconds = Arrays.copyOf(milliseconds, newCapacity);
            nanoOfMilliseconds = Arrays.copyOf(nanoOfMilliseconds, newCapacity);
        }
    }

    /**
     * 获取指定位置的时间戳。
     *
     * <p>如果使用了字典编码，则会自动从字典中解码。
     *
     * @param i 索引位置
     * @param precision 时间戳精度（未使用，内部总是返回完整精度）
     * @return 该位置的时间戳对象
     */
    @Override
    public Timestamp getTimestamp(int i, int precision) {
        if (dictionary == null) {
            return Timestamp.fromEpochMillis(milliseconds[i], nanoOfMilliseconds[i]);
        } else {
            return dictionary.decodeToTimestamp(dictionaryIds.vector[i]);
        }
    }

    /**
     * 设置指定位置的时间戳。
     *
     * @param i 索引位置
     * @param timestamp 要设置的时间戳对象
     */
    @Override
    public void setTimestamp(int i, Timestamp timestamp) {
        milliseconds[i] = timestamp.getMillisecond();
        nanoOfMilliseconds[i] = timestamp.getNanoOfMillisecond();
    }

    /**
     * 追加一个时间戳到向量末尾。
     *
     * <p>如果容量不足，会自动扩展向量。
     *
     * @param timestamp 要追加的时间戳对象
     */
    @Override
    public void appendTimestamp(Timestamp timestamp) {
        reserve(elementsAppended + 1);
        setTimestamp(elementsAppended, timestamp);
        elementsAppended++;
    }

    /**
     * 用指定的时间戳填充整个向量。
     *
     * @param value 填充用的时间戳对象
     */
    @Override
    public void fill(Timestamp value) {
        Arrays.fill(milliseconds, value.getMillisecond());
        Arrays.fill(nanoOfMilliseconds, value.getNanoOfMillisecond());
    }

    /**
     * 重置向量到初始状态。
     *
     * <p>清空所有数据，重置元素计数器。如果数组大小与容量不匹配，则重新分配数组。
     */
    @Override
    public void reset() {
        super.reset();
        if (milliseconds.length != capacity) {
            milliseconds = new long[capacity];
        } else {
            Arrays.fill(milliseconds, 0L);
        }
        if (nanoOfMilliseconds.length != capacity) {
            nanoOfMilliseconds = new int[capacity];
        } else {
            Arrays.fill(nanoOfMilliseconds, 0);
        }
    }
}
