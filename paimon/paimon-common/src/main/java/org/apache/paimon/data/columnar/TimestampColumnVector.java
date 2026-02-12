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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.Timestamp;

/**
 * 时间戳列向量接口,用于访问时间戳类型的列数据。
 *
 * <p>此接口提供对 TIMESTAMP 和 TIMESTAMP WITH LOCAL TIME ZONE 类型数据的访问,
 * 支持不同精度的时间戳(从秒到纳秒)。
 *
 * <h2>时间戳精度</h2>
 * <ul>
 *   <li>precision = 0: 秒级精度
 *   <li>precision = 3: 毫秒级精度
 *   <li>precision = 6: 微秒级精度
 *   <li>precision = 9: 纳秒级精度
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>事件时间戳记录
 *   <li>数据生成时间追踪
 *   <li>时间序列分析
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.Timestamp 时间戳表示类
 * @see org.apache.paimon.data.columnar.heap.HeapTimestampVector 堆内存实现
 * @see org.apache.paimon.data.columnar.writable.WritableTimestampVector 可写实现
 */
public interface TimestampColumnVector extends ColumnVector {
    /**
     * 获取指定位置的时间戳值。
     *
     * @param i 行索引(从0开始)
     * @param precision 时间戳精度(0-9)
     * @return 时间戳对象
     */
    Timestamp getTimestamp(int i, int precision);
}
