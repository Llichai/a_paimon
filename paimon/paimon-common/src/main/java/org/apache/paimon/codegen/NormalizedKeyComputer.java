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

package org.apache.paimon.codegen;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.MemorySegment;

/**
 * 标准化键计算器。
 *
 * <p>用于 {@code SortBuffer} 的标准化键计算。标准化键是一种优化的排序键表示形式,
 * 可以将排序字段转换为固定长度的字节数组,从而可以直接进行字节比较而无需反序列化,
 * 大大提高排序性能。
 *
 * <p>为了性能考虑,该接口的子类通常通过 CodeGenerator 动态生成。
 *
 * <p>主要功能:
 * <ul>
 *   <li>将记录转换为标准化键并写入内存段</li>
 *   <li>直接比较内存段中的标准化键</li>
 *   <li>交换内存段中的标准化键</li>
 *   <li>提供标准化键的元信息（长度、是否完全确定等）</li>
 * </ul>
 */
public interface NormalizedKeyComputer {

    /**
     * 为给定记录写入标准化键到目标 {@link MemorySegment}。
     *
     * @param record 要转换的记录
     * @param target 目标内存段
     * @param offset 目标内存段中的偏移量
     */
    void putKey(InternalRow record, MemorySegment target, int offset);

    /**
     * 比较两个标准化键。
     *
     * @param segI 第一个键所在的内存段
     * @param offsetI 第一个键在内存段中的偏移量
     * @param segJ 第二个键所在的内存段
     * @param offsetJ 第二个键在内存段中的偏移量
     * @return 比较结果: 负数表示第一个键较小,0表示相等,正数表示第一个键较大
     */
    int compareKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ);

    /**
     * 交换两个标准化键。
     *
     * @param segI 第一个键所在的内存段
     * @param offsetI 第一个键在内存段中的偏移量
     * @param segJ 第二个键所在的内存段
     * @param offsetJ 第二个键在内存段中的偏移量
     */
    void swapKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ);

    /**
     * 获取标准化键的字节长度。
     *
     * @return 标准化键占用的字节数
     */
    int getNumKeyBytes();

    /**
     * 标准化键是否能完全确定比较结果。
     *
     * <p>如果标准化键能够完全表示排序字段的所有信息,则返回 true,
     * 此时可以仅通过标准化键比较就确定最终的排序顺序。
     * 如果返回 false,则在标准化键相等时还需要比较完整的记录。
     *
     * @return true 如果标准化键完全确定比较结果
     */
    boolean isKeyFullyDetermines();

    /**
     * 标准化键比较是否应该反转。
     *
     * <p>用于支持降序排序。
     *
     * @return true 如果应该反转键比较结果
     */
    boolean invertKey();
}
