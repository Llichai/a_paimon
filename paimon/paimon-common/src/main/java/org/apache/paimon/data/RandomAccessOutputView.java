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

import java.io.EOFException;

/**
 * 随机访问输出视图,用于在内存段数组上进行顺序写入。
 *
 * <h2>设计目的</h2>
 * <p>RandomAccessOutputView扩展了{@link AbstractPagedOutputView},提供多段内存写入能力:
 * <ul>
 *   <li>在多个内存段上顺序写入数据</li>
 *   <li>自动处理段边界切换</li>
 *   <li>支持固定段大小的内存管理</li>
 *   <li>高效的批量数据写入</li>
 * </ul>
 *
 * <h2>内存布局</h2>
 * <p>数据顺序写入到内存段数组中:
 * <pre>
 * +-------------+-------------+-------------+-------------+
 * | Segment 0   | Segment 1   | Segment 2   | Segment N-1 |
 * | (full)      | (full)      | (partial)   | (empty)     |
 * +-------------+-------------+-------------+-------------+
 *                              ^
 *                              |
 *                         当前写入位置
 * </pre>
 *
 * <h2>段大小要求</h2>
 * <p>段大小必须是2的幂,原因:
 * <ul>
 *   <li>便于使用位运算进行快速定位</li>
 *   <li>保证内存对齐,提高访问效率</li>
 *   <li>简化边界检查逻辑</li>
 * </ul>
 *
 * <p>常用段大小: 4KB(4096), 8KB(8192), 32KB(32768), 64KB(65536)
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 准备内存段数组
 * MemorySegment[] segments = new MemorySegment[10];
 * for (int i = 0; i < segments.length; i++) {
 *     segments[i] = MemorySegment.wrap(new byte[4096]);
 * }
 *
 * // 创建随机访问输出视图
 * RandomAccessOutputView view = new RandomAccessOutputView(segments, 4096);
 *
 * // 写入数据
 * view.writeInt(100);
 * view.writeLong(200L);
 * view.writeUTF("Hello");
 * view.write(byteArray);
 *
 * // 视图会自动切换段,直到所有段用完或抛出EOFException
 * }</pre>
 *
 * <h2>段切换机制</h2>
 * <p>当当前段写满时:
 * <ol>
 *   <li>调用nextSegment方法获取下一个段</li>
 *   <li>更新currentSegmentIndex</li>
 *   <li>继续在新段上写入</li>
 *   <li>如果没有更多段,抛出EOFException</li>
 * </ol>
 *
 * <h2>错误处理</h2>
 * <ul>
 *   <li>段大小不是2的幂: 抛出IllegalArgumentException</li>
 *   <li>段数组用完: 抛出EOFException</li>
 *   <li>写入失败: 传播底层IOException</li>
 * </ul>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li>零拷贝: 直接写入内存段,无需额外缓冲</li>
 *   <li>批量写入: 支持写入任意长度数据,自动跨段</li>
 *   <li>预分配: 使用预先分配的内存段数组</li>
 *   <li>顺序访问: 针对顺序写入优化,无随机访问开销</li>
 * </ul>
 *
 * <h2>与RandomAccessInputView的对应关系</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>RandomAccessOutputView</th>
 *     <th>RandomAccessInputView</th>
 *   </tr>
 *   <tr>
 *     <td>操作方向</td>
 *     <td>写入(输出)</td>
 *     <td>读取(输入)</td>
 *   </tr>
 *   <tr>
 *     <td>访问模式</td>
 *     <td>顺序写入</td>
 *     <td>随机读取</td>
 *   </tr>
 *   <tr>
 *     <td>段切换</td>
 *     <td>写满自动切换</td>
 *     <td>读完自动切换</td>
 *   </tr>
 *   <tr>
 *     <td>位置控制</td>
 *     <td>顺序推进</td>
 *     <td>支持seek</td>
 *   </tr>
 * </table>
 *
 * @see AbstractPagedOutputView
 * @see RandomAccessInputView
 */
public class RandomAccessOutputView extends AbstractPagedOutputView {

    /** 内存段数组 */
    private final MemorySegment[] segments;

    /** 当前段索引 */
    private int currentSegmentIndex;

    /**
     * 创建RandomAccessOutputView。
     *
     * <p>初始化过程:
     * <ol>
     *   <li>验证段大小是2的幂</li>
     *   <li>初始化第一个段</li>
     *   <li>设置段大小</li>
     *   <li>准备开始写入</li>
     * </ol>
     *
     * @param segments 内存段数组(预先分配)
     * @param segmentSize 每个段的大小(必须是2的幂)
     * @throws IllegalArgumentException 如果段大小不是2的幂
     */
    public RandomAccessOutputView(MemorySegment[] segments, int segmentSize) {
        super(segments[0], segmentSize);

        if ((segmentSize & (segmentSize - 1)) != 0) {
            throw new IllegalArgumentException("Segment size must be a power of 2!");
        }

        this.segments = segments;
    }

    /**
     * 获取下一个内存段。
     *
     * <p>当当前段写满时,由父类自动调用此方法切换到下一段。
     *
     * <p>切换逻辑:
     * <ol>
     *   <li>递增段索引</li>
     *   <li>检查是否还有剩余段</li>
     *   <li>如果有,返回下一个段</li>
     *   <li>如果没有,抛出EOFException</li>
     * </ol>
     *
     * @param current 当前内存段
     * @param positionInCurrent 当前段内的位置
     * @return 下一个内存段
     * @throws EOFException 如果没有更多段可用
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws EOFException {
        if (++this.currentSegmentIndex < this.segments.length) {
            return this.segments[this.currentSegmentIndex];
        } else {
            throw new EOFException();
        }
    }
}
