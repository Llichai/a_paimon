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

import org.apache.paimon.io.SeekableDataInputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MathUtils;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;

/**
 * 随机访问输入视图,用于在内存段数组上进行随机位置读取。
 *
 * <h2>设计目的</h2>
 * <p>RandomAccessInputView扩展了{@link AbstractPagedInputView},提供随机访问能力:
 * <ul>
 *   <li>支持通过绝对位置快速定位(seek)</li>
 *   <li>在多个内存段上顺序或随机读取数据</li>
 *   <li>自动处理段边界切换</li>
 *   <li>高效的位置计算(使用位运算)</li>
 * </ul>
 *
 * <h2>内存布局</h2>
 * <p>数据存储在一个内存段数组中:
 * <pre>
 * +-------------+-------------+-------------+-------------+
 * | Segment 0   | Segment 1   | Segment 2   | Segment N-1 |
 * | (full size) | (full size) | (full size) | (partial)   |
 * +-------------+-------------+-------------+-------------+
 *                                            ^
 *                                            |
 *                                    limitInLastSegment
 * </pre>
 *
 * <h2>位置计算机制</h2>
 * <p>使用位运算实现高效的位置计算:
 * <ul>
 *   <li>段索引 = position >>> segmentSizeBits</li>
 *   <li>段内偏移 = position & segmentSizeMask</li>
 *   <li>绝对位置 = (segmentIndex << segmentSizeBits) + offsetInSegment</li>
 * </ul>
 *
 * <p>前提条件: 段大小必须是2的幂(如4096, 8192, 32768等)
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建内存段数组
 * ArrayList<MemorySegment> segments = new ArrayList<>();
 * segments.add(MemorySegment.wrap(new byte[4096]));
 * segments.add(MemorySegment.wrap(new byte[4096]));
 * segments.add(MemorySegment.wrap(new byte[2048])); // 最后一个段部分使用
 *
 * // 创建随机访问输入视图
 * RandomAccessInputView view = new RandomAccessInputView(
 *     segments,
 *     4096,    // 段大小
 *     2048     // 最后一个段的有效长度
 * );
 *
 * // 随机访问
 * view.setReadPosition(5000);  // 跳到位置5000
 * int value = view.readInt();  // 读取整数
 *
 * // 获取当前位置
 * long position = view.getReadPosition();
 *
 * // 顺序读取
 * byte[] data = new byte[100];
 * view.read(data);
 * }</pre>
 *
 * <h2>段边界处理</h2>
 * <p>自动处理跨段读取:
 * <ul>
 *   <li>当前段数据读完时,自动切换到下一段</li>
 *   <li>最后一个段使用limitInLastSegment限制读取范围</li>
 *   <li>超出范围时抛出EOFException</li>
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>位运算: 使用位移和掩码代替除法和取模</li>
 *   <li>零拷贝: 直接在内存段上读取,无需额外拷贝</li>
 *   <li>批量读取: 支持读取任意长度数据,自动跨段</li>
 *   <li>位置缓存: currentSegmentIndex避免重复计算</li>
 * </ul>
 *
 * @see SeekableDataInputView
 * @see AbstractPagedInputView
 */
public class RandomAccessInputView extends AbstractPagedInputView implements SeekableDataInputView {

    /** 内存段列表 */
    private final ArrayList<MemorySegment> segments;

    /** 当前段索引 */
    private int currentSegmentIndex;

    /** 段大小的位数(log2(segmentSize)) */
    private final int segmentSizeBits;

    /** 段大小掩码(segmentSize - 1) */
    private final int segmentSizeMask;

    /** 段大小(字节) */
    private final int segmentSize;

    /** 最后一个段的有效长度 */
    private final int limitInLastSegment;

    /**
     * 创建RandomAccessInputView,最后一个段完全使用。
     *
     * @param segments 内存段列表
     * @param segmentSize 段大小(必须是2的幂)
     */
    public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize) {
        this(segments, segmentSize, segmentSize);
    }

    /**
     * 创建RandomAccessInputView,指定最后一个段的有效长度。
     *
     * <p>初始化过程:
     * <ol>
     *   <li>验证段大小是2的幂</li>
     *   <li>计算位运算参数(bits和mask)</li>
     *   <li>初始化到第一个段</li>
     *   <li>设置读取限制</li>
     * </ol>
     *
     * @param segments 内存段列表
     * @param segmentSize 段大小(必须是2的幂,如4096, 8192等)
     * @param limitInLastSegment 最后一个段的有效长度
     */
    public RandomAccessInputView(
            ArrayList<MemorySegment> segments, int segmentSize, int limitInLastSegment) {
        super(segments.get(0), segments.size() > 1 ? segmentSize : limitInLastSegment);
        this.segments = segments;
        this.currentSegmentIndex = 0;
        this.segmentSize = segmentSize;
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        this.segmentSizeMask = segmentSize - 1;
        this.limitInLastSegment = limitInLastSegment;
    }

    /**
     * 设置读取位置(随机访问)。
     *
     * <p>位置计算:
     * <ul>
     *   <li>段索引 = position >>> segmentSizeBits (右移,相当于除以segmentSize)</li>
     *   <li>段内偏移 = position & segmentSizeMask (取低位,相当于对segmentSize取模)</li>
     * </ul>
     *
     * <p>示例: 假设segmentSize=4096(2^12)
     * <ul>
     *   <li>position=5000: 段索引=1, 偏移=904</li>
     *   <li>position=10000: 段索引=2, 偏移=1808</li>
     * </ul>
     *
     * @param position 绝对字节位置
     */
    @Override
    public void setReadPosition(long position) {
        final int bufferNum = (int) (position >>> this.segmentSizeBits);
        final int offset = (int) (position & this.segmentSizeMask);

        this.currentSegmentIndex = bufferNum;
        seekInput(
                this.segments.get(bufferNum),
                offset,
                bufferNum < this.segments.size() - 1 ? this.segmentSize : this.limitInLastSegment);
    }

    /**
     * 获取当前读取位置。
     *
     * <p>位置计算:
     * <pre>
     * position = (currentSegmentIndex << segmentSizeBits) + offsetInSegment
     *          = currentSegmentIndex * segmentSize + offsetInSegment
     * </pre>
     *
     * @return 当前绝对字节位置
     */
    public long getReadPosition() {
        return (((long) currentSegmentIndex) << segmentSizeBits) + getCurrentPositionInSegment();
    }

    /**
     * 获取下一个内存段。
     *
     * <p>当当前段读取完毕时,由父类自动调用此方法切换到下一段。
     *
     * @param current 当前内存段
     * @return 下一个内存段
     * @throws EOFException 如果没有更多段
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
        if (++this.currentSegmentIndex < this.segments.size()) {
            return this.segments.get(this.currentSegmentIndex);
        } else {
            throw new EOFException();
        }
    }

    /**
     * 获取指定段的读取限制。
     *
     * <p>限制说明:
     * <ul>
     *   <li>非最后段: 返回完整的segmentSize</li>
     *   <li>最后一段: 返回limitInLastSegment</li>
     * </ul>
     *
     * @param segment 内存段
     * @return 该段的读取限制(字节数)
     */
    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return this.currentSegmentIndex == this.segments.size() - 1
                ? this.limitInLastSegment
                : this.segmentSize;
    }

    /**
     * 返回内存段列表。
     *
     * @return 所有内存段
     */
    public List<MemorySegment> segments() {
        return segments;
    }
}
