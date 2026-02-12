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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MathUtils;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.paimon.data.serializer.BinaryRowSerializer.getSerializedRowLength;
import static org.apache.paimon.data.serializer.BinaryRowSerializer.serializeWithoutLength;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 分页输出序列化器,使用内存页高效处理行数据的序列化。
 *
 * <p>该序列化器采用两阶段策略,在性能和内存管理之间取得平衡:
 *
 * <ol>
 *   <li><strong>初始阶段:</strong> 将数据写入初始缓冲区,直到达到指定的页大小</li>
 *   <li><strong>分页阶段:</strong> 当初始缓冲区超过页大小后,切换到使用 {@link SimpleCollectingOutputView}
 *       和分配的内存段来进行高效的内存管理</li>
 * </ol>
 *
 * <p>该设计确保了在不同场景下的最佳性能:
 * <ul>
 *   <li><b>小数据集:</b> 使用单一缓冲区,避免分页开销</li>
 *   <li><b>大数据集:</b> 使用分页内存分配,避免大缓冲区和频繁扩容</li>
 * </ul>
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>自适应切换:</b> 根据数据大小自动在单缓冲和分页模式间切换</li>
 *   <li><b>内存效率:</b> 分页模式避免大块连续内存分配</li>
 *   <li><b>零拷贝优化:</b> 使用 BinaryRow 直接序列化,减少数据转换</li>
 *   <li><b>延迟分页:</b> 只在必要时才切换到分页模式</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>批量写入:</b> 批量序列化大量行数据</li>
 *   <li><b>缓存构建:</b> 构建内存中的序列化数据缓存</li>
 *   <li><b>Spill:</b> 将内存数据溢写到磁盘前的序列化</li>
 *   <li><b>网络传输:</b> 准备网络传输的序列化数据</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * InternalRowSerializer serializer = ...;
 * DataPagedOutputSerializer output = new DataPagedOutputSerializer(
 *     serializer,
 *     4096,    // 初始缓冲区大小
 *     64 * 1024 // 页大小(64KB)
 * );
 *
 * // 写入行数据
 * for (InternalRow row : rows) {
 *     output.write(row);
 * }
 *
 * // 获取分页输出视图
 * SimpleCollectingOutputView result = output.close();
 *
 * // 访问序列化后的数据
 * List<MemorySegment> segments = result.getSegments();
 * }</pre>
 *
 * <h3>性能优化</h3>
 * <table border="1">
 *   <tr>
 *     <th>阶段</th>
 *     <th>特点</th>
 *     <th>优势</th>
 *   </tr>
 *   <tr>
 *     <td>初始阶段</td>
 *     <td>单一连续缓冲区</td>
 *     <td>快速访问,无分页开销</td>
 *   </tr>
 *   <tr>
 *     <td>分页阶段</td>
 *     <td>固定大小的内存页</td>
 *     <td>避免大内存分配,减少GC压力</td>
 *   </tr>
 * </table>
 *
 * <h3>内存管理</h3>
 * <ul>
 *   <li>初始缓冲区:由 {@link DataOutputSerializer} 管理,支持自动扩容</li>
 *   <li>分页缓冲区:由 {@link SimpleCollectingOutputView} 管理,使用固定大小页</li>
 *   <li>切换阈值:当 {@code currentSize + rowSize > pageSize} 时触发切换</li>
 *   <li>最终页大小:对齐到 2 的幂次方,提高内存访问效率</li>
 * </ul>
 *
 * <h3>线程安全性</h3>
 * <p>该类<b>不是线程安全的</b>。每个线程应该使用独立的实例。
 *
 * @see DataOutputSerializer
 * @see SimpleCollectingOutputView
 * @see InternalRowSerializer
 */
public class DataPagedOutputSerializer {

    /** 行序列化器,用于将行数据转换为二进制格式。 */
    private final InternalRowSerializer serializer;

    /** 页大小(字节),超过此大小后切换到分页模式。 */
    private final int pageSize;

    /** 初始阶段使用的输出序列化器,基于单一连续缓冲区。 */
    private DataOutputSerializer initialOut;

    /** 分页阶段使用的输出视图,基于多个固定大小的内存页。 */
    private SimpleCollectingOutputView pagedOut;

    /**
     * 构造一个新的分页输出序列化器。
     *
     * <p><b>参数选择建议:</b>
     * <ul>
     *   <li><b>startSize:</b> 建议设置为预估的小批量数据大小(如 4KB)
     *   <li><b>pageSize:</b> 建议设置为 64KB-1MB,平衡分页开销和内存压力
     * </ul>
     *
     * @param serializer 用于将行转换为二进制格式的内部行序列化器
     * @param startSize 初始缓冲区大小,用于存储序列化数据
     * @param pageSize 每个内存页的最大大小,超过后切换到分页模式
     */
    public DataPagedOutputSerializer(
            InternalRowSerializer serializer, int startSize, int pageSize) {
        this.serializer = serializer;
        this.pageSize = pageSize;
        this.initialOut = new DataOutputSerializer(startSize);
    }

    /** 测试可见:返回分页输出视图,用于验证分页模式是否已激活。 */
    @VisibleForTesting
    SimpleCollectingOutputView pagedOut() {
        return pagedOut;
    }

    /**
     * 将一个二进制行序列化到输出。
     *
     * <p>根据当前状态和可用空间,该方法会采取不同的策略:
     *
     * <ul>
     *   <li><b>初始阶段:</b> 如果剩余空间足够,直接写入初始缓冲区</li>
     *   <li><b>切换时机:</b> 当 {@code currentSize + rowSize > pageSize} 时,
     *       切换到分页模式,并将已有数据迁移到第一个页</li>
     *   <li><b>分页阶段:</b> 写入分页输出视图,自动管理多个内存页</li>
     * </ul>
     *
     * <p><b>性能考虑:</b>
     * <ul>
     *   <li>初始阶段的写入速度最快,因为是连续内存</li>
     *   <li>切换到分页模式有一次性开销(数据迁移)</li>
     *   <li>分页阶段写入速度略慢,但避免了大内存分配</li>
     * </ul>
     *
     * @param row 要序列化的二进制行
     * @throws IOException 如果序列化过程中发生 I/O 错误
     */
    public void write(InternalRow row) throws IOException {
        if (pagedOut != null) {
            // 已在分页模式,直接序列化到页
            serializer.serializeToPages(row, pagedOut);
        } else {
            // 初始模式:先转换为 BinaryRow 以计算大小
            BinaryRow binaryRow = serializer.toBinaryRow(row);
            int serializedSize = getSerializedRowLength(binaryRow);

            if (initialOut.length() + serializedSize > pageSize) {
                // 触发切换到分页模式
                pagedOut = toPagedOutput(initialOut, pageSize);
                initialOut = null;
                serializer.serializeToPages(row, pagedOut);
            } else {
                // 继续写入初始缓冲区
                initialOut.writeInt(binaryRow.getSizeInBytes());
                serializeWithoutLength(binaryRow, initialOut);
            }
        }
    }

    /**
     * 将初始输出转换为分页输出。
     *
     * <p>该方法执行以下操作:
     * <ol>
     *   <li>验证初始输出大小不超过页大小</li>
     *   <li>创建 SimpleCollectingOutputView,使用指定的页大小</li>
     *   <li>将初始缓冲区的数据复制到第一个页</li>
     * </ol>
     *
     * @param output 初始输出序列化器
     * @param pageSize 页大小
     * @return 新创建的分页输出视图,包含初始数据
     * @throws IOException 如果数据迁移失败
     * @throws IllegalArgumentException 如果初始数据超过页大小
     */
    private static SimpleCollectingOutputView toPagedOutput(
            DataOutputSerializer output, int pageSize) throws IOException {
        checkArgument(output.length() <= pageSize);
        SimpleCollectingOutputView pagedOut =
                new SimpleCollectingOutputView(
                        new ArrayList<>(),
                        () -> MemorySegment.allocateHeapMemory(pageSize),
                        pageSize);
        pagedOut.write(output.getSharedBuffer(), 0, output.length());
        return pagedOut;
    }

    /**
     * 关闭序列化器并返回最终的分页输出视图。
     *
     * <p><b>行为说明:</b>
     * <ul>
     *   <li>如果已在分页模式,直接返回分页输出视图</li>
     *   <li>如果仍在初始模式,创建一个优化大小的分页输出视图:
     *       <ul>
     *         <li>页大小对齐到 2 的幂次方(向上取整)</li>
     *         <li>例如:1000 字节 → 1024 字节页</li>
     *       </ul>
     *   </li>
     * </ul>
     *
     * <p><b>为什么对齐到 2 的幂次方:</b>
     * <ul>
     *   <li>提高 CPU 缓存行对齐,加速访问</li>
     *   <li>简化位运算和地址计算</li>
     *   <li>与 MemorySegment 的最佳实践一致</li>
     * </ul>
     *
     * <p><b>注意:</b> 调用此方法后,不应再调用 {@link #write(InternalRow)}。
     *
     * @return 包含所有序列化数据的分页输出视图
     * @throws IOException 如果创建分页输出失败
     */
    public SimpleCollectingOutputView close() throws IOException {
        if (pagedOut != null) {
            return pagedOut;
        }

        // 对齐到 2 的幂次方,优化内存访问
        int pageSize = MathUtils.roundUpToPowerOf2(initialOut.length());
        return toPagedOutput(initialOut, pageSize);
    }
}
