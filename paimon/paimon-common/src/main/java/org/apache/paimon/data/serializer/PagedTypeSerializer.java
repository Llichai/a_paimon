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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;

import java.io.IOException;

/**
 * 分页类型序列化器接口 - 提供跨内存页的序列化和反序列化支持。
 *
 * <p>这个接口扩展了基础的 {@link Serializer} 接口,添加了处理分页内存的能力。
 * 在 Paimon 中,数据通常存储在多个固定大小的内存页(MemorySegment)中,
 * 这个接口提供了高效处理跨页数据的方法。
 *
 * <p>核心功能:
 * <ul>
 *   <li>分页序列化: 将对象序列化到分页输出视图,处理跨页情况
 *   <li>分页反序列化: 从分页输入视图反序列化对象,支持数据拷贝
 *   <li>零拷贝映射: 直接将对象指向内存页,无需拷贝数据
 *   <li>记录跳过: 快速跳过记录而不进行反序列化
 *   <li>对象重用: 支持重用对象实例以减少 GC 压力
 * </ul>
 *
 * <p>分页序列化的挑战:
 * <ul>
 *   <li>跨页问题: 对象可能跨越多个内存页边界
 *   <li>对齐要求: 某些数据结构(如 {@link BinaryRow})需要固定部分不跨页
 *   <li>零拷贝 vs 拷贝: 需要权衡性能和生命周期管理
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>排序操作: 在多个内存页中读写记录
 *   <li>溢出到磁盘: 将内存数据写入文件
 *   <li>网络传输: 通过分页缓冲区发送数据
 *   <li>内存池管理: 从固定大小的内存池中分配和读取数据
 * </ul>
 *
 * <p>方法对比:
 * <table border="1">
 *   <tr>
 *     <th>方法</th>
 *     <th>数据拷贝</th>
 *     <th>性能</th>
 *     <th>生命周期</th>
 *   </tr>
 *   <tr>
 *     <td>{@link #deserializeFromPages}</td>
 *     <td>是</td>
 *     <td>中等</td>
 *     <td>独立</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #mapFromPages}</td>
 *     <td>否</td>
 *     <td>最高</td>
 *     <td>依赖内存页</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #skipRecordFromPages}</td>
 *     <td>否</td>
 *     <td>最高</td>
 *     <td>N/A</td>
 *   </tr>
 * </table>
 *
 * <p>使用示例:
 * <pre>{@code
 * PagedTypeSerializer<BinaryRow> serializer = new BinaryRowSerializer(3);
 *
 * // 序列化到分页输出
 * AbstractPagedOutputView output = ...;
 * BinaryRow row = ...;
 * int skipped = serializer.serializeToPages(row, output);
 *
 * // 反序列化(拷贝数据)
 * AbstractPagedInputView input = ...;
 * BinaryRow deserialized = serializer.deserializeFromPages(input);
 *
 * // 零拷贝映射(高性能但需要管理内存页生命周期)
 * BinaryRow reuse = serializer.createReuseInstance();
 * BinaryRow mapped = serializer.mapFromPages(reuse, input);
 * // 注意:mapped 的数据直接指向 input 的内存页
 *
 * // 跳过记录(扫描时使用)
 * serializer.skipRecordFromPages(input);
 * }</pre>
 *
 * <p>实现注意事项:
 * <ul>
 *   <li>固定部分对齐: 对于 {@link BinaryRow} 等类型,需要处理跨页对齐
 *   <li>内存管理: 使用 mapFromPages 时要小心内存页的生命周期
 *   <li>错误处理: 跨页边界时可能需要特殊处理
 * </ul>
 *
 * @param <T> 要序列化的对象类型
 * @see Serializer
 * @see AbstractPagedOutputView
 * @see AbstractPagedInputView
 * @see BinaryRow
 */
public interface PagedTypeSerializer<T> extends Serializer<T> {

    /**
     * 创建可重用的实例。
     *
     * <p>这个方法创建一个可以在反序列化过程中重用的对象实例,
     * 以减少对象分配和 GC 压力。
     *
     * @return 新的可重用实例
     */
    T createReuseInstance();

    /**
     * 将记录序列化到分页输出视图。
     *
     * <p>某些实现可能会跳过一些字节,如果当前页剩余空间不足。
     * 例如,{@link BinaryRow} 会确保其固定长度部分不跨页边界。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>检查当前页是否有足够空间存储固定部分
     *   <li>如果空间不足,跳到下一页,返回跳过的字节数
     *   <li>写入记录数据到输出视图
     * </ol>
     *
     * @param record 要序列化的记录
     * @param target 目标分页输出视图
     * @return 跳过的字节数(通常为 0,某些情况下可能 > 0)
     * @throws IOException 如果序列化遇到 I/O 错误,通常由输出视图抛出
     */
    int serializeToPages(T record, AbstractPagedOutputView target) throws IOException;

    /**
     * 从分页输入视图反序列化记录。
     *
     * <p>为了与序列化格式保持一致,某些实现可能需要在反序列化之前跳过一些字节,
     * 例如 {@link BinaryRow} 需要跳过对齐字节。
     *
     * <p>通常,从源读取的内容应该被拷贝出来,而不是重用源的底层数据。
     * 如果需要零拷贝的行为,请使用 {@link #mapFromPages(T, AbstractPagedInputView)}。
     *
     * @param source 要读取数据的输入视图
     * @return 反序列化的元素(数据已拷贝)
     * @throws IOException 如果反序列化遇到 I/O 错误,通常由输入视图抛出
     */
    T deserializeFromPages(AbstractPagedInputView source) throws IOException;

    /**
     * 从分页输入视图反序列化记录(重用版本)。
     *
     * <p>这个方法重用提供的对象实例,避免创建新对象。
     *
     * @param reuse 要重用的对象实例
     * @param source 要读取数据的输入视图
     * @return 反序列化的元素(可能是 reuse 对象)
     * @throws IOException 如果反序列化遇到 I/O 错误
     */
    T deserializeFromPages(T reuse, AbstractPagedInputView source) throws IOException;

    /**
     * 从分页输入视图映射重用记录(零拷贝)。
     *
     * <p>这个方法提供了实现零拷贝反序列化的可能性。
     * 你可以选择拷贝或不拷贝从源读取的内容,但建议实现零拷贝。
     *
     * <p>零拷贝方式的注意事项:
     * <ul>
     *   <li>对象直接指向输入视图的内存页
     *   <li>必须正确处理内存页的生命周期
     *   <li>内存页被释放后,对象将变为无效
     *   <li>适合短期使用的场景(如立即处理后丢弃)
     * </ul>
     *
     * @param reuse 要映射的重用记录
     * @param source 要读取数据的输入视图
     * @return 映射的记录(直接指向内存页,零拷贝)
     * @throws IOException 如果反序列化遇到 I/O 错误,通常由输入视图抛出
     */
    T mapFromPages(T reuse, AbstractPagedInputView source) throws IOException;

    /**
     * 从分页输入视图跳过一条记录的字节。
     *
     * <p>这个方法用于快速扫描数据而不进行实际的反序列化,
     * 常用于:
     * <ul>
     *   <li>数据过滤: 根据某些条件跳过不需要的记录
     *   <li>计数操作: 统计记录数量而不读取内容
     *   <li>定位操作: 快速定位到特定位置的记录
     * </ul>
     *
     * @param source 源分页输入视图
     * @throws IOException 如果跳过过程中发生 I/O 错误
     */
    void skipRecordFromPages(AbstractPagedInputView source) throws IOException;
}
