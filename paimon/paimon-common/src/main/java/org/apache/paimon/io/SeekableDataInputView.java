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

/**
 * 可定位数据输入视图接口,标记 {@link DataInputView} 为可定位的。
 *
 * <p>可定位视图允许设置读取位置,支持随机访问模式而不仅仅是顺序读取。
 * 这在需要跳过大量数据或重复读取某些数据时特别有用。
 *
 * <p><b>使用场景:</b>
 * <ul>
 *   <li><b>随机访问:</b> 需要在数据中跳转到特定位置读取</li>
 *   <li><b>重复读取:</b> 需要多次读取相同的数据段</li>
 *   <li><b>索引读取:</b> 根据索引跳转到对应数据位置</li>
 *   <li><b>回溯读取:</b> 需要回退到之前的读取位置</li>
 * </ul>
 *
 * <p><b>实现要求:</b>
 * <ul>
 *   <li>实现类必须维护一个可调整的读取指针</li>
 *   <li>必须支持向前和向后定位</li>
 *   <li>定位操作应该是轻量级的,避免大量数据移动</li>
 * </ul>
 *
 * <p><b>使用示例:</b>
 * <pre>{@code
 * SeekableDataInputView view = ...;
 *
 * // 从位置 0 读取头部信息
 * view.setReadPosition(0);
 * int header = view.readInt();
 *
 * // 跳转到位置 1000 读取数据
 * view.setReadPosition(1000);
 * String data = view.readUTF();
 *
 * // 可以回退到之前的位置重新读取
 * view.setReadPosition(0);
 * int headerAgain = view.readInt(); // 再次读取头部
 * }</pre>
 *
 * <p><b>典型实现:</b>
 * <ul>
 *   <li>{@code DataInputDeserializer} - 基于字节数组,支持快速定位</li>
 *   <li>基于 {@link java.nio.ByteBuffer} 的实现</li>
 * </ul>
 *
 * <p><b>性能考虑:</b>
 * <ul>
 *   <li>定位操作通常比跳过读取更高效,因为不需要读取中间数据</li>
 *   <li>适用于基于内存的实现,对于基于流的实现可能效率较低</li>
 * </ul>
 *
 * @see DataInputView
 * @see DataInputDeserializer
 */
public interface SeekableDataInputView extends DataInputView {

    /**
     * 设置读取指针到给定的位置。
     *
     * <p>这个方法允许随机访问数据,可以向前或向后移动读取位置。
     * 下一个读取操作将从这个新位置开始。
     *
     * <p><b>位置语义:</b>
     * <ul>
     *   <li>位置 0 表示数据的开始</li>
     *   <li>位置必须在有效范围内 [0, dataLength]</li>
     *   <li>设置到 dataLength 表示到达末尾,下次读取会遇到 EOF</li>
     * </ul>
     *
     * <p><b>使用注意:</b>
     * <ul>
     *   <li>不要设置超出数据长度的位置,否则后续读取会失败</li>
     *   <li>频繁的随机定位可能影响性能,尽量批量读取</li>
     * </ul>
     *
     * @param position 新的读取位置,从 0 开始计数
     * @throws IllegalArgumentException 如果位置为负数或超出有效范围(可选)
     */
    void setReadPosition(long position);
}
