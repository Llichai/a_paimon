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

package org.apache.paimon.data.columnar.writable;

import org.apache.paimon.data.columnar.BytesColumnVector;

/**
 * 可写字节数组列向量接口。
 *
 * <p>WritableBytesVector 扩展了只读的 {@link BytesColumnVector},提供字节数组(变长二进制数据)的写入能力。
 * 适用于存储字符串、二进制数据、序列化对象等变长数据。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过putByteArray()在指定位置写入字节数组
 *   <li><b>追加写入</b>: 通过appendByteArray()追加字节数组
 *   <li><b>填充操作</b>: 通过fill()用指定数组填充整个向量
 * </ul>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>存储字符串数据(UTF-8编码的字节数组)
 *   <li>存储二进制数据(如图片、文件内容)
 *   <li>存储序列化后的对象
 *   <li>存储VARCHAR、VARBINARY等变长类型
 *   <li>存储JSON、XML等文本格式数据
 * </ul>
 *
 * <h2>与基本类型向量的区别</h2>
 * <ul>
 *   <li><b>变长存储</b>: 每个元素可以有不同的长度,而基本类型向量每个元素长度固定
 *   <li><b>引用语义</b>: 存储的是字节数组的引用或拷贝,不是原始值
 *   <li><b>内存管理</b>: 需要额外管理偏移量和长度信息
 *   <li><b>性能考虑</b>: 访问速度比基本类型向量稍慢,但更灵活
 * </ul>
 *
 * <h2>写入顺序约束</h2>
 * <p><b>重要</b>: putByteArray()方法要求按rowId顺序追加,不支持随机写入。这是因为:
 * <ul>
 *   <li>内部通常使用连续的字节缓冲区存储所有数据
 *   <li>每个元素的偏移量依赖于前一个元素的位置和长度
 *   <li>随机写入会破坏偏移量的连续性
 * </ul>
 *
 * <h2>内存布局</h2>
 * 字节数组向量通常采用以下存储结构:
 * <ul>
 *   <li><b>数据缓冲区</b>: 连续存储所有字节数组的内容
 *   <li><b>偏移量数组</b>: 记录每个元素在数据缓冲区中的起始位置
 *   <li><b>长度数组</b>: 记录每个元素的长度(可选,有的实现通过偏移量差值计算)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的字节数组向量
 * WritableBytesVector vector = new HeapBytesVector(100);
 *
 * // 存储字符串(转换为UTF-8字节数组)
 * String str1 = "Hello";
 * byte[] bytes1 = str1.getBytes(StandardCharsets.UTF_8);
 * vector.putByteArray(0, bytes1, 0, bytes1.length);
 *
 * String str2 = "World";
 * byte[] bytes2 = str2.getBytes(StandardCharsets.UTF_8);
 * vector.putByteArray(1, bytes2, 0, bytes2.length);
 *
 * // 追加字符串
 * String str3 = "Apache Paimon";
 * byte[] bytes3 = str3.getBytes(StandardCharsets.UTF_8);
 * vector.appendByteArray(bytes3, 0, bytes3.length);
 *
 * // 存储部分字节数组
 * byte[] largeArray = new byte[1000];
 * vector.appendByteArray(largeArray, 100, 50);  // 只存储[100, 150)的部分
 *
 * // 存储序列化对象
 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
 * ObjectOutputStream oos = new ObjectOutputStream(baos);
 * oos.writeObject(myObject);
 * byte[] serialized = baos.toByteArray();
 * vector.appendByteArray(serialized, 0, serialized.length);
 *
 * // 填充整个向量(所有位置都指向相同的字节数组)
 * byte[] defaultValue = "N/A".getBytes(StandardCharsets.UTF_8);
 * vector.fill(defaultValue);
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>预先reserve()足够容量,避免频繁扩容
 *   <li>尽量重用字节数组对象,减少GC压力
 *   <li>对于固定长度的数据,考虑使用基本类型向量
 *   <li>大数据量时注意内存占用,及时释放不需要的引用
 * </ul>
 *
 * <h2>写入模式对比</h2>
 * <ul>
 *   <li><b>putByteArray(rowId, ...)</b>: 在指定位置写入,但必须按顺序,不能跳过位置
 *   <li><b>appendByteArray(...)</b>: 在末尾追加,自动管理位置和扩容
 * </ul>
 *
 * <h2>字符串存储最佳实践</h2>
 * <pre>{@code
 * // 推荐: 使用UTF-8编码
 * String str = "中文字符串";
 * byte[] utf8Bytes = str.getBytes(StandardCharsets.UTF_8);
 * vector.appendByteArray(utf8Bytes, 0, utf8Bytes.length);
 *
 * // 读取时解码
 * byte[] stored = vector.getBytes(0).getBytes();
 * String decoded = new String(stored, StandardCharsets.UTF_8);
 * }</pre>
 *
 * <h2>NULL值处理</h2>
 * <ul>
 *   <li>字节数组向量支持NULL值,表示"缺失"或"未知"
 *   <li>NULL与空字节数组(length=0)是不同的
 *   <li>通过setNullAt()方法设置NULL
 *   <li>通过isNullAt()方法判断是否为NULL
 * </ul>
 *
 * <h2>与HeapBytesVector的关系</h2>
 * <ul>
 *   <li>WritableBytesVector是接口,定义写入操作
 *   <li>HeapBytesVector是实现,使用堆内存存储
 *   <li>HeapBytesVector通常用于构建数据,构建完成后可作为只读向量使用
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableBytesVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see BytesColumnVector 只读字节数组列向量接口
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableBytesVector extends WritableColumnVector, BytesColumnVector {

    /**
     * 在指定位置追加字节数组。
     *
     * <p><b>重要约束</b>: 必须按照rowId的顺序追加值,不能随机写入。这是因为内部存储结构
     * 依赖于顺序偏移量,随机写入会破坏数据结构的连续性。
     *
     * <p>正确用法示例:
     * <pre>{@code
     * vector.putByteArray(0, bytes1, 0, len1);  // 正确
     * vector.putByteArray(1, bytes2, 0, len2);  // 正确
     * vector.putByteArray(2, bytes3, 0, len3);  // 正确
     * }</pre>
     *
     * <p>错误用法示例:
     * <pre>{@code
     * vector.putByteArray(2, bytes3, 0, len3);  // 错误! 跳过了0和1
     * vector.putByteArray(0, bytes1, 0, len1);  // 错误! 不是顺序
     * }</pre>
     *
     * @param rowId 行ID,必须等于当前已追加的元素数量
     * @param value 源字节数组
     * @param offset 源数组的起始偏移量
     * @param length 要复制的字节数
     */
    void putByteArray(int rowId, byte[] value, int offset, int length);

    /**
     * 追加字节数组到向量末尾。
     *
     * <p>该方法会自动管理追加位置和容量,如果空间不足会自动扩容。
     *
     * @param value 源字节数组
     * @param offset 源数组的起始偏移量
     * @param length 要复制的字节数
     */
    void appendByteArray(byte[] value, int offset, int length);

    /**
     * 用指定的字节数组填充整个列向量。
     *
     * <p>填充后,向量中所有位置都指向相同的字节数组内容(可能是拷贝)。
     *
     * @param value 填充用的字节数组
     */
    void fill(byte[] value);
}
