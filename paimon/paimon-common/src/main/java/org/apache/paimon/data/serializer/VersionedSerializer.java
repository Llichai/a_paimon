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

import java.io.IOException;

/**
 * 版本化序列化器接口 - 支持多版本序列化格式的简单接口。
 *
 * <p>这个接口专门设计用于需要维护多个序列化版本的场景,
 * 提供了向后兼容的序列化和反序列化能力。
 *
 * <p>主要应用场景:
 * <ul>
 *   <li>元数据演化: 允许元数据格式随时间演进而不破坏兼容性
 *   <li>滚动升级: 支持新旧版本的系统同时运行
 *   <li>数据迁移: 可以读取旧版本数据并升级到新格式
 *   <li>持久化存储: 长期存储的数据可能跨越多个版本
 * </ul>
 *
 * <p>版本管理策略:
 * <ul>
 *   <li>当前版本: {@link #getVersion()} 返回序列化时使用的版本号
 *   <li>兼容性: 必须能够反序列化所有历史版本的数据
 *   <li>版本号递增: 新版本号应该大于旧版本号
 *   <li>不可变性: 已发布的版本格式不应改变
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 实现版本化序列化器
 * public class MyVersionedSerializer implements VersionedSerializer<MyData> {
 *     private static final int CURRENT_VERSION = 2;
 *
 *     @Override
 *     public int getVersion() {
 *         return CURRENT_VERSION;
 *     }
 *
 *     @Override
 *     public byte[] serialize(MyData obj) throws IOException {
 *         // 使用当前版本(v2)的格式序列化
 *         return serializeV2(obj);
 *     }
 *
 *     @Override
 *     public MyData deserialize(int version, byte[] serialized) throws IOException {
 *         switch (version) {
 *             case 1:
 *                 // 读取 v1 格式并转换
 *                 return deserializeV1(serialized);
 *             case 2:
 *                 // 读取 v2 格式
 *                 return deserializeV2(serialized);
 *             default:
 *                 throw new IOException("Unsupported version: " + version);
 *         }
 *     }
 * }
 *
 * // 使用序列化器
 * MyVersionedSerializer serializer = new MyVersionedSerializer();
 *
 * // 序列化(使用当前版本)
 * MyData data = new MyData();
 * byte[] bytes = serializer.serialize(data);
 * int version = serializer.getVersion();
 *
 * // 保存版本号和数据
 * storage.writeInt(version);
 * storage.write(bytes);
 *
 * // 读取时指定版本
 * int storedVersion = storage.readInt();
 * byte[] storedBytes = storage.read();
 * MyData restored = serializer.deserialize(storedVersion, storedBytes);
 * }</pre>
 *
 * <p>典型的版本演化模式:
 * <pre>
 * Version 1: 基础格式
 *   [field1][field2]
 *
 * Version 2: 添加新字段
 *   [field1][field2][field3]
 *   反序列化 v1 时,field3 使用默认值
 *
 * Version 3: 修改字段类型
 *   [field1][field2_new_type][field3]
 *   反序列化 v2 时,转换 field2 的类型
 * </pre>
 *
 * <p>与普通序列化器的区别:
 * <ul>
 *   <li>{@link Serializer}: 单一版本,无版本管理,性能更高
 *   <li>{@link VersionedSerializer}: 多版本支持,向后兼容,更灵活
 * </ul>
 *
 * <p>最佳实践:
 * <ul>
 *   <li>版本号外部存储: 版本号通常独立于序列化数据存储
 *   <li>测试兼容性: 为每个版本维护测试数据,确保向后兼容
 *   <li>文档化格式: 详细记录每个版本的格式变化
 *   <li>平滑升级: 新版本应该能够读取和处理所有旧版本数据
 * </ul>
 *
 * @param <T> 要序列化的对象类型
 */
public interface VersionedSerializer<T> {

    /**
     * 获取序列化器序列化时使用的版本号。
     *
     * <p>这个版本号表示当前序列化器生成的数据格式版本。
     * 调用 {@link #serialize} 时,数据将使用这个版本的格式进行序列化。
     *
     * @return 序列化模式的版本号
     */
    int getVersion();

    /**
     * 序列化给定对象。
     *
     * <p>序列化使用当前版本(由 {@link #getVersion()} 返回)的格式。
     *
     * @param obj 要序列化的对象
     * @return 序列化后的数据(字节数组)
     * @throws IOException 如果序列化失败
     */
    byte[] serialize(T obj) throws IOException;

    /**
     * 反序列化指定版本格式的数据。
     *
     * <p>这个方法必须能够处理所有历史版本的数据格式,
     * 提供向后兼容性。对于旧版本的数据,可能需要:
     * <ul>
     *   <li>格式转换: 将旧格式转换为新格式
     *   <li>默认值填充: 为新增字段提供默认值
     *   <li>类型转换: 处理字段类型的变化
     * </ul>
     *
     * @param version 数据序列化时使用的版本号
     * @param serialized 序列化的数据
     * @return 反序列化的对象
     * @throws IOException 如果反序列化失败(包括不支持的版本)
     */
    T deserialize(int version, byte[] serialized) throws IOException;
}
