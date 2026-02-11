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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.base.Supplier;
import org.apache.paimon.shade.guava30.com.google.common.base.Suppliers;

import javax.annotation.Nullable;

import java.util.function.Function;

/**
 * Lookup 序列化器工厂接口
 *
 * <p>创建 Lookup 场景的序列化器和反序列化器。
 *
 * <p>核心功能：
 * <ul>
 *   <li>version：获取序列化版本
 *   <li>createSerializer：创建序列化器（InternalRow -> byte[]）
 *   <li>createDeserializer：创建反序列化器（byte[] -> InternalRow）
 * </ul>
 *
 * <p>版本兼容性：
 * <ul>
 *   <li>支持不同版本间的序列化兼容
 *   <li>反序列化时可能需要 Schema 演化
 *   <li>fileSerVersion：文件的序列化版本
 *   <li>fileSchema：文件的原始 Schema（可能与当前 Schema 不同）
 * </ul>
 *
 * <p>实现：
 * <ul>
 *   <li>{@link DefaultLookupSerializerFactory}：默认实现
 * </ul>
 *
 * <p>单例获取：
 * <pre>
 * LookupSerializerFactory factory = LookupSerializerFactory.INSTANCE.get();
 * </pre>
 */
public interface LookupSerializerFactory {

    /**
     * 获取序列化版本
     *
     * @return 版本字符串
     */
    String version();

    /**
     * 创建序列化器
     *
     * @param currentSchema 当前 Schema
     * @return 序列化器（InternalRow -> byte[]）
     */
    Function<InternalRow, byte[]> createSerializer(RowType currentSchema);

    /**
     * 创建反序列化器
     *
     * @param fileSerVersion 文件的序列化版本
     * @param currentSchema 当前 Schema
     * @param fileSchema 文件的原始 Schema（可能为 null）
     * @return 反序列化器（byte[] -> InternalRow）
     */
    Function<byte[], InternalRow> createDeserializer(
            String fileSerVersion, RowType currentSchema, @Nullable RowType fileSchema);

    /** 单例实例（懒加载） */
    Supplier<LookupSerializerFactory> INSTANCE =
            Suppliers.memoize(
                    () ->
                            FactoryUtil.discoverSingletonFactory(
                                            LookupSerializerFactory.class.getClassLoader(),
                                            LookupSerializerFactory.class)
                                    .orElseGet(DefaultLookupSerializerFactory::new));
}
