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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;

import java.io.IOException;

/**
 * 抽象行数据序列化器 - {@link InternalRow} 序列化器的基类。
 *
 * <p>这个抽象类扩展了 {@link PagedTypeSerializer},为行数据序列化器提供了通用接口。
 * 它定义了行序列化器必须实现的两个核心方法。
 *
 * <p>核心功能:
 * <ul>
 *   <li>提供分页序列化支持: 继承自 {@link PagedTypeSerializer}
 *   <li>定义行特定接口: {@link #getArity()} 和 {@link #toBinaryRow}
 *   <li>统一行序列化抽象: 所有行序列化器的公共基类
 * </ul>
 *
 * <p>子类实现:
 * <ul>
 *   <li>{@link BinaryRowSerializer}: 针对 {@link BinaryRow} 的高性能序列化器
 *   <li>{@link InternalRowSerializer}: 通用的 {@link InternalRow} 序列化器
 *   <li>其他自定义行序列化器
 * </ul>
 *
 * <p>设计模式:
 * <ul>
 *   <li>模板方法模式: 定义序列化的框架,子类实现具体细节
 *   <li>类型参数化: 支持不同的行类型(BinaryRow, InternalRow 等)
 * </ul>
 *
 * @param <T> 行类型,必须是 {@link InternalRow} 的子类
 * @see InternalRow
 * @see BinaryRow
 * @see PagedTypeSerializer
 */
public abstract class AbstractRowDataSerializer<T extends InternalRow>
        implements PagedTypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    /**
     * 获取行的字段数量。
     *
     * <p>也称为"列数"或"arity"。这是行模式定义的固定值。
     *
     * @return 字段数量
     */
    public abstract int getArity();

    /**
     * 将 {@link InternalRow} 转换为 {@link BinaryRow}。
     *
     * <p>这是一个核心转换方法,用于:
     * <ul>
     *   <li>统一序列化格式: 所有行最终都转换为 BinaryRow
     *   <li>优化性能: BinaryRow 提供了最高效的序列化支持
     *   <li>零拷贝: 如果输入已经是 BinaryRow,可以直接返回
     * </ul>
     *
     * @param rowData 要转换的行数据
     * @return BinaryRow 表示
     * @throws IOException 如果转换过程中发生 I/O 错误
     */
    public abstract BinaryRow toBinaryRow(T rowData) throws IOException;
}
