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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * Variant 类型的数据类型定义。
 *
 * <p>Variant 是一种灵活的数据类型,可以存储不同类型的值,类似于 JSON 或动态类型。
 * 这种类型特别适合存储半结构化数据或需要灵活 schema 的场景。
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li>类型灵活性 - 可以存储多种不同类型的值
 *   <li>半结构化数据 - 适合 JSON、XML 等半结构化数据
 *   <li>动态 schema - 无需预先定义固定的数据结构
 *   <li>较大默认大小 - 默认 2048 字节以容纳复杂数据
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储 JSON 文档
 *   <li>处理动态或不确定的数据结构
 *   <li>数据集成场景中的临时数据存储
 *   <li>需要保留原始数据格式的场景
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建 Variant 类型
 * VariantType variantType = new VariantType();
 *
 * // 创建不可空的 Variant 类型
 * VariantType nonNullVariant = new VariantType(false);
 * }</pre>
 *
 * @since 1.1.0
 */
@Public
public class VariantType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 字符串格式。 */
    private static final String FORMAT = "VARIANT";

    /**
     * 创建 Variant 类型。
     *
     * @param isNullable 是否可为 NULL
     */
    public VariantType(boolean isNullable) {
        super(isNullable, DataTypeRoot.VARIANT);
    }

    /**
     * 创建可空的 Variant 类型。
     */
    public VariantType() {
        this(true);
    }

    /**
     * 返回此类型的默认大小(字节)。
     *
     * <p>Variant 类型的默认大小为 2048 字节,以容纳较复杂的半结构化数据。
     *
     * @return 2048 字节
     */
    @Override
    public int defaultSize() {
        return 2048;
    }

    /**
     * 创建具有指定可空性的 Variant 类型副本。
     *
     * @param isNullable 新实例是否可为 NULL
     * @return 新的 Variant 类型实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new VariantType(isNullable);
    }

    /**
     * 生成 SQL 字符串表示。
     *
     * <p>格式: VARIANT [NOT NULL]
     *
     * @return SQL 字符串表示
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
    }

    /**
     * 接受访问者访问。
     *
     * @param visitor 数据类型访问者
     * @param <R> 返回类型
     * @return 访问结果
     */
    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
