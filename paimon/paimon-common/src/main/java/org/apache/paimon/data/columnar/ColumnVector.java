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

package org.apache.paimon.data.columnar;

/**
 * 列向量的基础接口,表示一列可为空的数据。
 *
 * <p>列向量是列式存储的核心抽象,将一列数据存储为连续的向量,便于批量处理和 SIMD 优化。
 * 这个接口定义了所有列向量的通用行为,具体的数据访问需要通过特定的子接口完成。
 *
 * <h2>设计模式</h2>
 * <ul>
 *   <li>使用接口模式定义列向量的统一抽象
 *   <li>通过子接口(如 {@link IntColumnVector}, {@link BooleanColumnVector})提供类型特定的访问方法
 *   <li>支持嵌套结构,通过 getChildren() 访问复杂类型(Array/Map/Row)的子向量
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>批量读取和处理列式数据
 *   <li>向量化执行引擎的数据表示
 *   <li>与 Parquet、ORC 等列式格式的数据交互
 * </ul>
 *
 * <h2>实现要求</h2>
 * <ul>
 *   <li>所有列向量都必须支持 NULL 值检查
 *   <li>具体的数据访问方法由类型特定的子接口定义
 *   <li>嵌套类型需要实现 getChildren() 返回子向量数组
 * </ul>
 *
 * @see BooleanColumnVector 布尔列向量接口
 * @see IntColumnVector 整数列向量接口
 * @see BytesColumnVector 字节数组列向量接口
 * @see ArrayColumnVector 数组列向量接口
 * @see RowColumnVector 行列向量接口
 * @see MapColumnVector Map列向量接口
 */
public interface ColumnVector {

    /**
     * 检查指定位置的值是否为 NULL。
     *
     * @param i 行索引(从0开始)
     * @return 如果该位置的值为 NULL 返回 true,否则返回 false
     */
    boolean isNullAt(int i);

    /**
     * 获取此列向量的容量(最大行数)。
     *
     * <p>默认实现返回 Integer.MAX_VALUE,表示容量不受限制。
     * 具体实现可以覆盖此方法返回实际的容量限制。
     *
     * @return 列向量的最大容量
     */
    default int getCapacity() {
        return Integer.MAX_VALUE;
    }

    /**
     * 获取此列向量的子向量数组(用于复杂类型)。
     *
     * <p>对于简单类型(int, long, boolean等),此方法返回 null。
     * 对于复杂类型:
     * <ul>
     *   <li>Array类型: 返回包含元素向量的数组
     *   <li>Map类型: 返回包含key向量和value向量的数组
     *   <li>Row类型: 返回包含所有字段向量的数组
     * </ul>
     *
     * @return 子向量数组,简单类型返回 null
     */
    default ColumnVector[] getChildren() {
        return null;
    }
}
