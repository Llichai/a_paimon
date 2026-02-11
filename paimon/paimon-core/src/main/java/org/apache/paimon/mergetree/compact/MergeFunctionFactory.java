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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * 合并函数工厂接口，用于创建 {@link MergeFunction} 实例
 *
 * <p>工厂模式允许根据不同的配置和场景创建不同的合并函数实例。
 * 支持 schema 演化场景，通过 readType 参数可以创建适配不同 schema 的合并函数。
 */
@FunctionalInterface
public interface MergeFunctionFactory<T> extends Serializable {

    /**
     * 创建默认的合并函数实例
     *
     * <p>使用原始 schema（写入时的 schema）创建合并函数。
     *
     * @return 合并函数实例
     */
    default MergeFunction<T> create() {
        return create(null);
    }

    /**
     * 创建支持 schema 演化的合并函数实例
     *
     * <p>当读取旧数据时，readType 可能与写入时的 schema 不同（字段增删、类型变化等）。
     * 合并函数需要根据 readType 调整其行为，确保正确处理不同 schema 的数据。
     *
     * @param readType 读取时的行类型（schema），null 表示使用写入时的 schema
     * @return 合并函数实例
     */
    MergeFunction<T> create(@Nullable RowType readType);

    /**
     * 调整读取类型（用于 schema 演化）
     *
     * <p>某些合并函数可能需要调整 readType，例如：
     * <ul>
     *   <li>添加额外的内部字段
     *   <li>移除不兼容的字段
     *   <li>调整字段顺序
     * </ul>
     *
     * <p>默认实现：不调整，直接返回原 readType。
     *
     * @param readType 原始读取类型
     * @return 调整后的读取类型
     */
    default RowType adjustReadType(RowType readType) {
        return readType;
    }
}
