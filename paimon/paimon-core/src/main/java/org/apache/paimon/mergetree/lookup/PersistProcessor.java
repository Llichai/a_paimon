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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/**
 * 持久化处理器接口
 *
 * <p>处理值的持久化和读取。
 *
 * <p>核心功能：
 * <ul>
 *   <li>persistToDisk：将 KeyValue 持久化到磁盘（字节数组）
 *   <li>readFromDisk：从磁盘读取并构造结果对象
 *   <li>withPosition：是否需要行位置信息
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link PersistEmptyProcessor}：只返回 Boolean（键存在性检查）
 *   <li>{@link PersistPositionProcessor}：返回 FilePosition（Deletion Vector）
 *   <li>{@link PersistValueProcessor}：返回完整 KeyValue
 *   <li>PersistValueAndPosProcessor：返回 PositionedKeyValue
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：根据需求选择不同的持久化策略
 *   <li>Deletion Vector：仅持久化行位置信息
 *   <li>完整查询：持久化完整的值和元数据
 * </ul>
 */
public interface PersistProcessor<T> {

    /**
     * 是否需要行位置信息
     *
     * @return true 表示需要行位置
     */
    boolean withPosition();

    /**
     * 持久化到磁盘（不带行位置）
     *
     * @param kv 键值对
     * @return 字节数组
     */
    byte[] persistToDisk(KeyValue kv);

    /**
     * 持久化到磁盘（带行位置）
     *
     * @param kv 键值对
     * @param rowPosition 行位置
     * @return 字节数组
     */
    default byte[] persistToDisk(KeyValue kv, long rowPosition) {
        throw new UnsupportedOperationException();
    }

    /**
     * 从磁盘读取
     *
     * @param key 键
     * @param level 层级
     * @param valueBytes 值字节数组
     * @param fileName 文件名
     * @return 结果对象
     */
    T readFromDisk(InternalRow key, int level, byte[] valueBytes, String fileName);

    /**
     * 持久化处理器工厂接口
     */
    interface Factory<T> {

        /**
         * 获取标识符
         *
         * @return 标识符（如 "empty", "position", "value"）
         */
        String identifier();

        /**
         * 创建持久化处理器
         *
         * @param fileSerVersion 文件序列化版本
         * @param serializerFactory 序列化器工厂
         * @param fileSchema 文件 Schema
         * @return 持久化处理器
         */
        PersistProcessor<T> create(
                String fileSerVersion,
                LookupSerializerFactory serializerFactory,
                @Nullable RowType fileSchema);
    }
}
