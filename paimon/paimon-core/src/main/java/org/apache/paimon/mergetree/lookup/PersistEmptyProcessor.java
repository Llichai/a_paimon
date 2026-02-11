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
 * 空持久化处理器
 *
 * <p>仅返回 {@link Boolean} 的 {@link PersistProcessor}。
 *
 * <p>特点：
 * <ul>
 *   <li>持久化：返回空字节数组（不存储任何值）
 *   <li>读取：始终返回 Boolean.TRUE（表示键存在）
 *   <li>不需要行位置信息
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>键存在性检查：只需要知道键是否存在
 *   <li>布隆过滤器补充：确认键真实存在
 *   <li>最小化存储：不需要存储值，只存储键
 * </ul>
 *
 * <p>示例：
 * <pre>
 * // 写入：key -> []（空字节）
 * // 查询：key -> Boolean.TRUE（存在）
 * </pre>
 */
public class PersistEmptyProcessor implements PersistProcessor<Boolean> {

    /** 空字节数组常量 */
    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * 不需要行位置
     *
     * @return false
     */
    @Override
    public boolean withPosition() {
        return false;
    }

    /**
     * 持久化到磁盘（返回空字节数组）
     *
     * @param kv 键值对
     * @return 空字节数组
     */
    @Override
    public byte[] persistToDisk(KeyValue kv) {
        return EMPTY_BYTES;
    }

    /**
     * 从磁盘读取（始终返回 TRUE）
     *
     * @param key 键
     * @param level 层级
     * @param bytes 字节数组
     * @param fileName 文件名
     * @return Boolean.TRUE
     */
    @Override
    public Boolean readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        return Boolean.TRUE;
    }

    /**
     * 创建工厂
     *
     * @return 工厂实例
     */
    public static Factory<Boolean> factory() {
        return new Factory<Boolean>() {
            @Override
            public String identifier() {
                return "empty";
            }

            @Override
            public PersistProcessor<Boolean> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistEmptyProcessor();
            }
        };
    }
}
