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

package org.apache.paimon.sst;

import org.apache.paimon.memory.MemorySlice;

import java.util.Map.Entry;

import static java.util.Objects.requireNonNull;

/**
 * 块条目,表示键值对。
 *
 * <p>该类实现了 {@link Entry} 接口,用于表示 SST 文件块中的键值对数据。
 * 键和值都使用 {@link MemorySlice} 来表示,以实现零拷贝访问。
 *
 * <p>特点:
 * <ul>
 *   <li>不可变 - 键和值在创建后不可修改
 *   <li>零拷贝 - 使用内存切片直接引用数据
 *   <li>类型安全 - 不允许 null 键或值
 * </ul>
 */
public class BlockEntry implements Entry<MemorySlice, MemorySlice> {

    /** 键 */
    private final MemorySlice key;

    /** 值 */
    private final MemorySlice value;

    /**
     * 构造块条目。
     *
     * @param key 键,不能为 null
     * @param value 值,不能为 null
     */
    public BlockEntry(MemorySlice key, MemorySlice value) {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        this.key = key;
        this.value = value;
    }

    /** 返回键。 */
    @Override
    public MemorySlice getKey() {
        return key;
    }

    /** 返回值。 */
    @Override
    public MemorySlice getValue() {
        return value;
    }

    /**
     * 不支持设置值操作。
     *
     * @param value 新值
     * @return 不返回
     * @throws UnsupportedOperationException 始终抛出
     */
    @Override
    public final MemorySlice setValue(MemorySlice value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockEntry entry = (BlockEntry) o;

        if (!key.equals(entry.key)) {
            return false;
        }
        return value.equals(entry.value);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
