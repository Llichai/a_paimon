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

import java.util.Objects;

/**
 * Bloom Filter 句柄,用于定位 Bloom Filter 在文件中的位置。
 *
 * <p>包含 Bloom Filter 的偏移量、大小和预期条目数,用于读取和验证 Bloom Filter。
 */
public class BloomFilterHandle {

    /** Bloom Filter 在文件中的偏移量 */
    private final long offset;

    /** Bloom Filter 的大小(字节) */
    private final int size;

    /** 预期的条目数 */
    private final long expectedEntries;

    /**
     * 构造 Bloom Filter 句柄。
     *
     * @param offset 文件偏移量
     * @param size 大小
     * @param expectedEntries 预期条目数
     */
    public BloomFilterHandle(long offset, int size, long expectedEntries) {
        this.offset = offset;
        this.size = size;
        this.expectedEntries = expectedEntries;
    }

    /** 返回偏移量。 */
    public long offset() {
        return offset;
    }

    /** 返回大小。 */
    public int size() {
        return size;
    }

    /** 返回预期条目数。 */
    public long expectedEntries() {
        return expectedEntries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BloomFilterHandle that = (BloomFilterHandle) o;
        return offset == that.offset
                && size == that.size
                && expectedEntries == that.expectedEntries;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, size, expectedEntries);
    }
}
