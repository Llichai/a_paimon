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

package org.apache.paimon.memory;

import java.util.List;

/**
 * 无缓存的内存段池实现。
 *
 * <p>该实现与 {@link HeapMemorySegmentPool} 的主要区别在于:
 *
 * <ul>
 *   <li>不缓存归还的内存段,归还后立即丢弃
 *   <li>每次 {@link #nextSegment()} 都分配新的内存段
 *   <li>适用于内存段生命周期较长,不需要频繁复用的场景
 * </ul>
 *
 * <h2>性能考虑</h2>
 *
 * <ul>
 *   <li>减少内存池的内存占用,归还后由 GC 回收
 *   <li>避免了缓存队列的管理开销
 *   <li>适合内存段使用模式分散、复用率低的场景
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <p>适合短时间内大量分配内存,但不需要长期持有的场景,如批处理任务。
 */
public class CachelessSegmentPool implements MemorySegmentPool {

    /** 最大页数限制。 */
    private final int maxPages;

    /** 每页的大小(字节数)。 */
    private final int pageSize;

    /** 当前已分配的页数。 */
    private int numPage;

    /**
     * 构造无缓存内存段池。
     *
     * @param maxMemory 最大内存容量(字节)
     * @param pageSize 每页大小(字节)
     */
    public CachelessSegmentPool(long maxMemory, int pageSize) {
        this.maxPages = (int) (maxMemory / pageSize);
        this.pageSize = pageSize;
        this.numPage = 0;
    }

    @Override
    public MemorySegment nextSegment() {
        if (numPage < maxPages) {
            numPage++;
            return MemorySegment.allocateHeapMemory(pageSize);
        }

        return null;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        numPage -= memory.size();
    }

    @Override
    public int freePages() {
        return maxPages - numPage;
    }
}
