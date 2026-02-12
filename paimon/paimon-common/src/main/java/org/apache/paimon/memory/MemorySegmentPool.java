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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.options.MemorySize;

import java.util.List;

/**
 * 内存段池接口,用于管理内存页的分配和回收。
 *
 * <p>MemorySegmentPool 是 Paimon 内存管理的核心抽象,负责:
 *
 * <ul>
 *   <li>统一管理内存段(MemorySegment)的生命周期
 *   <li>支持内存段的分配和批量回收
 *   <li>跟踪可用内存页数量
 *   <li>提供固定大小的内存页,简化内存管理
 * </ul>
 *
 * <h2>设计模式</h2>
 *
 * <p>该接口实现了对象池模式,通过复用内存段对象减少 GC 压力和内存分配开销。
 *
 * <h2>线程安全性</h2>
 *
 * <p>实现类需要根据具体使用场景决定是否提供线程安全保证。
 *
 * @since 0.4.0
 */
@Public
public interface MemorySegmentPool extends MemorySegmentSource {

    /** 默认页大小:32 KB。这是性能和内存利用率之间的平衡值。 */
    int DEFAULT_PAGE_SIZE = 32 * 1024;

    /**
     * 获取此内存池中每个页的大小。
     *
     * @return 页大小(字节数)
     */
    int pageSize();

    /**
     * 将内存段批量归还到池中。
     *
     * <p>归还的内存段可以在后续的 {@link #nextSegment()} 调用中被重用。
     *
     * @param memory 要归还的内存段列表
     */
    void returnAll(List<MemorySegment> memory);

    /**
     * 获取当前可用的空闲页数量。
     *
     * @return 空闲页数量
     */
    int freePages();

    /**
     * 创建基于堆内存的内存段池。
     *
     * @param maxMemory 最大内存容量
     * @param pageSize 每页大小
     * @return 新创建的堆内存段池
     */
    static MemorySegmentPool createHeapPool(MemorySize maxMemory, MemorySize pageSize) {
        return new HeapMemorySegmentPool(maxMemory.getBytes(), (int) pageSize.getBytes());
    }
}
