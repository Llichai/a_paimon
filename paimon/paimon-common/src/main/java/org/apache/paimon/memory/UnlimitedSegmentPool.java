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

import javax.annotation.Nullable;

import java.util.List;

/**
 * 无限制的堆内存段池实现。
 *
 * <p>该实现没有内存容量限制,可以无限分配新的内存段,直到 JVM 堆内存耗尽。
 *
 * <h2>特点</h2>
 *
 * <ul>
 *   <li>每次 {@link #nextSegment()} 都分配新的堆内存段,永远不返回 null
 *   <li>{@link #freePages()} 始终返回 Integer.MAX_VALUE
 *   <li>{@link #returnAll(List)} 不执行任何操作,直接丢弃归还的内存段
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>测试环境,不需要内存限制
 *   <li>内存充足的环境,简化内存管理
 *   <li>临时性任务,不关心内存复用
 * </ul>
 *
 * <h2>风险警告</h2>
 *
 * <p>使用该池可能导致 OutOfMemoryError,建议仅在测试或内存管理由外部保证的场景下使用。
 */
public class UnlimitedSegmentPool implements MemorySegmentPool {

    /** 每页的大小(字节数)。 */
    private final int pageSize;

    /**
     * 构造无限制内存段池。
     *
     * @param pageSize 每页大小(字节)
     */
    public UnlimitedSegmentPool(int pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    /**
     * 不执行任何操作,直接丢弃归还的内存段,由 GC 回收。
     *
     * @param memory 要归还的内存段列表(被忽略)
     */
    @Override
    public void returnAll(List<MemorySegment> memory) {}

    @Override
    public int freePages() {
        return Integer.MAX_VALUE;
    }

    /**
     * 分配新的堆内存段。
     *
     * @return 新分配的内存段,永不返回 null
     */
    @Nullable
    @Override
    public MemorySegment nextSegment() {
        return MemorySegment.allocateHeapMemory(pageSize);
    }
}
