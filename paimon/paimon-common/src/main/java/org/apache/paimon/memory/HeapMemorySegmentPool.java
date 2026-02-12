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

/**
 * 堆内存段池实现。
 *
 * <p>这是 {@link AbstractMemorySegmentPool} 的具体实现,分配和管理堆内存段(基于 byte[])。
 *
 * <h2>特点</h2>
 *
 * <ul>
 *   <li>内存由 JVM 垃圾回收器管理,无需手动释放
 *   <li>适用于需要频繁分配和释放内存的场景
 *   <li>性能受 GC 影响,但避免了堆外内存泄漏风险
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <p>适合数据处理、排序、写缓冲等需要临时内存的场景。
 */
public class HeapMemorySegmentPool extends AbstractMemorySegmentPool {

    /**
     * 构造堆内存段池。
     *
     * @param maxMemory 最大内存容量(字节)
     * @param pageSize 每页大小(字节)
     */
    public HeapMemorySegmentPool(long maxMemory, int pageSize) {
        super(maxMemory, pageSize);
    }

    @Override
    protected MemorySegment allocateMemory() {
        return MemorySegment.allocateHeapMemory(pageSize);
    }
}
