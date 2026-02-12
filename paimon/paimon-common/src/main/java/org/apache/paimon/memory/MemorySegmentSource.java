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

import javax.annotation.Nullable;

/**
 * 内存段源接口,定义了提供内存段的能力。
 *
 * <p>这是一个轻量级的接口,用于抽象内存段的获取操作。实现类可以是:
 *
 * <ul>
 *   <li>内存池 ({@link MemorySegmentPool}):复用已分配的内存段
 *   <li>按需分配器:动态创建新的内存段
 *   <li>有限资源管理器:在资源耗尽时返回 null
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <p>该接口主要用于需要持续获取内存段的组件,如写缓冲区、排序器等。
 *
 * @since 0.4.0
 */
@Public
public interface MemorySegmentSource {

    /**
     * 获取下一个内存段。
     *
     * <p>如果没有可用的内存段,返回 null。调用者需要处理 null 情况,可能的策略包括:
     *
     * <ul>
     *   <li>等待内存段被归还
     *   <li>刷新数据释放内存
     *   <li>抛出内存不足异常
     * </ul>
     *
     * @return 下一个可用的内存段,如果无可用内存则返回 null
     */
    @Nullable
    MemorySegment nextSegment();
}
