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

package org.apache.paimon.codegen;

import org.apache.paimon.data.InternalRow;

import java.io.Serializable;
import java.util.Comparator;

/**
 * 记录比较器。
 *
 * <p>用于 {@code BinaryInMemorySortBuffer} 的记录比较。该接口继承了标准的 {@link Comparator} 接口,
 * 专门用于比较 {@link InternalRow} 对象。
 *
 * <p>为了性能考虑,该接口的子类通常通过 CodeGenerator 动态生成。这是一个帮助 JVM 内联优化的新接口。
 *
 * <p>主要用途:
 * <ul>
 *   <li>内存排序缓冲区中的记录排序</li>
 *   <li>多字段排序</li>
 *   <li>支持升序和降序排序</li>
 * </ul>
 */
public interface RecordComparator extends Comparator<InternalRow>, Serializable {

    /**
     * 比较两个记录。
     *
     * @param o1 第一个记录
     * @param o2 第二个记录
     * @return 比较结果: 负数表示 o1 < o2, 0 表示 o1 == o2, 正数表示 o1 > o2
     */
    @Override
    int compare(InternalRow o1, InternalRow o2);
}
