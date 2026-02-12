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

/**
 * 记录相等判断器。
 *
 * <p>用于判断两个 RowData 是否相等的接口。该接口可以比较两个 {@link InternalRow} 对象,
 * 并返回它们是否相等。
 *
 * <p>该接口通常由代码生成器动态生成,以获得最佳性能。生成的代码会针对特定的字段类型
 * 和字段列表进行优化。
 *
 * <p>主要用途:
 * <ul>
 *   <li>去重操作</li>
 *   <li>连接操作中的键比较</li>
 *   <li>分组操作中的键比较</li>
 * </ul>
 */
public interface RecordEqualiser extends Serializable {

    /**
     * 判断两个行是否相等。
     *
     * @param row1 第一个行
     * @param row2 第二个行
     * @return 如果行相等返回 {@code true},否则返回 {@code false}
     */
    boolean equals(InternalRow row1, InternalRow row2);
}
