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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;

/**
 * 投影接口。
 *
 * <p>用于代码生成的投影,可以将一个 RowData 映射到另一个 BinaryRowData。
 * 投影通常用于字段选择和重排序,是数据处理中的常见操作。
 *
 * <p>该接口的实现类通常由代码生成器动态生成,以获得最佳性能。
 *
 * <p>典型用途:
 * <ul>
 *   <li>字段选择 - 从输入行中选择特定字段</li>
 *   <li>字段重排序 - 改变字段的顺序</li>
 *   <li>字段复制 - 复制某些字段</li>
 * </ul>
 */
public interface Projection {

    /**
     * 应用投影。
     *
     * <p>将输入行根据预定义的映射关系转换为输出行。
     *
     * @param row 输入行
     * @return 投影后的二进制行
     */
    BinaryRow apply(InternalRow row);
}
