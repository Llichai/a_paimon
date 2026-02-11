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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;

import javax.annotation.Nullable;

import java.util.List;
import java.util.OptionalLong;

/**
 * {@link Split} 的包装类，添加查询授权信息。
 *
 * <p>QueryAuthSplit 用于实现细粒度的数据访问控制，在 Split 基础上添加授权结果，
 * 包括：列级别权限、行级别过滤等。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>列级别权限</b>: 限制可访问的列</li>
 *   <li><b>行级别权限</b>: 添加额外的过滤条件</li>
 *   <li><b>数据脱敏</b>: 对敏感列进行脱敏处理</li>
 * </ul>
 *
 * <h3>工作流程</h3>
 * <ol>
 *   <li>扫描阶段：生成 Split</li>
 *   <li>授权阶段：将 Split 包装为 QueryAuthSplit，添加授权信息</li>
 *   <li>读取阶段：根据授权信息过滤列和行</li>
 * </ol>
 *
 * @see Split 分片接口
 * @see TableQueryAuthResult 查询授权结果
 */
public class QueryAuthSplit implements Split {

    private static final long serialVersionUID = 1L;

    private final Split split;
    private final TableQueryAuthResult authResult;

    public QueryAuthSplit(Split split, TableQueryAuthResult authResult) {
        this.split = split;
        this.authResult = authResult;
    }

    public Split split() {
        return split;
    }

    @Nullable
    public TableQueryAuthResult authResult() {
        return authResult;
    }

    @Override
    public long rowCount() {
        return split.rowCount();
    }

    @Override
    public OptionalLong mergedRowCount() {
        List<String> filter = authResult.filter();
        if (filter != null && !filter.isEmpty()) {
            return OptionalLong.empty();
        }
        return split.mergedRowCount();
    }
}
