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

/**
 * 表查询授权接口，用于实现细粒度的数据访问控制。
 *
 * <p>TableQueryAuth 提供了查询授权功能，可以根据用户选择的列（SELECT 子句）
 * 返回授权结果，包括：允许访问的列、必须应用的行级别过滤条件等。
 *
 * <h3>授权流程</h3>
 * <ol>
 *   <li>用户执行查询（SELECT col1, col2 FROM table WHERE ...）</li>
 *   <li>调用 {@link #auth(List)} 进行授权检查</li>
 *   <li>返回 {@link TableQueryAuthResult}，包含允许的列和额外的过滤条件</li>
 *   <li>扫描和读取阶段应用授权结果</li>
 * </ol>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>列级别权限</b>: 限制用户只能访问特定列</li>
 *   <li><b>行级别权限</b>: 根据用户角色过滤数据行</li>
 *   <li><b>数据脱敏</b>: 对敏感数据进行脱敏处理</li>
 * </ul>
 *
 * @see TableQueryAuthResult 查询授权结果
 * @see QueryAuthSplit 查询授权分片
 */
public interface TableQueryAuth {

    /**
     * 执行查询授权检查。
     *
     * @param select 用户选择的列名列表（null 表示 SELECT *）
     * @return 授权结果，如果无需授权检查则返回 null
     */
    @Nullable
    TableQueryAuthResult auth(@Nullable List<String> select);
}
