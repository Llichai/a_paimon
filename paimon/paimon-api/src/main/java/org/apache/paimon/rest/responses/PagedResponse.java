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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;

import java.util.List;

/**
 * 分页响应接口。
 *
 * <p>该接口定义了分页响应的标准结构,用于支持大数据集的分页查询。
 *
 * <p>所有实现类都应提供:
 * <ul>
 *   <li>data() - 当前页的数据列表
 *   <li>getNextPageToken() - 下一页的令牌,用于获取后续数据
 * </ul>
 *
 * <p>分页机制:
 * <ol>
 *   <li>客户端发起初始请求
 *   <li>服务端返回第一页数据和 nextPageToken
 *   <li>客户端使用 nextPageToken 请求下一页
 *   <li>当 nextPageToken 为 null 时,表示已到达最后一页
 * </ol>
 *
 * <p>常见实现:
 * <ul>
 *   <li>{@link ListTablesResponse} - 表名称列表
 *   <li>{@link ListDatabasesResponse} - 数据库名称列表
 *   <li>{@link ListPartitionsResponse} - 分区列表
 *   <li>{@link ListSnapshotsResponse} - 快照列表
 * </ul>
 *
 * @param <T> 数据元素类型
 * @since 1.0
 */
public interface PagedResponse<T> extends RESTResponse {
    /**
     * 获取当前页的数据列表。
     *
     * @return 数据列表,不会为 null
     */
    List<T> data();

    /**
     * 获取下一页令牌。
     *
     * @return 下一页令牌,如果没有更多数据则返回 null
     */
    String getNextPageToken();
}
