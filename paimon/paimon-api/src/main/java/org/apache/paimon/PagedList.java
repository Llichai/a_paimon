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

package org.apache.paimon;

import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 分页列表类，支持从分页流中请求数据。
 *
 * <p>该类用于处理支持分页的 REST API 响应，包含当前页的数据元素和下一页的分页令牌。
 *
 * <h2>主要特点</h2>
 * <ul>
 *   <li>支持分页流式读取，不需要一次性加载所有数据
 *   <li>提供获取当前页元素和下一页令牌的方法
 *   <li>提供静态辅助方法来一次性加载所有分页数据
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 处理单个分页结果
 * PagedList<Table> pagedResult = catalog.listTables("db1");
 * List<Table> tables = pagedResult.getElements();
 * String nextToken = pagedResult.getNextPageToken();
 *
 * if (nextToken != null) {
 *     // 请求下一页
 *     PagedList<Table> nextPage = catalog.listTables("db1", nextToken);
 * }
 *
 * // 一次性加载所有数据
 * List<Table> allTables = PagedList.listAllFromPagedApi(
 *     pageToken -> catalog.listTables("db1", pageToken)
 * );
 * }</pre>
 *
 * @param <T> 列表中元素的类型
 * @since 1.1.0
 */
public class PagedList<T> {

    private final List<T> elements;

    @Nullable private final String nextPageToken;

    public PagedList(List<T> elements, @Nullable String nextPageToken) {
        this.elements = elements;
        this.nextPageToken = nextPageToken;
    }

    /** An array of element objects. */
    public List<T> getElements() {
        return this.elements;
    }

    /** Page token to retrieve the next page of results. Absent if there are no more pages. */
    @Nullable
    public String getNextPageToken() {
        return this.nextPageToken;
    }

    /** Util method to list all from paged api. */
    public static <T> List<T> listAllFromPagedApi(Function<String, PagedList<T>> pagedApi) {
        List<T> results = new ArrayList<>();
        String pageToken = null;
        do {
            PagedList<T> response = pagedApi.apply(pageToken);
            pageToken = response.getNextPageToken();
            List<T> elements = response.getElements();
            if (elements != null) {
                results.addAll(elements);
            }
            if (pageToken == null || elements == null || elements.isEmpty()) {
                break;
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return results;
    }
}
