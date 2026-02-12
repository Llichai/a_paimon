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

package org.apache.paimon.sst;

import org.apache.paimon.lookup.LookupStoreFactory.Context;

/**
 * 排序上下文。
 *
 * <p>该类为排序存储提供上下文信息，实现了 {@link Context} 接口。主要用于在创建排序查找存储时传递文件大小等元数据信息。
 *
 * <p>主要功能：
 * <ul>
 *   <li>存储文件大小信息
 *   <li>作为排序查找存储工厂的上下文参数
 *   <li>提供文件大小的访问接口
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建排序上下文
 * long fileSize = 1024 * 1024; // 1MB
 * SortContext context = new SortContext(fileSize);
 *
 * // 获取文件大小
 * long size = context.fileSize();
 * }</pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li>SST 文件的排序存储创建
 *   <li>查找存储的初始化配置
 *   <li>文件元数据的传递
 * </ul>
 */
public class SortContext implements Context {

    /** 文件大小（字节） */
    private final long fileSize;

    /**
     * 构造排序上下文。
     *
     * @param fileSize 文件大小（字节）
     */
    public SortContext(long fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * 获取文件大小。
     *
     * @return 文件大小（字节）
     */
    public long fileSize() {
        return fileSize;
    }
}
