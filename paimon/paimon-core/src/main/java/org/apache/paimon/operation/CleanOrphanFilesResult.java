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

package org.apache.paimon.operation;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 孤儿文件清理结果
 *
 * <p>封装孤儿文件清理操作的统计信息和结果数据。
 *
 * <h2>结果统计信息</h2>
 * <ul>
 *   <li><b>文件数量</b>：被删除的孤儿文件总数
 *   <li><b>文件大小</b>：被删除文件的总大小（字节）
 *   <li><b>文件列表</b>：被删除文件的完整路径列表（可选）
 * </ul>
 *
 * <h2>用途</h2>
 * <ul>
 *   <li>向用户报告清理结果
 *   <li>用于监控和审计
 *   <li>支持试运行模式（dry run）
 * </ul>
 *
 * @see OrphanFilesClean 孤儿文件清理器
 * @see LocalOrphanFilesClean 本地孤儿文件清理器
 */
public class CleanOrphanFilesResult {

    /** 被删除的文件数量 */
    private final long deletedFileCount;

    /** 被删除文件的总大小（字节） */
    private final long deletedFileTotalLenInBytes;

    /** 被删除文件的路径列表（可选） */
    @Nullable private final List<Path> deletedFilesPath;

    /**
     * 构造清理结果（不包含文件路径列表）
     *
     * @param deletedFileCount 删除的文件数量
     * @param deletedFileTotalLenInBytes 删除文件的总大小（字节）
     */
    public CleanOrphanFilesResult(long deletedFileCount, long deletedFileTotalLenInBytes) {
        this(deletedFileCount, deletedFileTotalLenInBytes, null);
    }

    /**
     * 构造清理结果（包含文件路径列表）
     *
     * @param deletedFileCount 删除的文件数量
     * @param deletedFileTotalLenInBytes 删除文件的总大小（字节）
     * @param deletedFilesPath 删除文件的路径列表（可选）
     */
    public CleanOrphanFilesResult(
            long deletedFileCount,
            long deletedFileTotalLenInBytes,
            @Nullable List<Path> deletedFilesPath) {
        this.deletedFilesPath = deletedFilesPath;
        this.deletedFileCount = deletedFileCount;
        this.deletedFileTotalLenInBytes = deletedFileTotalLenInBytes;
    }

    /**
     * 获取删除的文件数量
     *
     * @return 文件数量
     */
    public long getDeletedFileCount() {
        return deletedFileCount;
    }

    /**
     * 获取删除文件的总大小
     *
     * @return 总大小（字节）
     */
    public long getDeletedFileTotalLenInBytes() {
        return deletedFileTotalLenInBytes;
    }

    /**
     * 获取删除文件的路径列表
     *
     * @return 路径列表，如果未记录则返回 null
     */
    @Nullable
    public List<Path> getDeletedFilesPath() {
        return deletedFilesPath;
    }
}
