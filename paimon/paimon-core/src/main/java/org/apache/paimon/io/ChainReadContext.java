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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryRow;

import java.util.Map;

/**
 * 链式读取上下文信息。
 *
 * <p>存储链式读取所需的关键信息,包括:
 * <ul>
 *   <li>逻辑分区: 数据在逻辑上所属的分区</li>
 *   <li>文件到桶路径的映射: 记录每个文件对应的桶路径</li>
 *   <li>文件到分支的映射: 记录每个文件来自哪个分支</li>
 * </ul>
 *
 * <p>这些信息用于支持跨分支和跨桶的数据读取。
 */
public class ChainReadContext {

    /** 逻辑分区,表示数据逻辑上所属的分区 */
    private final BinaryRow logicalPartition;
    /** 文件名到桶路径的映射 */
    private final Map<String, String> fileBucketPathMapping;
    /** 文件名到分支名的映射 */
    private final Map<String, String> fileBranchMapping;

    /**
     * 构造链式读取上下文。
     *
     * @param readPartition 逻辑分区
     * @param fileBucketPathMapping 文件到桶路径的映射
     * @param fileBranchMapping 文件到分支的映射
     */
    private ChainReadContext(
            BinaryRow readPartition,
            Map<String, String> fileBucketPathMapping,
            Map<String, String> fileBranchMapping) {
        this.logicalPartition = readPartition;
        this.fileBucketPathMapping = fileBucketPathMapping;
        this.fileBranchMapping = fileBranchMapping;
    }

    /**
     * 获取逻辑分区。
     *
     * @return 逻辑分区的二进制行表示
     */
    public BinaryRow logicalPartition() {
        return logicalPartition;
    }

    /**
     * 获取文件到桶路径的映射。
     *
     * @return 文件名到桶路径的映射
     */
    public Map<String, String> fileBucketPathMapping() {
        return fileBucketPathMapping;
    }

    /**
     * 获取文件到分支的映射。
     *
     * @return 文件名到分支名的映射
     */
    public Map<String, String> fileBranchMapping() {
        return fileBranchMapping;
    }

    /**
     * 链式读取上下文构建器。
     */
    public static class Builder {
        private BinaryRow logicalPartition;

        private Map<String, String> fileBucketPathMapping;

        private Map<String, String> fileBranchPathMapping;

        /**
         * 设置逻辑分区。
         *
         * @param logicalPartition 逻辑分区
         * @return 构建器自身
         */
        public ChainReadContext.Builder withLogicalPartition(BinaryRow logicalPartition) {
            this.logicalPartition = logicalPartition;
            return this;
        }

        /**
         * 设置文件到桶路径的映射。
         *
         * @param fileBucketPathMapping 文件到桶路径的映射
         * @return 构建器自身
         */
        public ChainReadContext.Builder withFileBucketPathMapping(
                Map<String, String> fileBucketPathMapping) {
            this.fileBucketPathMapping = fileBucketPathMapping;
            return this;
        }

        /**
         * 设置文件到分支的映射。
         *
         * @param fileBranchPathMapping 文件到分支的映射
         * @return 构建器自身
         */
        public ChainReadContext.Builder withFileBranchPathMapping(
                Map<String, String> fileBranchPathMapping) {
            this.fileBranchPathMapping = fileBranchPathMapping;
            return this;
        }

        /**
         * 构建链式读取上下文。
         *
         * @return 链式读取上下文实例
         */
        public ChainReadContext build() {
            return new ChainReadContext(
                    logicalPartition, fileBucketPathMapping, fileBranchPathMapping);
        }
    }
}
