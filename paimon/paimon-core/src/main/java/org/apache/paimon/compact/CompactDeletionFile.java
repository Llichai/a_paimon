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

package org.apache.paimon.compact;

import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.index.IndexFileMeta;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * 压缩过程中的删除文件管理接口
 *
 * <p>用于管理压缩过程中产生的 Deletion Vector 索引文件。
 *
 * <p>功能概述：
 * <ul>
 *   <li>生成删除向量索引文件：记录数据文件中被删除的行位置
 *   <li>合并旧文件：处理多次压缩的文件合并
 *   <li>清理文件：删除过期的索引文件
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>异步压缩：压缩任务完成时立即生成删除文件（{@link #generateFiles}）
 *   <li>同步压缩：在 prepareCommit 阶段延迟生成删除文件（{@link #lazyGeneration}）
 * </ul>
 *
 * <p>实现策略：
 * <ul>
 *   <li>{@link GeneratedDeletionFile}：立即生成模式，用于异步压缩
 *   <li>{@link LazyCompactDeletionFile}：延迟生成模式，用于同步压缩
 * </ul>
 */
public interface CompactDeletionFile {

    /**
     * 获取或计算删除向量索引文件
     *
     * @return 删除向量索引文件（如果没有删除则为空）
     */
    Optional<IndexFileMeta> getOrCompute();

    /**
     * 合并旧的删除文件
     *
     * <p>当多次压缩产生多个删除文件时，需要合并处理。
     * 旧文件会被清理，新文件会被保留。
     *
     * @param old 旧的删除文件
     * @return 合并后的删除文件
     */
    CompactDeletionFile mergeOldFile(CompactDeletionFile old);

    /**
     * 清理删除文件
     *
     * <p>删除已生成的索引文件，释放存储空间。
     */
    void clean();

    /**
     * 立即生成删除文件（用于异步压缩）
     *
     * <p>在异步压缩场景下，压缩任务完成时立即生成删除向量索引文件。
     * 当 updateCompactResult 时，需要合并旧的删除文件（通过删除它们）。
     *
     * @param maintainer Deletion Vector 维护器
     * @return 已生成的删除文件包装器
     */
    static CompactDeletionFile generateFiles(BucketedDvMaintainer maintainer) {
        Optional<IndexFileMeta> file = maintainer.writeDeletionVectorsIndex();
        return new GeneratedDeletionFile(file.orElse(null), maintainer.dvIndexFile());
    }

    /**
     * 延迟生成删除文件（用于同步压缩）
     *
     * <p>在同步压缩场景下，仅在 prepareCommit 阶段创建删除文件。
     *
     * @param maintainer Deletion Vector 维护器
     * @return 延迟生成的删除文件包装器
     */
    static CompactDeletionFile lazyGeneration(BucketedDvMaintainer maintainer) {
        return new LazyCompactDeletionFile(maintainer);
    }

    /**
     * 立即生成模式的删除文件实现
     *
     * <p>用于异步压缩场景，压缩完成时立即写入删除向量索引文件。
     *
     * <p>特性：
     * <ul>
     *   <li>文件在构造时已经生成
     *   <li>支持与旧文件合并，会清理旧文件
     *   <li>支持清理自身生成的文件
     * </ul>
     */
    class GeneratedDeletionFile implements CompactDeletionFile {

        /** 删除向量索引文件元数据（null 表示没有删除） */
        @Nullable private final IndexFileMeta deletionFile;

        /** 删除向量索引文件访问接口 */
        private final DeletionVectorsIndexFile dvIndexFile;

        /** 标记 getOrCompute 是否已被调用 */
        private boolean getInvoked = false;

        /**
         * 构造已生成的删除文件
         *
         * @param deletionFile 删除向量索引文件元数据
         * @param dvIndexFile 删除向量索引文件访问接口
         */
        public GeneratedDeletionFile(
                @Nullable IndexFileMeta deletionFile, DeletionVectorsIndexFile dvIndexFile) {
            this.deletionFile = deletionFile;
            this.dvIndexFile = dvIndexFile;
        }

        /**
         * 获取删除向量索引文件
         *
         * @return 索引文件元数据（如果没有删除则为空）
         */
        @Override
        public Optional<IndexFileMeta> getOrCompute() {
            this.getInvoked = true;
            return Optional.ofNullable(deletionFile);
        }

        /**
         * 合并旧的删除文件
         *
         * <p>合并规则：
         * <ul>
         *   <li>如果当前文件为 null，使用旧文件
         *   <li>否则，清理旧文件，使用当前文件
         * </ul>
         *
         * @param old 旧的删除文件
         * @return 合并后的删除文件
         * @throws IllegalStateException 如果旧文件不是 GeneratedDeletionFile 或已被访问
         */
        @Override
        public CompactDeletionFile mergeOldFile(CompactDeletionFile old) {
            if (!(old instanceof GeneratedDeletionFile)) {
                throw new IllegalStateException(
                        "old should be a GeneratedDeletionFile, but it is: " + old.getClass());
            }

            if (((GeneratedDeletionFile) old).getInvoked) {
                throw new IllegalStateException("old should not be get, this is a bug.");
            }

            // 如果当前没有删除文件，保留旧文件
            if (deletionFile == null) {
                return old;
            }

            // 清理旧文件，使用新文件
            old.clean();
            return this;
        }

        /**
         * 清理删除向量索引文件
         */
        @Override
        public void clean() {
            if (deletionFile != null) {
                dvIndexFile.delete(deletionFile);
            }
        }
    }

    /**
     * 延迟生成模式的删除文件实现
     *
     * <p>用于同步压缩场景，在第一次调用 getOrCompute 时才生成删除向量索引文件。
     *
     * <p>特性：
     * <ul>
     *   <li>文件在首次访问时才生成
     *   <li>合并时始终保留新的维护器
     *   <li>不支持清理（因为文件可能未生成）
     * </ul>
     */
    class LazyCompactDeletionFile implements CompactDeletionFile {

        /** Deletion Vector 维护器，用于延迟生成文件 */
        private final BucketedDvMaintainer maintainer;

        /** 标记文件是否已生成 */
        private boolean generated = false;

        /**
         * 构造延迟生成的删除文件
         *
         * @param maintainer Deletion Vector 维护器
         */
        public LazyCompactDeletionFile(BucketedDvMaintainer maintainer) {
            this.maintainer = maintainer;
        }

        /**
         * 延迟生成并获取删除向量索引文件
         *
         * <p>第一次调用时才会真正生成文件。
         *
         * @return 索引文件元数据（如果没有删除则为空）
         */
        @Override
        public Optional<IndexFileMeta> getOrCompute() {
            generated = true;
            return generateFiles(maintainer).getOrCompute();
        }

        /**
         * 合并旧的删除文件
         *
         * <p>延迟模式下，始终保留新的维护器，忽略旧文件。
         *
         * @param old 旧的删除文件
         * @return 当前删除文件
         * @throws IllegalStateException 如果旧文件不是 LazyCompactDeletionFile 或已生成
         */
        @Override
        public CompactDeletionFile mergeOldFile(CompactDeletionFile old) {
            if (!(old instanceof LazyCompactDeletionFile)) {
                throw new IllegalStateException(
                        "old should be a LazyCompactDeletionFile, but it is: " + old.getClass());
            }

            if (((LazyCompactDeletionFile) old).generated) {
                throw new IllegalStateException("old should not be generated, this is a bug.");
            }

            return this;
        }

        /**
         * 清理删除文件
         *
         * <p>延迟模式下不执行清理，因为文件可能未生成。
         */
        @Override
        public void clean() {}
    }
}
