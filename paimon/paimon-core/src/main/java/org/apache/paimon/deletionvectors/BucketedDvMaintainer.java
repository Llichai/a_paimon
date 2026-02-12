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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 分桶表的删除向量维护器。
 *
 * <p>此类负责管理单个bucket的所有删除向量,提供删除通知、向量合并、持久化等功能。
 * 它是删除向量系统的核心组件,连接了内存中的删除向量和持久化的索引文件。
 *
 * <h2>核心功能:</h2>
 * <ul>
 *   <li><b>删除通知</b>: 记录新的删除操作
 *   <li><b>向量合并</b>: 合并来自不同来源的删除向量
 *   <li><b>向量移除</b>: 删除不再需要的删除向量(如压缩后)
 *   <li><b>持久化</b>: 将修改后的删除向量写入索引文件
 *   <li><b>查询</b>: 根据文件名查询删除向量
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>追加表的DELETE操作: 记录被删除的行位置
 *   <li>主键表的更新操作: 标记旧版本行为删除
 *   <li>压缩过程: 移除旧文件的删除向量,合并新文件的删除向量
 *   <li>读取优化: 为读取器提供删除向量查询
 * </ul>
 *
 * <h2>修改追踪:</h2>
 * <p>维护器使用{@code modified}标志跟踪是否有任何修改。只有当有修改时,
 * {@link #writeDeletionVectorsIndex()}才会实际写入新的索引文件。
 *
 * <h2>线程安全:</h2>
 * <p>此类<b>不是线程安全的</b>。通常每个bucket有一个独立的维护器实例,
 * 由单个线程操作。
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建维护器
 * BucketedDvMaintainer.Factory factory = BucketedDvMaintainer.factory(indexFileHandler);
 * BucketedDvMaintainer maintainer = factory.create(partition, bucket, restoredFiles);
 *
 * // 通知新的删除
 * maintainer.notifyNewDeletion("file1.parquet", 100);
 * maintainer.notifyNewDeletion("file1.parquet", 200);
 *
 * // 持久化修改
 * Optional<IndexFileMeta> newIndex = maintainer.writeDeletionVectorsIndex();
 *
 * // 查询删除向量
 * Optional<DeletionVector> dv = maintainer.deletionVectorOf("file1.parquet");
 * }</pre>
 *
 * @see DeletionVector 删除向量
 * @see DeletionVectorsIndexFile 删除向量索引文件
 * @see IndexFileMeta 索引文件元数据
 */
public class BucketedDvMaintainer {

    /** 删除向量索引文件接口 */
    private final DeletionVectorsIndexFile dvIndexFile;
    /** 文件名到删除向量的映射 */
    private final Map<String, DeletionVector> deletionVectors;
    /** 是否使用64位位图 */
    protected final boolean bitmap64;
    /** 是否有修改 */
    private boolean modified;

    /**
     * 创建分桶删除向量维护器。
     *
     * @param dvIndexFile 删除向量索引文件接口
     * @param deletionVectors 初始的删除向量映射
     */
    private BucketedDvMaintainer(
            DeletionVectorsIndexFile dvIndexFile, Map<String, DeletionVector> deletionVectors) {
        this.dvIndexFile = dvIndexFile;
        this.deletionVectors = deletionVectors;
        this.bitmap64 = dvIndexFile.bitmap64();
        this.modified = false;
    }

    /**
     * 根据配置创建新的删除向量。
     *
     * @return 新的删除向量实例(32位或64位)
     */
    private DeletionVector createNewDeletionVector() {
        return bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    }

    /**
     * 通知新的删除操作,标记指定文件中指定位置的行为已删除。
     *
     * <p>如果文件还没有删除向量,会自动创建一个新的。
     * 只有当位置之前未被删除时才会设置modified标志。
     *
     * @param fileName 发生删除的文件名
     * @param position 文件中已删除的行位置
     */
    public void notifyNewDeletion(String fileName, long position) {
        DeletionVector deletionVector =
                deletionVectors.computeIfAbsent(fileName, k -> createNewDeletionVector());
        if (deletionVector.checkedDelete(position)) {
            modified = true;
        }
    }

    /**
     * 通知新的删除向量,直接关联到指定文件。
     *
     * <p>此方法会替换该文件的现有删除向量(如果有)。
     *
     * @param fileName 发生删除的文件名
     * @param deletionVector 要关联的删除向量
     */
    public void notifyNewDeletion(String fileName, DeletionVector deletionVector) {
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * 合并新的删除向量到指定文件。
     *
     * <p>如果该文件已有删除向量,则将新向量合并到旧向量中。
     * 否则直接使用新向量。
     *
     * <p>此方法常用于压缩场景,需要合并多个文件的删除向量。
     *
     * @param fileName 发生删除的文件名
     * @param deletionVector 要合并的删除向量
     */
    public void mergeNewDeletion(String fileName, DeletionVector deletionVector) {
        DeletionVector old = deletionVectors.get(fileName);
        if (old != null) {
            deletionVector.merge(old);
        }
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * 移除指定文件的删除向量。
     *
     * <p>此方法通常用于压缩过程中移除旧文件(before files)的删除向量。
     *
     * @param fileName 要移除删除向量的文件名
     */
    public void removeDeletionVectorOf(String fileName) {
        if (deletionVectors.containsKey(fileName)) {
            deletionVectors.remove(fileName);
            modified = true;
        }
    }

    /**
     * 如果有任何修改,则写入新的删除向量索引文件。
     *
     * <p>此方法是幂等的,只有当{@code modified}为true时才会实际写入。
     * 写入成功后会重置{@code modified}标志。
     *
     * @return 如果没有修改返回空Optional,否则返回新的删除向量索引文件元数据
     */
    public Optional<IndexFileMeta> writeDeletionVectorsIndex() {
        if (modified) {
            modified = false;
            return Optional.of(dvIndexFile.writeSingleFile(deletionVectors));
        }
        return Optional.empty();
    }

    /**
     * 获取与指定文件关联的删除向量。
     *
     * @param fileName 要查询的文件名
     * @return 包含删除向量的Optional(如果存在),否则为空Optional
     */
    public Optional<DeletionVector> deletionVectorOf(String fileName) {
        return Optional.ofNullable(deletionVectors.get(fileName));
    }

    /**
     * 获取删除向量索引文件接口。
     *
     * @return 删除向量索引文件
     */
    public DeletionVectorsIndexFile dvIndexFile() {
        return dvIndexFile;
    }

    /**
     * 获取所有删除向量映射(仅用于测试)。
     *
     * @return 文件名到删除向量的映射
     */
    @VisibleForTesting
    public Map<String, DeletionVector> deletionVectors() {
        return deletionVectors;
    }

    /**
     * 是否使用64位位图。
     *
     * @return 如果使用64位位图返回true,否则返回false
     */
    public boolean bitmap64() {
        return bitmap64;
    }

    /**
     * 创建工厂实例。
     *
     * @param handler 索引文件处理器
     * @return 维护器工厂
     */
    public static Factory factory(IndexFileHandler handler) {
        return new Factory(handler);
    }

    /**
     * 用于恢复{@link BucketedDvMaintainer}的工厂类。
     *
     * <p>此工厂负责从持久化的索引文件中恢复删除向量,
     * 并创建新的维护器实例。
     */
    public static class Factory {

        /** 索引文件处理器 */
        private final IndexFileHandler handler;

        /**
         * 创建工厂。
         *
         * @param handler 索引文件处理器
         */
        private Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        /**
         * 获取索引文件处理器。
         *
         * @return 索引文件处理器
         */
        public IndexFileHandler indexFileHandler() {
            return handler;
        }

        /**
         * 从索引文件恢复并创建维护器。
         *
         * @param partition 分区键
         * @param bucket 分桶ID
         * @param restoredFiles 要恢复的索引文件列表,可为null
         * @return 新的维护器实例
         */
        public BucketedDvMaintainer create(
                BinaryRow partition, int bucket, @Nullable List<IndexFileMeta> restoredFiles) {
            if (restoredFiles == null) {
                restoredFiles = Collections.emptyList();
            }
            Map<String, DeletionVector> deletionVectors =
                    new HashMap<>(handler.readAllDeletionVectors(partition, bucket, restoredFiles));
            return create(partition, bucket, deletionVectors);
        }

        /**
         * 使用给定的删除向量创建维护器。
         *
         * @param partition 分区键
         * @param bucket 分桶ID
         * @param deletionVectors 初始的删除向量映射
         * @return 新的维护器实例
         */
        public BucketedDvMaintainer create(
                BinaryRow partition, int bucket, Map<String, DeletionVector> deletionVectors) {
            return new BucketedDvMaintainer(handler.dvIndex(partition, bucket), deletionVectors);
        }
    }
}
