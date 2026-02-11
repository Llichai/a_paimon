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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.deserializedBytes;
import static org.apache.paimon.utils.SerializationUtils.serializeBytes;

/**
 * Sink 的文件提交消息实现。
 *
 * <p>此类包含了一次写入操作产生的所有文件变更信息，分为两部分：
 * <ul>
 *     <li><b>DataIncrement</b>：普通数据文件的增量
 *         <ul>
 *             <li>newFiles: 新写入的数据文件
 *             <li>deletedFiles: 被删除的数据文件（通常为空，追加表除外）
 *             <li>changelogFiles: 新生成的 changelog 文件
 *             <li>newIndexFiles: 新创建的索引文件
 *             <li>deletedIndexFiles: 被删除的索引文件
 *         </ul>
 *     <li><b>CompactIncrement</b>：压缩操作的增量
 *         <ul>
 *             <li>compactBefore: 压缩前的文件（将被删除）
 *             <li>compactAfter: 压缩后的文件（新创建）
 *             <li>changelogFiles: 压缩生成的 changelog 文件
 *             <li>newIndexFiles: 压缩生成的索引文件
 *             <li>deletedIndexFiles: 压缩删除的索引文件
 *         </ul>
 * </ul>
 *
 * <p>序列化机制：
 * <ul>
 *     <li>使用自定义的序列化器 {@link CommitMessageSerializer}
 *     <li>支持多个版本的兼容性
 *     <li>通过 ThreadLocal 缓存序列化器实例
 *     <li>transient 字段在序列化时由自定义逻辑处理
 * </ul>
 *
 * <p>与 Operation 层的关系：
 * <ul>
 *     <li>Operation 层也有 CommitMessageImpl
 *     <li>两者名称相同但位于不同的包
 *     <li>Table 层的 CommitMessage 会被转换为 Operation 层的 CommitMessage
 * </ul>
 */
public class CommitMessageImpl implements CommitMessage {

    private static final long serialVersionUID = 1L;

    /** 序列化器的 ThreadLocal 缓存 */
    private static final ThreadLocal<CommitMessageSerializer> CACHE =
            ThreadLocal.withInitial(CommitMessageSerializer::new);

    /** 分区（transient，由自定义序列化处理） */
    private transient BinaryRow partition;

    /** 分桶号（transient，由自定义序列化处理） */
    private transient int bucket;

    /** 总分桶数（transient，由自定义序列化处理） */
    private transient @Nullable Integer totalBuckets;

    /** 数据增量（transient，由自定义序列化处理） */
    private transient DataIncrement dataIncrement;

    /** 压缩增量（transient，由自定义序列化处理） */
    private transient CompactIncrement compactIncrement;

    /**
     * 构造函数。
     *
     * @param partition 分区
     * @param bucket 分桶号
     * @param totalBuckets 总分桶数
     * @param dataIncrement 数据增量
     * @param compactIncrement 压缩增量
     */
    public CommitMessageImpl(
            BinaryRow partition,
            int bucket,
            @Nullable Integer totalBuckets,
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement) {
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.dataIncrement = dataIncrement;
        this.compactIncrement = compactIncrement;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public @Nullable Integer totalBuckets() {
        return totalBuckets;
    }

    /**
     * 获取新文件增量。
     *
     * @return 数据增量
     */
    public DataIncrement newFilesIncrement() {
        return dataIncrement;
    }

    /**
     * 获取压缩增量。
     *
     * @return 压缩增量
     */
    public CompactIncrement compactIncrement() {
        return compactIncrement;
    }

    /**
     * 判断是否为空提交。
     *
     * <p>当没有任何文件变更时返回 true。
     *
     * @return true 如果是空提交
     */
    public boolean isEmpty() {
        return dataIncrement.isEmpty() && compactIncrement.isEmpty();
    }

    /**
     * 自定义序列化方法。
     *
     * <p>使用 {@link CommitMessageSerializer} 进行序列化。
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        CommitMessageSerializer serializer = CACHE.get();
        out.writeInt(serializer.getVersion());
        serializeBytes(new DataOutputViewStreamWrapper(out), serializer.serialize(this));
    }

    /**
     * 自定义反序列化方法。
     *
     * <p>使用 {@link CommitMessageSerializer} 进行反序列化。
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        byte[] bytes = deserializedBytes(new DataInputViewStreamWrapper(in));
        CommitMessageImpl message = (CommitMessageImpl) CACHE.get().deserialize(version, bytes);
        this.partition = message.partition;
        this.bucket = message.bucket;
        this.totalBuckets = message.totalBuckets;
        this.dataIncrement = message.dataIncrement;
        this.compactIncrement = message.compactIncrement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CommitMessageImpl that = (CommitMessageImpl) o;
        return bucket == that.bucket
                && Objects.equals(partition, that.partition)
                && Objects.equals(totalBuckets, that.totalBuckets)
                && Objects.equals(dataIncrement, that.dataIncrement)
                && Objects.equals(compactIncrement, that.compactIncrement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, totalBuckets, dataIncrement, compactIncrement);
    }

    @Override
    public String toString() {
        return String.format(
                "FileCommittable {"
                        + "partition = %s, "
                        + "bucket = %d, "
                        + "totalBuckets = %s, "
                        + "newFilesIncrement = %s, "
                        + "compactIncrement = %s}",
                partition, bucket, totalBuckets, dataIncrement, compactIncrement);
    }
}
