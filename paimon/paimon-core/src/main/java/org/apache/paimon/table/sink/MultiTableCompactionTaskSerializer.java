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

import org.apache.paimon.append.MultiTableAppendCompactTask;
import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.io.IdentifierSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 多表压缩任务序列化器。
 *
 * <p>负责 {@link MultiTableAppendCompactTask} 的序列化和反序列化，用于在
 * 分布式环境中传输多表压缩任务信息。
 *
 * <p>多表压缩任务包含：
 * <ul>
 *   <li><b>表标识符</b>：任务所属的表（支持跨表压缩）
 *   <li><b>分区信息</b>：任务所属的分区
 *   <li><b>待压缩文件列表</b>：需要进行压缩的数据文件列表
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li><b>多表统一压缩</b>：
 *       <ul>
 *         <li>在一个作业中压缩多个表的数据
 *         <li>适用于 Database Compaction 等场景
 *       </ul>
 *   <li><b>共享压缩资源</b>：
 *       <ul>
 *         <li>多个表共享压缩线程池
 *         <li>提高资源利用率
 *       </ul>
 *   <li><b>批量压缩调度</b>：
 *       <ul>
 *         <li>统一调度多个表的压缩任务
 *         <li>支持跨表的优先级管理
 *       </ul>
 * </ul>
 *
 * <p>序列化格式（Version 1）：
 * <pre>
 * +------------------+
 * | partition        | BinaryRow
 * +------------------+
 * | num_files        | int
 * +------------------+
 * | file_1           | DataFileMeta
 * +------------------+
 * | file_2           | DataFileMeta
 * +------------------+
 * | ...              |
 * +------------------+
 * | table_identifier | Identifier
 * +------------------+
 * </pre>
 *
 * <p>与 {@link AppendCompactTaskSerializer} 的区别：
 * <ul>
 *   <li>{@link AppendCompactTaskSerializer}：单表压缩任务
 *   <li>{@link MultiTableCompactionTaskSerializer}：多表压缩任务，增加了表标识符
 * </ul>
 *
 * <p>版本管理：
 * <ul>
 *   <li><b>当前版本</b>：Version 1
 *   <li><b>版本兼容性</b>：
 *       <ul>
 *         <li>不同版本不兼容，需要重启作业
 *         <li>不支持从旧版本 Savepoint 恢复
 *       </ul>
 * </ul>
 *
 * <p>使用示例：
 * <pre>
 * // 序列化
 * MultiTableCompactionTaskSerializer serializer =
 *     new MultiTableCompactionTaskSerializer();
 * byte[] bytes = serializer.serialize(task);
 *
 * // 反序列化
 * MultiTableAppendCompactTask task =
 *     serializer.deserialize(version, bytes);
 * </pre>
 *
 * @see MultiTableAppendCompactTask 多表追加压缩任务
 * @see AppendCompactTaskSerializer 单表压缩任务序列化器
 * @see DataFileMetaSerializer 数据文件元数据序列化器
 * @see IdentifierSerializer 表标识符序列化器
 */
public class MultiTableCompactionTaskSerializer
        implements VersionedSerializer<MultiTableAppendCompactTask> {

    /** 当前序列化版本号 */
    private static final int CURRENT_VERSION = 1;

    /** 数据文件元数据序列化器 */
    private final DataFileMetaSerializer dataFileSerializer;

    /** 表标识符序列化器 */
    private final IdentifierSerializer identifierSerializer;

    /**
     * 构造多表压缩任务序列化器。
     */
    public MultiTableCompactionTaskSerializer() {
        this.dataFileSerializer = new DataFileMetaSerializer();
        this.identifierSerializer = new IdentifierSerializer();
    }

    /**
     * 获取当前序列化版本号。
     *
     * @return 版本号 1
     */
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    /**
     * 序列化压缩任务为字节数组。
     *
     * @param task 待序列化的压缩任务
     * @return 序列化后的字节数组
     * @throws IOException 序列化失败
     */
    @Override
    public byte[] serialize(MultiTableAppendCompactTask task) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(task, view);
        return out.toByteArray();
    }

    /**
     * 序列化单个压缩任务。
     *
     * @param task 压缩任务
     * @param view 输出视图
     * @throws IOException 序列化失败
     */
    private void serialize(MultiTableAppendCompactTask task, DataOutputView view)
            throws IOException {
        // 序列化分区
        serializeBinaryRow(task.partition(), view);
        // 序列化待压缩文件列表
        dataFileSerializer.serializeList(task.compactBefore(), view);
        // 序列化表标识符（与单表版本的主要区别）
        identifierSerializer.serialize(task.tableIdentifier(), view);
    }

    /**
     * 从字节数组反序列化压缩任务。
     *
     * @param version 序列化版本号
     * @param serialized 序列化的字节数组
     * @return 反序列化的压缩任务
     * @throws IOException 反序列化失败
     */
    @Override
    public MultiTableAppendCompactTask deserialize(int version, byte[] serialized)
            throws IOException {
        checkVersion(version);
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return deserialize(view);
    }

    /**
     * 反序列化单个压缩任务。
     *
     * @param view 输入视图
     * @return 反序列化的压缩任务
     * @throws IOException 反序列化失败
     */
    private MultiTableAppendCompactTask deserialize(DataInputView view) throws IOException {
        return new MultiTableAppendCompactTask(
                deserializeBinaryRow(view),
                dataFileSerializer.deserializeList(view),
                identifierSerializer.deserialize(view));
    }

    /**
     * 反序列化压缩任务列表。
     *
     * <p>用于批量反序列化多个压缩任务。
     *
     * @param version 序列化版本号
     * @param view 输入视图
     * @return 压缩任务列表
     * @throws IOException 反序列化失败
     */
    public List<MultiTableAppendCompactTask> deserializeList(int version, DataInputView view)
            throws IOException {
        checkVersion(version);
        int length = view.readInt();
        List<MultiTableAppendCompactTask> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(deserialize(view));
        }
        return list;
    }

    /**
     * 序列化压缩任务列表。
     *
     * <p>用于批量序列化多个压缩任务。
     *
     * @param list 压缩任务列表
     * @param view 输出视图
     * @throws IOException 序列化失败
     */
    public void serializeList(List<MultiTableAppendCompactTask> list, DataOutputView view)
            throws IOException {
        view.writeInt(list.size());
        for (MultiTableAppendCompactTask commitMessage : list) {
            serialize(commitMessage, view);
        }
    }

    /**
     * 检查版本兼容性。
     *
     * <p>多表压缩任务不支持版本兼容，必须使用相同的版本号。
     *
     * @param version 要检查的版本号
     * @throws UnsupportedOperationException 如果版本不匹配
     */
    private void checkVersion(int version) {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting MultiTableCompactionTaskSerializer version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".\nCompactionTask is not a compatible data structure. "
                            + "Please restart the job afresh (do not recover from savepoint).");
        }
    }
}
