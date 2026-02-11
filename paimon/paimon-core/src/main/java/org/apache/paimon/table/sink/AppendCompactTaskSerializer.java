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

import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 追加压缩任务序列化器。
 *
 * <p>负责 {@link AppendCompactTask} 的序列化和反序列化，用于在分布式环境中
 * 传输压缩任务信息。
 *
 * <p>追加压缩任务包含：
 * <ul>
 *   <li><b>分区信息</b>：任务所属的分区
 *   <li><b>待压缩文件列表</b>：需要进行压缩的数据文件列表
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li><b>追加表压缩</b>：
 *       <ul>
 *         <li>追加表（Append-Only）的小文件合并
 *         <li>将多个小文件压缩成大文件，提高查询效率
 *       </ul>
 *   <li><b>分布式压缩</b>：
 *       <ul>
 *         <li>在 Flink/Spark 等分布式环境中传输压缩任务
 *         <li>支持跨节点的任务分配和执行
 *       </ul>
 * </ul>
 *
 * <p>序列化格式（Version 2）：
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
 * </pre>
 *
 * <p>版本管理：
 * <ul>
 *   <li><b>当前版本</b>：Version 2
 *   <li><b>版本兼容性</b>：
 *       <ul>
 *         <li>不同版本不兼容，需要重启作业
 *         <li>不支持从旧版本 Savepoint 恢复
 *         <li>这是为了保持序列化格式的简洁性
 *       </ul>
 * </ul>
 *
 * <p>使用示例：
 * <pre>
 * // 序列化
 * AppendCompactTaskSerializer serializer = new AppendCompactTaskSerializer();
 * byte[] bytes = serializer.serialize(task);
 *
 * // 反序列化
 * AppendCompactTask task = serializer.deserialize(version, bytes);
 * </pre>
 *
 * @see AppendCompactTask 追加压缩任务
 * @see DataFileMetaSerializer 数据文件元数据序列化器
 */
public class AppendCompactTaskSerializer implements VersionedSerializer<AppendCompactTask> {

    /** 当前序列化版本号 */
    private static final int CURRENT_VERSION = 2;

    /** 数据文件元数据序列化器 */
    private final DataFileMetaSerializer dataFileSerializer;

    /**
     * 构造追加压缩任务序列化器。
     */
    public AppendCompactTaskSerializer() {
        this.dataFileSerializer = new DataFileMetaSerializer();
    }

    /**
     * 获取当前序列化版本号。
     *
     * @return 版本号 2
     */
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    /**
     * 序列化压缩任务为字节数组。
     *
     * @param obj 待序列化的压缩任务
     * @return 序列化后的字节数组
     * @throws IOException 序列化失败
     */
    @Override
    public byte[] serialize(AppendCompactTask obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(obj, view);
        return out.toByteArray();
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
    public void serializeList(List<AppendCompactTask> list, DataOutputView view)
            throws IOException {
        view.writeInt(list.size());
        for (AppendCompactTask commitMessage : list) {
            serialize(commitMessage, view);
        }
    }

    /**
     * 序列化单个压缩任务。
     *
     * @param task 压缩任务
     * @param view 输出视图
     * @throws IOException 序列化失败
     */
    private void serialize(AppendCompactTask task, DataOutputView view) throws IOException {
        // 序列化分区
        serializeBinaryRow(task.partition(), view);
        // 序列化待压缩文件列表
        dataFileSerializer.serializeList(task.compactBefore(), view);
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
    public AppendCompactTask deserialize(int version, byte[] serialized) throws IOException {
        checkVersion(version);
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return deserialize(view);
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
    public List<AppendCompactTask> deserializeList(int version, DataInputView view)
            throws IOException {
        checkVersion(version);
        int length = view.readInt();
        List<AppendCompactTask> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(deserialize(view));
        }
        return list;
    }

    /**
     * 检查版本兼容性。
     *
     * <p>追加压缩任务不支持版本兼容，必须使用相同的版本号。
     *
     * @param version 要检查的版本号
     * @throws UnsupportedOperationException 如果版本不匹配
     */
    private void checkVersion(int version) {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting CompactionTask version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".\nCompactionTask is not a compatible data structure. "
                            + "Please restart the job afresh (do not recover from savepoint).");
        }
    }

    /**
     * 反序列化单个压缩任务。
     *
     * @param view 输入视图
     * @return 反序列化的压缩任务
     * @throws IOException 反序列化失败
     */
    private AppendCompactTask deserialize(DataInputView view) throws IOException {
        return new AppendCompactTask(
                deserializeBinaryRow(view), dataFileSerializer.deserializeList(view));
    }
}
