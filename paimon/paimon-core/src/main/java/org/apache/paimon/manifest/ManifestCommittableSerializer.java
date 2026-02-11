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

package org.apache.paimon.manifest;

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageLegacyV2Serializer;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ManifestCommittable 序列化器
 *
 * <p>负责将 {@link ManifestCommittable} 序列化为字节数组以及反序列化（用于状态存储和网络传输）。
 *
 * <p>序列化格式：
 * <ul>
 *   <li>identifier（Long）- 提交标识符
 *   <li>watermark（Boolean + Long）- 是否为 null + 水位线值
 *   <li>properties（Int + Map）- 属性数量 + 键值对列表
 *   <li>commitMessageVersion（Int）- CommitMessage 序列化版本
 *   <li>commitMessages（List）- CommitMessage 列表
 * </ul>
 *
 * <p>版本演化：
 * <ul>
 *   <li>版本 1-3：早期版本（已废弃）
 *   <li>版本 4：添加 properties 字段
 *   <li>版本 5：当前版本（移除 legacy log offsets）
 * </ul>
 *
 * <p>兼容性处理：
 * <ul>
 *   <li>版本 1-4：跳过 legacy log offsets 字段
 *   <li>版本 2：使用 {@link CommitMessageLegacyV2Serializer} 反序列化
 *   <li>向后兼容：支持反序列化旧版本数据
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Checkpoint：Flink 在 checkpoint 时序列化 ManifestCommittable
 *   <li>网络传输：Job Manager 和 Task Manager 之间传输
 *   <li>状态恢复：从 checkpoint 恢复时反序列化
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 序列化
 * ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
 * byte[] bytes = serializer.serialize(committable);
 *
 * // 反序列化
 * ManifestCommittable restored = serializer.deserialize(version, bytes);
 * }</pre>
 */
public class ManifestCommittableSerializer implements VersionedSerializer<ManifestCommittable> {

    /** 当前版本号 */
    private static final int CURRENT_VERSION = 5;

    /** CommitMessage 序列化器 */
    private final CommitMessageSerializer commitMessageSerializer;

    /** Legacy V2 CommitMessage 序列化器（用于兼容旧版本） */
    private CommitMessageLegacyV2Serializer legacyV2CommitMessageSerializer;

    /** 构造 ManifestCommittableSerializer */
    public ManifestCommittableSerializer() {
        this.commitMessageSerializer = new CommitMessageSerializer();
    }

    /** 获取当前版本号 */
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    /**
     * 序列化 ManifestCommittable 为字节数组
     *
     * @param obj ManifestCommittable 实例
     * @return 字节数组
     * @throws IOException 序列化失败
     */
    @Override
    public byte[] serialize(ManifestCommittable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeLong(obj.identifier());
        Long watermark = obj.watermark();
        if (watermark == null) {
            view.writeBoolean(true);
        } else {
            view.writeBoolean(false);
            view.writeLong(watermark);
        }
        serializeProperties(view, obj.properties());
        view.writeInt(commitMessageSerializer.getVersion());
        commitMessageSerializer.serializeList(obj.fileCommittables(), view);
        return out.toByteArray();
    }

    /**
     * 序列化自定义属性
     *
     * @param view 输出流
     * @param properties 属性 Map
     * @throws IOException 序列化失败
     */
    private void serializeProperties(
            DataOutputViewStreamWrapper view, Map<String, String> properties) throws IOException {
        view.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            view.writeUTF(entry.getKey());
            view.writeUTF(entry.getValue());
        }
    }

    /**
     * 反序列化字节数组为 ManifestCommittable
     *
     * <p>支持向后兼容，可以反序列化旧版本数据。
     *
     * @param version 序列化版本号
     * @param serialized 字节数组
     * @return ManifestCommittable 实例
     * @throws IOException 反序列化失败
     */
    @Override
    public ManifestCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version > CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting ManifestCommittableSerializer version to be smaller or equal than "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }

        DataInputDeserializer view = new DataInputDeserializer(serialized);
        long identifier = view.readLong();
        Long watermark = view.readBoolean() ? null : view.readLong();
        // 版本 1-4 包含 legacy log offsets，需要跳过
        if (version <= 4) {
            skipLegacyLogOffsets(view);
        }
        // 版本 4+ 包含 properties
        Map<String, String> properties =
                version >= 4 ? deserializeProperties(view) : new HashMap<>();
        int fileCommittableSerializerVersion = view.readInt();
        List<CommitMessage> fileCommittables;
        try {
            fileCommittables =
                    commitMessageSerializer.deserializeList(fileCommittableSerializerVersion, view);
        } catch (Exception e) {
            // 如果反序列化失败且版本是 2，尝试使用 legacy 序列化器
            if (fileCommittableSerializerVersion != 2) {
                throw e;
            }

            // 重建 view
            view = new DataInputDeserializer(serialized);
            view.readLong();
            if (!view.readBoolean()) {
                view.readLong();
            }
            skipLegacyLogOffsets(view);
            view.readInt();

            if (legacyV2CommitMessageSerializer == null) {
                legacyV2CommitMessageSerializer = new CommitMessageLegacyV2Serializer();
            }
            fileCommittables = legacyV2CommitMessageSerializer.deserializeList(view);
        }

        return new ManifestCommittable(identifier, watermark, fileCommittables, properties);
    }

    /**
     * 跳过 legacy log offsets 字段（版本 1-4）
     *
     * @param view 输入流
     * @throws IOException 读取失败
     */
    private void skipLegacyLogOffsets(DataInputDeserializer view) throws IOException {
        int size = view.readInt();
        for (int i = 0; i < size; i++) {
            view.readInt();
            view.readLong();
        }
    }

    /**
     * 反序列化自定义属性
     *
     * @param view 输入流
     * @return 属性 Map
     * @throws IOException 读取失败
     */
    private Map<String, String> deserializeProperties(DataInputDeserializer view)
            throws IOException {
        int size = view.readInt();
        Map<String, String> properties = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            properties.put(view.readUTF(), view.readUTF());
        }
        return properties;
    }
}
