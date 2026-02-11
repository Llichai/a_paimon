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

import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Manifest 提交单元
 *
 * <p>ManifestCommittable 是提交阶段的核心数据结构，封装了一次提交的所有信息。
 *
 * <p>四个核心字段：
 * <ul>
 *   <li>identifier：提交标识符（通常是 checkpoint ID）
 *   <li>watermark：水位线（用于流式计算）
 *   <li>commitMessages：提交消息列表（{@link CommitMessage}）
 *   <li>properties：自定义属性（如 log offsets）
 * </ul>
 *
 * <p>提交流程：
 * <pre>
 * 1. Writer 生成 CommitMessage（包含 ManifestEntry）
 * 2. Committer 收集所有 CommitMessage 到 ManifestCommittable
 * 3. FileStoreCommit 处理 ManifestCommittable
 * 4. 写入 Snapshot + ManifestList + ManifestFile
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>批式写入：{@link TableWrite} 生成 ManifestCommittable
 *   <li>流式写入：{@link StreamTableWrite} 在 checkpoint 时生成 ManifestCommittable
 *   <li>提交阶段：{@link FileStoreCommit} 处理 ManifestCommittable
 *   <li>Compaction：{@link CompactManager} 生成 Compaction 的 ManifestCommittable
 * </ul>
 *
 * <p>与 CommitMessage 的区别：
 * <ul>
 *   <li>CommitMessage：单个 Writer 的输出（单个 partition 或 bucket）
 *   <li>ManifestCommittable：整个 Job 的输出（包含所有 CommitMessage）
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建 ManifestCommittable
 * ManifestCommittable committable = new ManifestCommittable(
 *     checkpointId,              // identifier
 *     watermark                  // watermark
 * );
 *
 * // 添加 CommitMessage
 * committable.addFileCommittable(commitMessage1);
 * committable.addFileCommittable(commitMessage2);
 *
 * // 添加自定义属性
 * committable.addProperty("kafka.offset", "100");
 *
 * // 提交
 * fileStoreCommit.commit(committable);
 * }</pre>
 */
public class ManifestCommittable {

    /** 提交标识符（通常是 checkpoint ID） */
    private final long identifier;

    /** 水位线（用于流式计算，可为 null） */
    @Nullable private final Long watermark;

    /** 自定义属性（如 log offsets） */
    private final Map<String, String> properties;

    /** 提交消息列表（包含 ManifestEntry） */
    private final List<CommitMessage> commitMessages;

    /**
     * 构造 ManifestCommittable（无水位线）
     *
     * @param identifier 提交标识符
     */
    public ManifestCommittable(long identifier) {
        this(identifier, null);
    }

    /**
     * 构造 ManifestCommittable
     *
     * @param identifier 提交标识符
     * @param watermark 水位线（可为 null）
     */
    public ManifestCommittable(long identifier, @Nullable Long watermark) {
        this.identifier = identifier;
        this.watermark = watermark;
        this.commitMessages = new ArrayList<>();
        this.properties = new HashMap<>();
    }

    /**
     * 构造 ManifestCommittable（带 CommitMessage 列表）
     *
     * @param identifier 提交标识符
     * @param watermark 水位线
     * @param commitMessages 提交消息列表
     */
    public ManifestCommittable(
            long identifier, @Nullable Long watermark, List<CommitMessage> commitMessages) {
        this(identifier, watermark, commitMessages, new HashMap<>());
    }

    /**
     * 构造 ManifestCommittable（完整参数）
     *
     * @param identifier 提交标识符
     * @param watermark 水位线
     * @param commitMessages 提交消息列表
     * @param properties 自定义属性
     */
    public ManifestCommittable(
            long identifier,
            @Nullable Long watermark,
            List<CommitMessage> commitMessages,
            Map<String, String> properties) {
        this.identifier = identifier;
        this.watermark = watermark;
        this.commitMessages = commitMessages;
        this.properties = properties;
    }

    /**
     * 添加 CommitMessage
     *
     * @param commitMessage 提交消息
     */
    public void addFileCommittable(CommitMessage commitMessage) {
        commitMessages.add(commitMessage);
    }

    /**
     * 添加自定义属性
     *
     * @param key 属性键
     * @param value 属性值
     */
    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    /** 获取提交标识符 */
    public long identifier() {
        return identifier;
    }

    /** 获取水位线（可为 null） */
    @Nullable
    public Long watermark() {
        return watermark;
    }

    /** 获取提交消息列表 */
    public List<CommitMessage> fileCommittables() {
        return commitMessages;
    }

    /** 获取自定义属性 */
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManifestCommittable that = (ManifestCommittable) o;
        return Objects.equals(identifier, that.identifier)
                && Objects.equals(watermark, that.watermark)
                && Objects.equals(commitMessages, that.commitMessages)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, watermark, commitMessages, properties);
    }

    @Override
    public String toString() {
        return String.format(
                "ManifestCommittable {"
                        + "identifier = %s, "
                        + "watermark = %s, "
                        + "commitMessages = %s, "
                        + "properties = %s}",
                identifier, watermark, commitMessages, properties);
    }
}
