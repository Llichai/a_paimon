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

package org.apache.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * Changelog 元数据
 *
 * <p>Changelog 是独立于 Snapshot 的元数据文件，用于存储表的变更日志信息。
 *
 * <p>核心概念：
 * <ul>
 *   <li><b>独立生命周期</b>：Changelog 的生命周期独立于 Snapshot
 *     <ul>
 *       <li>Snapshot 过期后，Changelog 可以继续保留
 *       <li>用于长期保留变更历史
 *     </ul>
 *   </li>
 *   <li><b>从 Snapshot 生成</b>：Changelog 在 Snapshot 过期时从 Snapshot 生成
 *     <ul>
 *       <li>保留 Snapshot 的 changelogManifestList
 *       <li>保留 Snapshot 的元信息（提交时间、记录数等）
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>与 Snapshot 的关系：
 * <ul>
 *   <li>继承自 {@link Snapshot}，复用所有字段和方法
 *   <li>Snapshot 包含完整的数据状态（baseManifestList + deltaManifestList + changelogManifestList）
 *   <li>Changelog 只包含变更日志（changelogManifestList）
 *   <li>Snapshot 过期时，如果需要保留变更历史，会创建 Changelog 文件
 * </ul>
 *
 * <p>数据来源：
 * <ul>
 *   <li>changelog-producer = 'input'：从外部输入的 Changelog 文件
 *   <li>Changelog 文件路径：{table_path}/changelog/changelog-{id}
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>长期保留变更历史（用于审计、回溯）
 *   <li>与 Snapshot 分离管理生命周期
 *   <li>减少存储开销（只保留变更日志，不保留完整数据）
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 从 Snapshot 创建 Changelog
 * Snapshot snapshot = ...;
 * Changelog changelog = new Changelog(snapshot);
 *
 * // 序列化为 JSON
 * String json = changelog.toJson();
 *
 * // 从 JSON 反序列化
 * Changelog changelog = Changelog.fromJson(json);
 *
 * // 从文件读取
 * Changelog changelog = Changelog.fromPath(fileIO, path);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Changelog extends Snapshot {

    /**
     * 从 Snapshot 创建 Changelog
     *
     * <p>复制 Snapshot 的所有字段到 Changelog。
     *
     * @param snapshot 源 Snapshot
     */
    public Changelog(Snapshot snapshot) {
        this(
                snapshot.version(),
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.baseManifestList(),
                snapshot.baseManifestListSize(),
                snapshot.deltaManifestList(),
                snapshot.deltaManifestListSize(),
                snapshot.changelogManifestList(),
                snapshot.changelogManifestListSize(),
                snapshot.indexManifest(),
                snapshot.commitUser(),
                snapshot.commitIdentifier(),
                snapshot.commitKind(),
                snapshot.timeMillis(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                snapshot.changelogRecordCount(),
                snapshot.watermark(),
                snapshot.statistics(),
                snapshot.properties,
                snapshot.nextRowId);
    }

    /**
     * 构造函数（用于 Jackson 反序列化）
     *
     * @param version 版本号
     * @param id Changelog ID
     * @param schemaId Schema ID
     * @param baseManifestList Base Manifest List 文件名
     * @param baseManifestListSize Base Manifest List 文件大小
     * @param deltaManifestList Delta Manifest List 文件名
     * @param deltaManifestListSize Delta Manifest List 文件大小
     * @param changelogManifestList Changelog Manifest List 文件名
     * @param changelogManifestListSize Changelog Manifest List 文件大小
     * @param indexManifest Index Manifest 文件名
     * @param commitUser 提交用户
     * @param commitIdentifier 提交标识符
     * @param commitKind 提交类型
     * @param timeMillis 提交时间（毫秒）
     * @param totalRecordCount 总记录数
     * @param deltaRecordCount 增量记录数
     * @param changelogRecordCount 变更日志记录数
     * @param watermark 水位线
     * @param statistics 统计信息
     * @param properties 属性
     * @param nextRowId 下一个行 ID
     */
    @JsonCreator
    public Changelog(
            @JsonProperty(FIELD_VERSION) int version,
            @JsonProperty(FIELD_ID) long id,
            @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
            @JsonProperty(FIELD_BASE_MANIFEST_LIST) String baseManifestList,
            @JsonProperty(FIELD_BASE_MANIFEST_LIST_SIZE) @Nullable Long baseManifestListSize,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST) String deltaManifestList,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST_SIZE) @Nullable Long deltaManifestListSize,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST) @Nullable String changelogManifestList,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST_SIZE) @Nullable
                    Long changelogManifestListSize,
            @JsonProperty(FIELD_INDEX_MANIFEST) @Nullable String indexManifest,
            @JsonProperty(FIELD_COMMIT_USER) String commitUser,
            @JsonProperty(FIELD_COMMIT_IDENTIFIER) long commitIdentifier,
            @JsonProperty(FIELD_COMMIT_KIND) CommitKind commitKind,
            @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
            @JsonProperty(FIELD_TOTAL_RECORD_COUNT) long totalRecordCount,
            @JsonProperty(FIELD_DELTA_RECORD_COUNT) long deltaRecordCount,
            @JsonProperty(FIELD_CHANGELOG_RECORD_COUNT) @Nullable Long changelogRecordCount,
            @JsonProperty(FIELD_WATERMARK) @Nullable Long watermark,
            @JsonProperty(FIELD_STATISTICS) @Nullable String statistics,
            @JsonProperty(FIELD_PROPERTIES) Map<String, String> properties,
            @JsonProperty(FIELD_NEXT_ROW_ID) @Nullable Long nextRowId) {
        super(
                version,
                id,
                schemaId,
                baseManifestList,
                baseManifestListSize,
                deltaManifestList,
                deltaManifestListSize,
                changelogManifestList,
                changelogManifestListSize,
                indexManifest,
                commitUser,
                commitIdentifier,
                commitKind,
                timeMillis,
                totalRecordCount,
                deltaRecordCount,
                changelogRecordCount,
                watermark,
                statistics,
                properties,
                nextRowId);
    }

    /**
     * 从 JSON 字符串反序列化 Changelog
     *
     * @param json JSON 字符串
     * @return Changelog 实例
     */
    public static Changelog fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Changelog.class);
    }

    /**
     * 从文件路径读取 Changelog
     *
     * <p>如果文件不存在，抛出 RuntimeException。
     *
     * @param fileIO 文件 I/O 接口
     * @param path 文件路径
     * @return Changelog 实例
     * @throws RuntimeException 如果读取失败
     */
    public static Changelog fromPath(FileIO fileIO, Path path) {
        try {
            return tryFromPath(fileIO, path);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Fails to read changelog from path " + path, e);
        }
    }

    /**
     * 尝试从文件路径读取 Changelog
     *
     * <p>如果文件不存在，抛出 FileNotFoundException。
     *
     * @param fileIO 文件 I/O 接口
     * @param path 文件路径
     * @return Changelog 实例
     * @throws FileNotFoundException 如果文件不存在
     * @throws RuntimeException 如果读取失败
     */
    public static Changelog tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        try {
            String json = fileIO.readFileUtf8(path);
            return Changelog.fromJson(json);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException("Fails to read changelog from path " + path, e);
        }
    }
}
