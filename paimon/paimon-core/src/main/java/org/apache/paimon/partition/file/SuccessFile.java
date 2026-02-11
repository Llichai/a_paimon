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

package org.apache.paimon.partition.file;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

/**
 * 成功文件的 JSON 表示形式。
 *
 * <p>成功文件(通常命名为 _SUCCESS)是数据仓库中常用的标记机制,
 * 用于指示分区数据已完整写入且可以被下游系统安全消费。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>数据完整性保证</b> - 标记数据写入已完成,避免读取不完整数据
 *   <li><b>ETL 流水线协调</b> - 下游任务检查 _SUCCESS 文件后再开始处理
 *   <li><b>增量数据发现</b> - 扫描带有 _SUCCESS 文件的分区进行增量处理
 *   <li><b>数据质量检查点</b> - 作为数据管道中的质量检查点
 * </ul>
 *
 * <h3>文件内容示例</h3>
 * <pre>{@code
 * {
 *   "creationTime": 1704067200000,     // 首次创建时间戳
 *   "modificationTime": 1704153600000  // 最后更新时间戳
 * }
 * }</pre>
 *
 * <h3>时间戳用途</h3>
 * <ul>
 *   <li><b>creationTime</b> - 分区首次标记完成的时间,用于数据溯源
 *   <li><b>modificationTime</b> - 分区最后更新时间,用于增量处理和数据新鲜度判断
 * </ul>
 *
 * <h3>不可变性</h3>
 * <p>该类是不可变的,更新修改时间需要调用 {@link #updateModificationTime(long)} 创建新实例。
 *
 * @see org.apache.paimon.partition.actions.SuccessFileMarkDoneAction
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SuccessFile {

    private static final String FIELD_CREATION_TIME = "creationTime";
    private static final String FIELD_MODIFICATION_TIME = "modificationTime";

    /** 创建时间戳(毫秒),表示成功文件首次创建的时间 */
    @JsonProperty(FIELD_CREATION_TIME)
    private final long creationTime;

    /** 修改时间戳(毫秒),表示成功文件最后更新的时间 */
    @JsonProperty(FIELD_MODIFICATION_TIME)
    private final long modificationTime;

    /**
     * 构造成功文件实例。
     *
     * @param creationTime 创建时间戳(毫秒)
     * @param modificationTime 修改时间戳(毫秒)
     */
    @JsonCreator
    public SuccessFile(
            @JsonProperty(FIELD_CREATION_TIME) long creationTime,
            @JsonProperty(FIELD_MODIFICATION_TIME) long modificationTime) {
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
    }

    /**
     * 获取创建时间戳。
     *
     * @return 创建时间戳(毫秒)
     */
    @JsonGetter(FIELD_CREATION_TIME)
    public long creationTime() {
        return creationTime;
    }

    /**
     * 获取修改时间戳。
     *
     * @return 修改时间戳(毫秒)
     */
    @JsonGetter(FIELD_MODIFICATION_TIME)
    public long modificationTime() {
        return modificationTime;
    }

    /**
     * 创建一个新的成功文件实例并更新修改时间。
     *
     * <p>由于类是不可变的,此方法返回一个新实例而不是修改当前实例。
     * 创建时间保持不变,仅更新修改时间。
     *
     * <h3>使用场景</h3>
     * <p>当分区数据重新写入或更新时,需要更新 _SUCCESS 文件的修改时间,
     * 但保留原始创建时间以追踪首次数据加载时间。
     *
     * @param modificationTime 新的修改时间戳(毫秒)
     * @return 新的成功文件实例,具有更新的修改时间
     */
    public SuccessFile updateModificationTime(long modificationTime) {
        return new SuccessFile(creationTime, modificationTime);
    }

    /**
     * 将成功文件序列化为 JSON 字符串。
     *
     * @return JSON 格式的字符串表示
     */
    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    /**
     * 从 JSON 字符串反序列化成功文件。
     *
     * @param json JSON 格式的字符串
     * @return 反序列化后的成功文件实例
     */
    public static SuccessFile fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, SuccessFile.class);
    }

    /**
     * 安全地从文件路径读取成功文件。
     *
     * <p>如果文件不存在,返回 null 而不是抛出异常。
     * 适用于需要检查成功文件是否存在的场景。
     *
     * @param fileIO 文件 IO 接口
     * @param path 成功文件的路径
     * @return 成功文件实例,如果文件不存在则返回 null
     * @throws IOException 读取文件失败时抛出(不包括文件不存在的情况)
     */
    @Nullable
    public static SuccessFile safelyFromPath(FileIO fileIO, Path path) throws IOException {
        try {
            String json = fileIO.readFileUtf8(path);
            return SuccessFile.fromJson(json);
        } catch (FileNotFoundException e) {
            // 文件不存在时返回 null 而不是抛出异常
            return null;
        }
    }

    /**
     * 从文件路径读取成功文件。
     *
     * <p>如果文件不存在,会抛出 {@link java.io.FileNotFoundException}。
     * 适用于明确要求文件必须存在的场景。
     *
     * @param fileIO 文件 IO 接口
     * @param path 成功文件的路径
     * @return 成功文件实例
     * @throws IOException 读取文件失败或文件不存在时抛出
     */
    public static SuccessFile fromPath(FileIO fileIO, Path path) throws IOException {
        String json = fileIO.readFileUtf8(path);
        return SuccessFile.fromJson(json);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SuccessFile that = (SuccessFile) o;
        return creationTime == that.creationTime && modificationTime == that.modificationTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(creationTime, modificationTime);
    }
}
