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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;

import java.util.ArrayList;
import java.util.List;

/**
 * 压缩单元
 *
 * <p>表示一次压缩操作的文件集合和相关配置。
 *
 * <p>核心属性：
 * <ul>
 *   <li>outputLevel：压缩结果输出到的层级
 *   <li>files：参与压缩的文件列表
 *   <li>fileRewrite：是否为文件重写模式
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LSM 树压缩：将多个层级的文件合并到目标层级
 *   <li>文件重写：重写单个文件（如更新 schema、删除过期数据）
 * </ul>
 *
 * <p>创建方式：
 * <ul>
 *   <li>{@link #fromLevelRuns}：从 LSM 层级运行中创建
 *   <li>{@link #fromFiles}：从文件列表创建
 * </ul>
 */
public class CompactUnit {

    /** 压缩结果输出到的层级（LSM 树层级） */
    private final int outputLevel;

    /** 参与压缩的文件列表 */
    private final List<DataFileMeta> files;

    /** 是否为文件重写模式（单文件重写，非合并） */
    private final boolean fileRewrite;

    /**
     * 构造压缩单元
     *
     * @param outputLevel 输出层级
     * @param files 参与压缩的文件列表
     * @param fileRewrite 是否为文件重写模式
     */
    public CompactUnit(int outputLevel, List<DataFileMeta> files, boolean fileRewrite) {
        this.outputLevel = outputLevel;
        this.files = files;
        this.fileRewrite = fileRewrite;
    }

    /**
     * 获取输出层级
     *
     * @return LSM 树层级编号
     */
    public int outputLevel() {
        return outputLevel;
    }

    /**
     * 获取参与压缩的文件列表
     *
     * @return 文件元数据列表
     */
    public List<DataFileMeta> files() {
        return files;
    }

    /**
     * 是否为文件重写模式
     *
     * @return true 如果是文件重写（非合并）
     */
    public boolean fileRewrite() {
        return fileRewrite;
    }

    /**
     * 从 LSM 层级运行中创建压缩单元
     *
     * <p>从多个层级的排序运行中收集所有文件，创建合并压缩单元。
     *
     * @param outputLevel 输出层级
     * @param runs LSM 层级运行列表
     * @return 压缩单元
     */
    public static CompactUnit fromLevelRuns(int outputLevel, List<LevelSortedRun> runs) {
        List<DataFileMeta> files = new ArrayList<>();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return fromFiles(outputLevel, files, false);
    }

    /**
     * 从文件列表创建压缩单元
     *
     * @param outputLevel 输出层级
     * @param files 参与压缩的文件列表
     * @param fileRewrite 是否为文件重写模式
     * @return 压缩单元
     */
    public static CompactUnit fromFiles(
            int outputLevel, List<DataFileMeta> files, boolean fileRewrite) {
        return new CompactUnit(outputLevel, files, fileRewrite);
    }
}
