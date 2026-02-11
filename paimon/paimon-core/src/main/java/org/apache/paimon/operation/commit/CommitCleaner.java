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

package org.apache.paimon.operation.commit;

import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.Pair;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 提交清理器
 *
 * <p>用于清理提交过程中产生的临时文件。
 *
 * <h2>清理场景</h2>
 * <p>在以下两种场景需要清理临时文件：
 * <ul>
 *   <li><b>提交失败</b>：提交过程中出错，需要清理已生成的临时文件
 *   <li><b>提交成功但复用失败</b>：新旧快照复用Manifest失败，需要清理旧文件
 * </ul>
 *
 * <h2>清理内容</h2>
 * <p>该类负责清理以下类型的文件：
 * <ul>
 *   <li><b>Manifest List文件</b>：包含Manifest文件列表的元文件
 *   <li><b>Manifest文件</b>：包含数据文件列表的文件
 *   <li><b>Index Manifest文件</b>：索引相关的Manifest文件
 * </ul>
 *
 * <h2>清理策略</h2>
 * <p>支持两种清理策略：
 * <ul>
 *   <li><b>复用模式清理</b>：{@link #cleanUpReuseTmpManifests}
 *       <ul>
 *         <li>删除Delta Manifest List及其包含的Manifest文件
 *         <li>删除Changelog Manifest List及其包含的Manifest文件
 *         <li>删除新的Index Manifest文件
 *       </ul>
 *   <li><b>非复用模式清理</b>：{@link #cleanUpNoReuseTmpManifests}
 *       <ul>
 *         <li>删除Base Manifest List
 *         <li>删除新生成的Manifest文件（不在原列表中的）
 *       </ul>
 * </ul>
 *
 * @see ManifestList Manifest List操作
 * @see ManifestFile Manifest文件操作
 * @see IndexManifestFile 索引Manifest文件操作
 */
public class CommitCleaner {

    /** Manifest List操作接口 */
    private final ManifestList manifestList;

    /** Manifest文件操作接口 */
    private final ManifestFile manifestFile;

    /** 索引Manifest文件操作接口 */
    private final IndexManifestFile indexManifestFile;

    /**
     * 构造提交清理器
     *
     * @param manifestList Manifest List操作接口
     * @param manifestFile Manifest文件操作接口
     * @param indexManifestFile 索引Manifest文件操作接口
     */
    public CommitCleaner(
            ManifestList manifestList,
            ManifestFile manifestFile,
            IndexManifestFile indexManifestFile) {
        this.manifestList = manifestList;
        this.manifestFile = manifestFile;
        this.indexManifestFile = indexManifestFile;
    }

    /**
     * 清理复用模式下的临时Manifest文件
     *
     * <p>当快照复用Manifest时，清理Delta和Changelog相关的临时文件。
     *
     * @param deltaManifestList Delta Manifest List文件名及大小
     * @param changelogManifestList Changelog Manifest List文件名及大小
     * @param oldIndexManifest 旧的Index Manifest文件名
     * @param newIndexManifest 新的Index Manifest文件名
     */
    public void cleanUpReuseTmpManifests(
            Pair<String, Long> deltaManifestList,
            Pair<String, Long> changelogManifestList,
            String oldIndexManifest,
            String newIndexManifest) {
        // 清理Delta Manifest List及其包含的Manifest文件
        if (deltaManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(deltaManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(deltaManifestList.getKey());
        }

        // 清理Changelog Manifest List及其包含的Manifest文件
        if (changelogManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(changelogManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(changelogManifestList.getKey());
        }

        // 清理Index Manifest文件
        cleanIndexManifest(oldIndexManifest, newIndexManifest);
    }

    /**
     * 清理非复用模式下的临时Manifest文件
     *
     * <p>当快照不复用Manifest时，清理新生成的Manifest文件。
     *
     * @param baseManifestList Base Manifest List文件名及大小
     * @param mergeBeforeManifests 合并前的Manifest文件列表
     * @param mergeAfterManifests 合并后的Manifest文件列表
     */
    public void cleanUpNoReuseTmpManifests(
            Pair<String, Long> baseManifestList,
            List<ManifestFileMeta> mergeBeforeManifests,
            List<ManifestFileMeta> mergeAfterManifests) {
        // 删除Base Manifest List
        if (baseManifestList != null) {
            manifestList.delete(baseManifestList.getKey());
        }

        // 找出新生成的Manifest文件并删除
        Set<String> oldMetaSet =
                mergeBeforeManifests.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toSet());
        for (ManifestFileMeta suspect : mergeAfterManifests) {
            if (!oldMetaSet.contains(suspect.fileName())) {
                manifestFile.delete(suspect.fileName());
            }
        }
    }

    /**
     * 清理Index Manifest文件
     *
     * <p>如果新Index Manifest与旧的不同，则删除新的。
     *
     * @param oldIndexManifest 旧的Index Manifest文件名
     * @param newIndexManifest 新的Index Manifest文件名
     */
    private void cleanIndexManifest(String oldIndexManifest, String newIndexManifest) {
        if (newIndexManifest != null && !Objects.equals(oldIndexManifest, newIndexManifest)) {
            indexManifestFile.delete(newIndexManifest);
        }
    }
}
