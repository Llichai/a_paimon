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

package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.manifest.ManifestEntry.recordCount;

/**
 * 清理不存在的清单文件操作。
 *
 * <p>用于清理 Paimon 表中的孤立清单文件。清单（Manifest）文件记录了表的数据文件信息，
 * 在某些情况下（如异常中止、意外删除等），清单文件可能会与实际的数据文件不一致。
 * 本操作扫描并清理那些在清单中引用但实际不存在的文件记录。
 *
 * <p>主要特性：
 * <ul>
 *   <li>扫描所有快照和清单文件</li>
 *   <li>检测清单中引用但不存在的数据文件</li>
 *   <li>自动清理孤立的清单文件记录</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>不删除任何实际的数据文件</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 清理表的孤立清单文件
 *     RemoveUnexistingManifestsAction action = new RemoveUnexistingManifestsAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig
 *     );
 *     action.run();
 * }</pre>
 *
 * <p>清理过程：
 * <ul>
 *   <li>读取表的所有快照信息</li>
 *   <li>解析每个快照的清单文件列表</li>
 *   <li>验证清单文件中引用的数据文件是否存在</li>
 *   <li>删除指向不存在数据文件的清单条目</li>
 *   <li>重新提交更新后的清单信息</li>
 * </ul>
 *
 * <p>什么时候需要执行本操作：
 * <ul>
 *   <li>表级别的数据不一致问题</li>
 *   <li>删除数据文件后，清单信息未及时更新</li>
 *   <li>异常中止导致的元数据不一致</li>
 *   <li>存储中发现孤立的文件记录</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>本操作仅清理清单元数据，不删除数据文件</li>
 *   <li>执行前建议备份表的元数据</li>
 *   <li>若发现大量孤立清单，应检查表的整体健康状态</li>
 *   <li>清理过程中表应处于空闲状态</li>
 * </ul>
 *
 * @see ManifestList
 * @see ActionBase
 * @since 0.1
 */
public class RemoveUnexistingManifestsAction extends ActionBase implements LocalAction {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoveUnexistingManifestsAction.class);

    private final String databaseName;
    private final String tableName;

    public RemoveUnexistingManifestsAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public void executeLocally() throws Exception {
        Identifier identifier = new Identifier(databaseName, tableName);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        FileIO fileIO = table.fileIO();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        if (latest == null) {
            return;
        }

        ManifestsReader manifestsReader = table.store().newScan().manifestsReader();
        ManifestsReader.Result manifestsResult = manifestsReader.read(latest, ScanMode.ALL);
        List<ManifestFileMeta> manifests = manifestsResult.allManifests;
        List<ManifestFileMeta> existingManifestFiles = new ArrayList<>();
        List<ManifestEntry> baseManifestEntries = new ArrayList<>();

        FileStorePathFactory pathFactory = table.store().pathFactory();
        boolean brokenManifestFile = false;
        for (ManifestFileMeta meta : manifests) {
            try {
                Path path = pathFactory.toManifestFilePath(meta.fileName());
                if (!fileIO.exists(path)) {
                    brokenManifestFile = true;
                    LOG.warn("Drop manifest file: " + meta.fileName());
                } else {
                    baseManifestEntries.addAll(table.store().newScan().readManifest(meta));
                    existingManifestFiles.add(meta);
                }
            } catch (Exception e) {
                throw new RuntimeException("Exception happens", e);
            }
        }

        if (!brokenManifestFile) {
            return;
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        long totalRecordCount = recordCount(baseManifestEntries);
        Pair<String, Long> baseManifestList = manifestList.write(existingManifestFiles);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());

        try (FileStoreCommitImpl fileStoreCommit =
                (FileStoreCommitImpl)
                        table.store().newCommit("Repair-table-" + UUID.randomUUID(), table)) {
            boolean result =
                    fileStoreCommit.replaceManifestList(
                            latest, totalRecordCount, baseManifestList, deltaManifestList);
            if (!result) {
                throw new RuntimeException(
                        "Failed, snapshot conflict, maybe multiple jobs is running to commit snapshots.");
            }
        }
    }
}
