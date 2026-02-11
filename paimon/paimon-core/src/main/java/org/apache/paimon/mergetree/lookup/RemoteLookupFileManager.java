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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 远程 Lookup 文件管理器
 *
 * <p>管理用于 Lookup 的远程文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>genRemoteLookupFile：生成远程 Lookup 文件（上传到远程存储）
 *   <li>tryToDownload：下载远程文件到本地
 *   <li>层级过滤：只处理高于阈值层级的文件
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>生成本地 Lookup 文件（LookupFile）
 *   <li>上传到远程存储（SST 文件）
 *   <li>更新文件元数据（添加 extraFiles）
 *   <li>查询时下载到本地缓存
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：远程存储 Lookup 文件
 *   <li>本地缓存：下载到本地提供快速查询
 *   <li>减少计算：预先生成 Lookup 索引
 * </ul>
 */
public class RemoteLookupFileManager<T> implements RemoteFileDownloader {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteLookupFileManager.class);

    /** 文件 IO */
    private final FileIO fileIO;
    /** 数据文件路径工厂 */
    private final DataFilePathFactory pathFactory;
    /** Lookup Levels */
    private final LookupLevels<T> lookupLevels;
    /** 层级阈值（只处理高于此阈值的层级） */
    private final int levelThreshold;

    /**
     * 构造远程 Lookup 文件管理器
     *
     * @param fileIO 文件 IO
     * @param pathFactory 数据文件路径工厂
     * @param lookupLevels Lookup Levels
     * @param levelThreshold 层级阈值
     */
    public RemoteLookupFileManager(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            LookupLevels<T> lookupLevels,
            int levelThreshold) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.lookupLevels = lookupLevels;
        this.levelThreshold = levelThreshold;
        this.lookupLevels.setRemoteFileDownloader(this);
    }

    /**
     * 生成远程 Lookup 文件
     *
     * <p>流程：
     * <ol>
     *   <li>检查层级：低于阈值直接返回
     *   <li>检查存在：已存在则跳过
     *   <li>创建本地 Lookup 文件
     *   <li>上传到远程存储
     *   <li>更新文件元数据（添加远程 SST 文件名）
     * </ol>
     *
     * @param file 数据文件元数据
     * @return 更新后的文件元数据（包含远程 SST 文件名）
     * @throws IOException IO 异常
     */
    public DataFileMeta genRemoteLookupFile(DataFileMeta file) throws IOException {
        if (file.level() < levelThreshold) {
            return file; // 层级低于阈值，跳过
        }

        if (lookupLevels.remoteSst(file).isPresent()) {
            // ignore existed
            return file; // 已存在，跳过
        }

        // 创建本地 Lookup 文件
        LookupFile lookupFile = lookupLevels.createLookupFile(file);
        long length = lookupFile.localFile().length();
        String remoteSstName = lookupLevels.newRemoteSst(file, length);
        Path sstFile = remoteSstPath(file, remoteSstName);
        // 上传到远程存储
        try (FileInputStream is = new FileInputStream(lookupFile.localFile());
                PositionOutputStream os = fileIO.newOutputStream(sstFile, true)) {
            IOUtils.copy(is, os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 添加到本地缓存
        lookupLevels.addLocalFile(file, lookupFile);
        // 更新文件元数据
        List<String> extraFiles = new ArrayList<>(file.extraFiles());
        extraFiles.add(remoteSstName); // 添加远程 SST 文件名
        return file.copy(extraFiles);
    }

    /**
     * 尝试下载远程文件到本地
     *
     * @param dataFile 数据文件元数据
     * @param remoteSstFile 远程 SST 文件名
     * @param localFile 本地文件路径
     * @return true 表示下载成功，false 表示下载失败
     */
    @Override
    public boolean tryToDownload(DataFileMeta dataFile, String remoteSstFile, File localFile) {
        Path remoteSstPath = remoteSstPath(dataFile, remoteSstFile);
        try (SeekableInputStream is = fileIO.newInputStream(remoteSstPath);
                FileOutputStream os = new FileOutputStream(localFile)) {
            IOUtils.copy(is, os); // 复制远程文件到本地
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to download remote lookup file {}, skipping.", remoteSstPath, e);
            return false;
        }
    }

    /**
     * 获取远程 SST 文件路径
     *
     * @param file 数据文件元数据
     * @param remoteSstName 远程 SST 文件名
     * @return 远程 SST 文件路径
     */
    private Path remoteSstPath(DataFileMeta file, String remoteSstName) {
        return new Path(pathFactory.toPath(file).getParent(), remoteSstName);
    }
}
