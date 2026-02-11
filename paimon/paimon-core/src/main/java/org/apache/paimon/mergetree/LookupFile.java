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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Lookup 文件
 *
 * <p>用于将远程文件缓存到本地的 Lookup 文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>get：根据键查询值（Lookup）
 *   <li>close：关闭文件并清理本地缓存
 *   <li>访问统计：跟踪请求次数和命中次数
 * </ul>
 *
 * <p>缓存策略：
 * <ul>
 *   <li>使用 Caffeine 缓存管理本地文件
 *   <li>基于访问时间的过期策略（expireAfterAccess）
 *   <li>基于磁盘大小的淘汰策略（maximumWeight）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：缓存远程文件到本地以提供快速查询
 *   <li>减少网络请求：本地缓存避免重复下载
 *   <li>LRU 淘汰：自动清理不常用的文件
 * </ul>
 */
public class LookupFile {

    private static final Logger LOG = LoggerFactory.getLogger(LookupFile.class);

    /** 本地文件路径 */
    private final File localFile;
    /** 层级 */
    private final int level;
    /** Schema ID */
    private final long schemaId;
    /** 序列化版本 */
    private final String serVersion;
    /** Lookup 存储读取器 */
    private final LookupStoreReader reader;
    /** 关闭回调 */
    private final Runnable callback;

    /** 请求次数 */
    private long requestCount;
    /** 命中次数 */
    private long hitCount;
    /** 是否已关闭 */
    private boolean isClosed = false;

    /**
     * 构造 Lookup 文件
     *
     * @param localFile 本地文件
     * @param level 层级
     * @param schemaId Schema ID
     * @param serVersion 序列化版本
     * @param reader Lookup 存储读取器
     * @param callback 关闭回调
     */
    public LookupFile(
            File localFile,
            int level,
            long schemaId,
            String serVersion,
            LookupStoreReader reader,
            Runnable callback) {
        this.localFile = localFile;
        this.level = level;
        this.schemaId = schemaId;
        this.serVersion = serVersion;
        this.reader = reader;
        this.callback = callback;
    }

    /**
     * 获取本地文件
     *
     * @return 本地文件
     */
    public File localFile() {
        return localFile;
    }

    /**
     * 获取 Schema ID
     *
     * @return Schema ID
     */
    public long schemaId() {
        return schemaId;
    }

    /**
     * 获取序列化版本
     *
     * @return 序列化版本
     */
    public String serVersion() {
        return serVersion;
    }

    /**
     * 根据键查询值
     *
     * <p>统计请求次数和命中次数
     *
     * @param key 键（字节数组）
     * @return 值（字节数组），如果不存在则返回 null
     * @throws IOException IO 异常
     */
    @Nullable
    public byte[] get(byte[] key) throws IOException {
        checkArgument(!isClosed);
        requestCount++; // 请求计数+1
        byte[] res = reader.lookup(key);
        if (res != null) {
            hitCount++; // 命中计数+1
        }
        return res;
    }

    /**
     * 获取层级
     *
     * @return 层级
     */
    public int level() {
        return level;
    }

    /**
     * 是否已关闭
     *
     * @return 是否已关闭
     */
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * 关闭文件
     *
     * <p>关闭读取器、执行回调、记录统计信息、删除本地文件
     *
     * @param cause 移除原因
     * @throws IOException IO 异常
     */
    public void close(RemovalCause cause) throws IOException {
        reader.close();
        isClosed = true;
        callback.run();
        LOG.info(
                "Delete Lookup file {} due to {}. Access stats: requestCount={}, hitCount={}, size={}KB",
                localFile.getName(),
                cause,
                requestCount,
                hitCount,
                localFile.length() >> 10);
        FileIOUtils.deleteFileOrDirectory(localFile); // 删除本地文件
    }

    // ==================== 本地文件缓存 ======================

    /**
     * 创建本地文件缓存
     *
     * <p>使用 Caffeine 构建 LRU 缓存：
     * <ul>
     *   <li>expireAfterAccess：基于访问时间的过期策略
     *   <li>maximumWeight：基于磁盘大小的淘汰策略
     *   <li>weigher：文件权重计算器（文件大小）
     *   <li>removalListener：移除监听器（关闭并删除文件）
     * </ul>
     *
     * @param fileRetention 文件保留时间（最后访问时间）
     * @param maxDiskSize 最大磁盘大小
     * @return Caffeine 缓存
     */
    public static Cache<String, LookupFile> createCache(
            Duration fileRetention, MemorySize maxDiskSize) {
        return Caffeine.newBuilder()
                .expireAfterAccess(fileRetention) // 最后访问后过期
                .maximumWeight(maxDiskSize.getKibiBytes()) // 最大权重（KiB）
                .weigher(LookupFile::fileWeigh) // 权重计算器
                .removalListener(LookupFile::removalCallback) // 移除监听器
                .executor(Runnable::run) // 同步执行器
                .build();
    }

    /**
     * 计算文件权重（文件大小，单位 KiB）
     *
     * @param file 缓存键
     * @param lookupFile Lookup 文件
     * @return 权重（KiB）
     */
    private static int fileWeigh(String file, LookupFile lookupFile) {
        return fileKibiBytes(lookupFile.localFile);
    }

    /**
     * 移除回调
     *
     * <p>当文件从缓存中移除时，关闭并删除本地文件
     *
     * @param file 缓存键
     * @param lookupFile Lookup 文件
     * @param cause 移除原因
     */
    private static void removalCallback(String file, LookupFile lookupFile, RemovalCause cause) {
        if (lookupFile != null) {
            try {
                lookupFile.close(cause);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * 生成本地文件前缀
     *
     * <p>格式：
     * <ul>
     *   <li>无分区：bucket-remoteFileName
     *   <li>有分区：partition-bucket-remoteFileName
     * </ul>
     *
     * @param partitionType 分区类型
     * @param partition 分区值
     * @param bucket 桶号
     * @param remoteFileName 远程文件名
     * @return 本地文件前缀
     */
    public static String localFilePrefix(
            RowType partitionType, BinaryRow partition, int bucket, String remoteFileName) {
        if (partition.getFieldCount() == 0) {
            // 无分区
            return String.format("%s-%s", bucket, remoteFileName);
        } else {
            // 有分区：将分区值转为字符串
            String partitionString = partToSimpleString(partitionType, partition, "-", 20);
            return String.format("%s-%s-%s", partitionString, bucket, remoteFileName);
        }
    }
}
