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

package org.apache.paimon.disk;

import org.apache.paimon.disk.FileIOChannel.Enumerator;
import org.apache.paimon.disk.FileIOChannel.ID;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * IO 管理器实现
 *
 * <p>提供磁盘 I/O 服务的外观实现。
 *
 * <p>核心特性：
 * <ul>
 *   <li>懒初始化：文件通道管理器按需创建
 *   <li>多目录支持：支持多个临时目录提高 I/O 并行度
 *   <li>自动清理：关闭时删除所有临时文件
 *   <li>线程安全：使用双重检查锁定初始化通道管理器
 * </ul>
 *
 * <p>临时目录结构：
 * <pre>
 * tempDir/
 *   └── io-{UUID}/
 *       ├── channel-file-1
 *       ├── channel-file-2
 *       └── ...
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>排序溢写：外部排序的临时文件管理
 *   <li>哈希表溢写：哈希表分区的磁盘溢写
 *   <li>缓冲区溢写：内存不足时的数据溢写
 * </ul>
 */
public class IOManagerImpl implements IOManager {

    protected static final Logger LOG = LoggerFactory.getLogger(IOManagerImpl.class);

    /** 目录名前缀 */
    private static final String DIR_NAME_PREFIX = "io";

    /** 临时目录数组 */
    private final String[] tempDirs;

    /** 文件通道管理器（懒初始化） */
    private volatile FileChannelManager lazyChannelManager;

    // -------------------------------------------------------------------------
    //               Constructors / Destructors
    // -------------------------------------------------------------------------

    /**
     * 构造 IO 管理器
     *
     * @param tempDirs 临时目录路径数组
     */
    public IOManagerImpl(String... tempDirs) {
        Preconditions.checkNotNull(tempDirs);
        this.tempDirs = tempDirs;
    }

    /**
     * 获取文件通道管理器
     *
     * <p>使用双重检查锁定实现线程安全的懒初始化
     *
     * @return 文件通道管理器
     */
    private FileChannelManager fileChannelManager() {
        if (lazyChannelManager == null) {
            synchronized (this) {
                if (lazyChannelManager == null) {
                    lazyChannelManager = new FileChannelManagerImpl(tempDirs, DIR_NAME_PREFIX);
                }
            }
        }

        return lazyChannelManager;
    }

    /**
     * 关闭 IO 管理器
     *
     * <p>删除所有临时文件和目录
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void close() throws Exception {
        if (lazyChannelManager != null) {
            lazyChannelManager.close();
        }
    }

    /**
     * 创建文件通道
     *
     * @return 文件通道 ID
     */
    @Override
    public ID createChannel() {
        return fileChannelManager().createChannel();
    }

    /**
     * 创建带前缀的文件通道
     *
     * @param prefix 文件名前缀
     * @return 文件通道 ID
     */
    @Override
    public ID createChannel(String prefix) {
        return fileChannelManager().createChannel(prefix);
    }

    /**
     * 获取临时目录数组
     *
     * @return 临时目录路径数组
     */
    @Override
    public String[] tempDirs() {
        return tempDirs;
    }

    /**
     * 创建通道枚举器
     *
     * @return 通道枚举器
     */
    @Override
    public Enumerator createChannelEnumerator() {
        return fileChannelManager().createChannelEnumerator();
    }

    /**
     * 删除文件通道
     *
     * <p>删除通道对应的底层文件。如果通道仍然打开，此调用可能失败。
     *
     * @param channel 要删除的通道 ID
     */
    public static void deleteChannel(ID channel) {
        if (channel != null) {
            if (channel.getPathFile().exists() && !channel.getPathFile().delete()) {
                LOG.warn("IOManager failed to delete temporary file {}", channel.getPath());
            }
        }
    }

    /**
     * 获取溢写目录
     *
     * @return 溢写目录数组
     */
    public File[] getSpillingDirectories() {
        return fileChannelManager().getPaths();
    }

    /**
     * 获取溢写目录路径字符串
     *
     * @return 溢写目录路径字符串数组
     */
    public String[] getSpillingDirectoriesPaths() {
        File[] paths = fileChannelManager().getPaths();
        String[] strings = new String[paths.length];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = paths[i].getAbsolutePath();
        }
        return strings;
    }

    /**
     * 获取溢写目录路径字符串（用于日志）
     *
     * @return 溢写目录路径字符串（换行分隔）
     */
    private String getSpillingDirectoriesPathsString() {
        return Arrays.stream(fileChannelManager().getPaths())
                .map(File::getAbsolutePath)
                .collect(Collectors.joining("\n\t"));
    }

    /**
     * 创建缓冲文件写入器
     *
     * @param channelID 文件通道 ID
     * @return 缓冲文件写入器
     * @throws IOException IO 异常
     */
    @Override
    public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException {
        return new BufferFileWriterImpl(channelID);
    }

    /**
     * 创建缓冲文件读取器
     *
     * @param channelID 文件通道 ID
     * @return 缓冲文件读取器
     * @throws IOException IO 异常
     */
    @Override
    public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) throws IOException {
        return new BufferFileReaderImpl(channelID);
    }

    /**
     * 分割路径字符串
     *
     * <p>支持逗号或系统路径分隔符分隔的多个路径
     *
     * @param separatedPaths 分隔的路径字符串
     * @return 路径数组
     */
    public static String[] splitPaths(@Nonnull String separatedPaths) {
        return separatedPaths.length() > 0
                ? separatedPaths.split(",|" + File.pathSeparator)
                : new String[0];
    }
}
