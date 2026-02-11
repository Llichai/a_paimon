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
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 文件通道管理器实现类
 *
 * <p>基于多个临时目录创建和删除文件通道的管理器。
 *
 * <p>核心特性：
 * <ul>
 *   <li>多目录支持：支持配置多个临时目录，提高 I/O 吞吐量
 *   <li>轮询分配：使用轮询算法将文件均匀分配到各个目录
 *   <li>唯一 ID：使用 UUID + 随机数生成唯一的通道 ID
 *   <li>自动清理：关闭时自动删除所有临时目录和文件
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>初始化：在每个临时目录下创建 "paimon-{prefix}-{uuid}" 子目录
 *   <li>创建通道：使用原子计数器轮询选择目录，生成随机文件名
 *   <li>清理资源：关闭时递归删除所有临时子目录
 * </ol>
 *
 * <p>目录结构示例：
 * <pre>
 * /tmp/paimon-io-abc123/
 *   ├── data_456.tmp
 *   ├── data_789.tmp
 * /data/temp/paimon-io-abc123/
 *   ├── data_012.tmp
 * </pre>
 *
 * <p>并发安全：
 * <ul>
 *   <li>使用 AtomicLong 保证轮询计数器的线程安全
 *   <li>Random 实例非线程安全，但每个通道独立创建
 * </ul>
 */
public class FileChannelManagerImpl implements FileChannelManager {
    private static final Logger LOG = LoggerFactory.getLogger(FileChannelManagerImpl.class);

    /** 临时目录数组 */
    private final File[] paths;

    /** 随机数生成器（用于生成文件名） */
    private final Random random;

    /** 下一个路径索引（原子计数器） */
    private final AtomicLong nextPath = new AtomicLong(0);

    /**
     * 构造文件通道管理器
     *
     * @param tempDirs 临时目录路径数组
     * @param prefix 子目录前缀
     */
    public FileChannelManagerImpl(String[] tempDirs, String prefix) {
        checkNotNull(tempDirs, "The temporary directories must not be null.");
        checkArgument(tempDirs.length > 0, "The temporary directories must not be empty.");

        this.random = new Random();

        // 注册关闭钩子后创建目录，确保目录在需要时可以被删除
        this.paths = createFiles(tempDirs, prefix);

        LOG.info(
                "Created a new {} for spilling of task related data to disk (joins, sorting, ...). Used directories:\n\t{}",
                FileChannelManager.class.getSimpleName(),
                String.join("\n\t", tempDirs));
    }

    /**
     * 创建临时目录
     *
     * <p>在每个临时目录下创建 "paimon-{prefix}-{uuid}" 子目录。
     *
     * @param tempDirs 临时目录路径数组
     * @param prefix 子目录前缀
     * @return 创建的子目录数组
     * @throws RuntimeException 所有目录创建失败
     */
    private static File[] createFiles(String[] tempDirs, String prefix) {
        List<File> filesList = new ArrayList<>();
        for (String tempDir : tempDirs) {
            File baseDir = new File(tempDir);
            // 生成唯一子目录名
            String subfolder = String.format("paimon-%s-%s", prefix, UUID.randomUUID());
            File storageDir = new File(baseDir, subfolder);

            // 创建目录
            if (!storageDir.exists() && !storageDir.mkdirs()) {
                LOG.warn(
                        "Failed to create directory {}, temp directory {} will not be used",
                        storageDir.getAbsolutePath(),
                        tempDir);
                continue;
            }

            filesList.add(storageDir);

            LOG.debug(
                    "FileChannelManager uses directory {} for spill files.",
                    storageDir.getAbsolutePath());
        }

        // 至少需要一个可用目录
        if (filesList.isEmpty()) {
            throw new RuntimeException("No available temporary directories");
        }

        return filesList.toArray(new File[0]);
    }

    /**
     * 创建文件通道 ID
     *
     * <p>使用轮询算法选择目录，创建随机文件名。
     *
     * @return 文件通道 ID
     */
    @Override
    public ID createChannel() {
        // 轮询选择目录
        int num = (int) (nextPath.getAndIncrement() % paths.length);
        return new ID(paths[num], num, random);
    }

    /**
     * 创建带前缀的文件通道 ID
     *
     * @param prefix 文件名前缀
     * @return 文件通道 ID
     */
    @Override
    public ID createChannel(String prefix) {
        // 轮询选择目录
        int num = (int) (nextPath.getAndIncrement() % paths.length);
        return new ID(paths[num], num, prefix, random);
    }

    /**
     * 创建通道枚举器
     *
     * @return 通道枚举器
     */
    @Override
    public Enumerator createChannelEnumerator() {
        return new Enumerator(paths, random);
    }

    /**
     * 获取所有临时目录路径
     *
     * @return 临时目录数组（副本）
     */
    @Override
    public File[] getPaths() {
        return Arrays.copyOf(paths, paths.length);
    }

    /**
     * 删除所有临时目录
     *
     * <p>递归删除所有创建的临时子目录及其文件。
     *
     * @throws Exception 清理失败
     */
    @Override
    public void close() throws Exception {
        IOUtils.closeAll(
                Arrays.stream(paths)
                        .filter(File::exists)
                        .map(this::getFileCloser)
                        .collect(Collectors.toList()));
        LOG.info(
                "Closed {} with directories:\n\t{}",
                FileChannelManager.class.getSimpleName(),
                Arrays.stream(paths)
                        .map(File::getAbsolutePath)
                        .collect(Collectors.joining("\n\t")));
    }

    /**
     * 获取文件清理器
     *
     * <p>创建一个 AutoCloseable 对象，用于递归删除指定目录。
     *
     * @param path 要删除的目录
     * @return 文件清理器
     */
    private AutoCloseable getFileCloser(File path) {
        return () -> {
            try {
                FileIOUtils.deleteDirectory(path);
                LOG.info(
                        "FileChannelManager removed spill file directory {}",
                        path.getAbsolutePath());
            } catch (IOException e) {
                String errorMessage =
                        String.format(
                                "FileChannelManager failed to properly clean up temp file directory: %s",
                                path);
                throw new UncheckedIOException(errorMessage, e);
            }
        };
    }
}
