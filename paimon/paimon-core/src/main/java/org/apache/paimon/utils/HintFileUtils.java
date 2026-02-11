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

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/**
 * Hint 文件工具类
 *
 * <p>HintFileUtils 提供 Hint 文件的读取和写入功能。
 *
 * <p>核心功能：
 * <ul>
 *   <li>查找最新版本：{@link #findLatest} - 查找最新的快照或 Changelog 版本
 *   <li>查找最早版本：{@link #findEarliest} - 查找最早的快照或 Changelog 版本
 *   <li>读取 Hint：{@link #readHint} - 读取 Hint 文件内容
 *   <li>提交 Hint：{@link #commitHint} - 提交 Hint 文件
 *   <li>删除 Hint：{@link #deleteLatestHint}, {@link #deleteEarliestHint} - 删除 Hint 文件
 * </ul>
 *
 * <p>Hint 文件类型：
 * <ul>
 *   <li>LATEST：指向最新版本的 Hint 文件
 *   <li>EARLIEST：指向最早版本的 Hint 文件
 * </ul>
 *
 * <p>Hint 文件的作用：
 * <ul>
 *   <li>快速查找：避免列举目录中的所有文件
 *   <li>性能优化：减少文件系统操作
 *   <li>一致性：确保多个读取器看到相同的版本
 * </ul>
 *
 * <p>查找策略：
 * <ul>
 *   <li>优先读取 Hint 文件：快速获取版本号
 *   <li>验证 Hint 文件：检查文件是否存在
 *   <li>回退到列举：如果 Hint 文件不存在或无效，列举目录中的所有文件
 * </ul>
 *
 * <p>重试机制：
 * <ul>
 *   <li>读取重试：最多重试 3 次，间隔 1 毫秒
 *   <li>提交重试：最多重试 3 次，随机间隔 500-1500 毫秒
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>快照管理：查找最新/最早的快照
 *   <li>Changelog 管理：查找最新/最早的 Changelog
 *   <li>版本查询：快速获取版本信息
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * FileIO fileIO = ...;
 * Path snapshotDir = new Path("/path/to/snapshot");
 *
 * // 查找最新快照
 * Long latestSnapshot = HintFileUtils.findLatest(
 *     fileIO,
 *     snapshotDir,
 *     "snapshot-",
 *     version -> new Path(snapshotDir, "snapshot-" + version)
 * );
 * System.out.println("Latest snapshot: " + latestSnapshot);
 *
 * // 查找最早快照
 * Long earliestSnapshot = HintFileUtils.findEarliest(
 *     fileIO,
 *     snapshotDir,
 *     "snapshot-",
 *     version -> new Path(snapshotDir, "snapshot-" + version)
 * );
 * System.out.println("Earliest snapshot: " + earliestSnapshot);
 *
 * // 提交最新 Hint
 * HintFileUtils.commitLatestHint(fileIO, 100L, snapshotDir);
 *
 * // 提交最早 Hint
 * HintFileUtils.commitEarliestHint(fileIO, 1L, snapshotDir);
 *
 * // 读取 Hint
 * Long hintValue = HintFileUtils.readHint(fileIO, HintFileUtils.LATEST, snapshotDir);
 * System.out.println("Hint value: " + hintValue);
 * }</pre>
 *
 * @see SnapshotManager
 * @see ChangelogManager
 */
public class HintFileUtils {

    /** EARLIEST Hint 文件名 */
    public static final String EARLIEST = "EARLIEST";
    /** LATEST Hint 文件名 */
    public static final String LATEST = "LATEST";

    /** 读取 Hint 文件的最大重试次数 */
    private static final int READ_HINT_RETRY_NUM = 3;
    /** 读取 Hint 文件的重试间隔（毫秒） */
    private static final int READ_HINT_RETRY_INTERVAL = 1;

    /**
     * 查找最新版本
     *
     * <p>查找策略：
     * <ol>
     *   <li>读取 LATEST Hint 文件
     *   <li>验证：检查下一个版本是否存在
     *   <li>如果验证失败，回退到列举目录中的所有文件
     * </ol>
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @param prefix 文件前缀
     * @param file 版本号到文件路径的转换函数
     * @return 最新版本号，如果不存在返回 null
     * @throws IOException 如果 I/O 错误
     */
    @Nullable
    public static Long findLatest(FileIO fileIO, Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        Long snapshotId = readHint(fileIO, LATEST, dir);
        if (snapshotId != null && snapshotId > 0) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!fileIO.exists(file.apply(nextSnapshot))) {
                return snapshotId;
            }
        }
        return findByListFiles(fileIO, Math::max, dir, prefix);
    }

    /**
     * 查找最早版本
     *
     * <p>查找策略：
     * <ol>
     *   <li>读取 EARLIEST Hint 文件
     *   <li>验证：检查该版本文件是否存在
     *   <li>如果验证失败，回退到列举目录中的所有文件
     * </ol>
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @param prefix 文件前缀
     * @param file 版本号到文件路径的转换函数
     * @return 最早版本号，如果不存在返回 null
     * @throws IOException 如果 I/O 错误
     */
    @Nullable
    public static Long findEarliest(
            FileIO fileIO, Path dir, String prefix, Function<Long, Path> file) throws IOException {
        Long snapshotId = readHint(fileIO, EARLIEST, dir);
        // null and it is the earliest only it exists
        if (snapshotId != null && fileIO.exists(file.apply(snapshotId))) {
            return snapshotId;
        }

        return findByListFiles(fileIO, Math::min, dir, prefix);
    }

    /**
     * 读取 Hint 文件
     *
     * <p>重试机制：最多重试 3 次，每次间隔 1 毫秒。
     *
     * @param fileIO 文件 I/O
     * @param fileName Hint 文件名（LATEST 或 EARLIEST）
     * @param dir 目录路径
     * @return Hint 值（版本号），如果读取失败返回 null
     */
    public static Long readHint(FileIO fileIO, String fileName, Path dir) {
        Path path = new Path(dir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return fileIO.readOverwrittenFileUtf8(path).map(Long::parseLong).orElse(null);
            } catch (Exception ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    /**
     * 通过列举文件查找版本号
     *
     * @param fileIO 文件 I/O
     * @param reducer 归约函数（Math::max 用于查找最新，Math::min 用于查找最早）
     * @param dir 目录路径
     * @param prefix 文件前缀
     * @return 版本号，如果不存在返回 null
     * @throws IOException 如果 I/O 错误
     */
    public static Long findByListFiles(
            FileIO fileIO, BinaryOperator<Long> reducer, Path dir, String prefix)
            throws IOException {
        return listVersionedFiles(fileIO, dir, prefix).reduce(reducer).orElse(null);
    }

    /**
     * 提交最新 Hint
     *
     * @param fileIO 文件 I/O
     * @param id 版本号
     * @param dir 目录路径
     * @throws IOException 如果 I/O 错误
     */
    public static void commitLatestHint(FileIO fileIO, long id, Path dir) throws IOException {
        commitHint(fileIO, id, LATEST, dir);
    }

    /**
     * 提交最早 Hint
     *
     * @param fileIO 文件 I/O
     * @param id 版本号
     * @param dir 目录路径
     * @throws IOException 如果 I/O 错误
     */
    public static void commitEarliestHint(FileIO fileIO, long id, Path dir) throws IOException {
        commitHint(fileIO, id, EARLIEST, dir);
    }

    /**
     * 删除最新 Hint
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @throws IOException 如果 I/O 错误
     */
    public static void deleteLatestHint(FileIO fileIO, Path dir) throws IOException {
        Path hintFile = new Path(dir, LATEST);
        fileIO.delete(hintFile, false);
    }

    /**
     * 删除最早 Hint
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @throws IOException 如果 I/O 错误
     */
    public static void deleteEarliestHint(FileIO fileIO, Path dir) throws IOException {
        Path hintFile = new Path(dir, EARLIEST);
        fileIO.delete(hintFile, false);
    }

    /**
     * 提交 Hint 文件
     *
     * <p>重试机制：最多重试 3 次，每次随机间隔 500-1500 毫秒。
     *
     * @param fileIO 文件 I/O
     * @param id 版本号
     * @param fileName Hint 文件名（LATEST 或 EARLIEST）
     * @param dir 目录路径
     * @throws IOException 如果 I/O 错误且重试次数耗尽
     */
    public static void commitHint(FileIO fileIO, long id, String fileName, Path dir)
            throws IOException {
        Path hintFile = new Path(dir, fileName);
        int loopTime = 3;
        while (loopTime-- > 0) {
            try {
                fileIO.overwriteHintFile(hintFile, String.valueOf(id));
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1000) + 500);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    // throw root cause
                    throw new RuntimeException(e);
                }
                if (loopTime == 0) {
                    throw e;
                }
            }
        }
    }
}
