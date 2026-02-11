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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * 文件读写工具类
 *
 * <p>FileUtils 提供了文件读取和写入的实用工具方法。
 *
 * <p>核心功能：
 * <ul>
 *   <li>版本文件列举：{@link #listVersionedFiles} - 列举目录中的版本文件
 *   <li>文件状态查询：{@link #listVersionedFileStatus} - 获取版本文件的状态
 *   <li>目录列举：{@link #listVersionedDirectories} - 列举版本目录
 *   <li>文件存在性检查：{@link #checkExists} - 检查文件是否存在
 *   <li>格式读取器创建：{@link #createFormatReader} - 创建格式读取器
 * </ul>
 *
 * <p>版本文件命名规则：
 * <ul>
 *   <li>文件名格式：prefix + version（如 "snapshot-1", "snapshot-2"）
 *   <li>版本号：Long 类型的数字
 *   <li>前缀：用于区分不同类型的文件（如 "snapshot-", "changelog-"）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>快照管理：列举快照文件
 *   <li>Changelog 管理：列举 Changelog 文件
 *   <li>文件扫描：查找特定前缀的版本文件
 *   <li>文件验证：检查文件是否存在
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * FileIO fileIO = ...;
 * Path snapshotDir = new Path("/path/to/snapshot");
 *
 * // 列举快照版本
 * Stream<Long> snapshots = FileUtils.listVersionedFiles(fileIO, snapshotDir, "snapshot-");
 * snapshots.forEach(version -> System.out.println("Snapshot: " + version));
 *
 * // 列举文件状态
 * Stream<FileStatus> fileStatuses = FileUtils.listVersionedFileStatus(fileIO, snapshotDir, "snapshot-");
 * fileStatuses.forEach(status -> System.out.println("File: " + status.getPath()));
 *
 * // 检查文件是否存在
 * Path snapshotFile = new Path("/path/to/snapshot/snapshot-1");
 * try {
 *     FileUtils.checkExists(fileIO, snapshotFile);
 *     System.out.println("File exists");
 * } catch (FileNotFoundException e) {
 *     System.err.println("File not found: " + e.getMessage());
 * }
 *
 * // 创建格式读取器
 * FormatReaderFactory format = ...;
 * Path dataFile = new Path("/path/to/data.parquet");
 * RecordReader<InternalRow> reader = FileUtils.createFormatReader(fileIO, format, dataFile, null);
 * }</pre>
 */
public class FileUtils {

    /**
     * 列举目录中的版本文件
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @param prefix 文件前缀
     * @return 版本号流
     * @throws IOException 如果 I/O 错误
     */
    public static Stream<Long> listVersionedFiles(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        return listOriginalVersionedFiles(fileIO, dir, prefix).map(Long::parseLong);
    }

    /**
     * 列举目录中的原始版本文件名（保留前缀后的完整字符串）
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @param prefix 文件前缀
     * @return 版本字符串流
     * @throws IOException 如果 I/O 错误
     */
    public static Stream<String> listOriginalVersionedFiles(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        return listVersionedFileStatus(fileIO, dir, prefix)
                .map(FileStatus::getPath)
                .map(Path::getName)
                .map(name -> name.substring(prefix.length()));
    }

    /**
     * 列举目录中的版本文件状态
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @param prefix 文件前缀
     * @return 文件状态流
     * @throws IOException 如果 I/O 错误
     */
    public static Stream<FileStatus> listVersionedFileStatus(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        if (!fileIO.exists(dir)) {
            return Stream.empty();
        }

        FileStatus[] statuses = fileIO.listStatus(dir);

        if (statuses == null) {
            throw new RuntimeException(
                    String.format(
                            "The return value is null of the listStatus for the '%s' directory.",
                            dir));
        }

        return Arrays.stream(statuses)
                .filter(status -> status.getPath().getName().startsWith(prefix));
    }

    /**
     * 列举目录中的版本子目录
     *
     * @param fileIO 文件 I/O
     * @param dir 目录路径
     * @param prefix 目录前缀
     * @return 文件状态流
     * @throws IOException 如果 I/O 错误
     */
    public static Stream<FileStatus> listVersionedDirectories(
            FileIO fileIO, Path dir, String prefix) throws IOException {
        if (!fileIO.exists(dir)) {
            return Stream.empty();
        }

        FileStatus[] statuses = fileIO.listDirectories(dir);

        if (statuses == null) {
            throw new RuntimeException(
                    String.format(
                            "The return value is null of the listStatus for the '%s' directory.",
                            dir));
        }

        return Arrays.stream(statuses)
                .filter(status -> status.getPath().getName().startsWith(prefix));
    }

    /**
     * 检查文件是否存在
     *
     * <p>如果文件不存在，抛出 FileNotFoundException，并提供可能的原因：
     * <ul>
     *   <li>快照过期太快：可配置 'snapshot.time-retained' 选项
     *   <li>消费太慢：可提高消费性能（如增加并行度）
     * </ul>
     *
     * @param fileIO 文件 I/O
     * @param file 文件路径
     * @throws IOException 如果文件不存在或 I/O 错误
     */
    public static void checkExists(FileIO fileIO, Path file) throws IOException {
        if (!fileIO.exists(file)) {
            throw new FileNotFoundException(
                    String.format(
                            "File '%s' not found, Possible causes: "
                                    + "1.snapshot expires too fast, you can configure 'snapshot.time-retained'"
                                    + " option with a larger value. "
                                    + "2.consumption is too slow, you can improve the performance of consumption"
                                    + " (For example, increasing parallelism).",
                            file));
        }
    }

    /**
     * 创建格式读取器
     *
     * @param fileIO 文件 I/O
     * @param format 格式读取器工厂
     * @param file 文件路径
     * @param fileSize 文件大小（可选，如果为 null 则自动获取）
     * @return 记录读取器
     * @throws IOException 如果 I/O 错误或文件不存在
     */
    public static RecordReader<InternalRow> createFormatReader(
            FileIO fileIO, FormatReaderFactory format, Path file, @Nullable Long fileSize)
            throws IOException {
        try {
            if (fileSize == null) {
                fileSize = fileIO.getFileSize(file);
            }
            return format.createReader(new FormatReaderContext(fileIO, file, fileSize));
        } catch (Exception e) {
            checkExists(fileIO, file);
            throw e;
        }
    }
}
