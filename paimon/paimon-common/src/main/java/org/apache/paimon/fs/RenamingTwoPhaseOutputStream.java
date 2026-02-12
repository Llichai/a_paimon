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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.Public;

import java.io.IOException;
import java.util.UUID;

/**
 * 重命名式两阶段输出流 - HDFS风格的原子写入实现.
 *
 * <p>这是 {@link TwoPhaseOutputStream} 的一个实现,通过"写临时文件+重命名"的方式实现原子提交。
 * 这种模式是 HDFS 和其他分布式文件系统常用的原子写入策略。
 *
 * <h2>两阶段提交流程</h2>
 * <pre>
 * 阶段 1: 写入阶段
 * ┌──────────────────────────┐
 * │  应用程序写入数据          │
 * ├──────────────────────────┤
 * │  write() → 写到临时文件    │
 * │  flush() → 刷新临时文件    │
 * │  closeForCommit()        │
 * └──────────────────────────┘
 *          ↓
 * 临时文件: /path/_temporary/.tmp.{uuid}
 *
 * 阶段 2: 提交阶段
 * ┌──────────────────────────┐
 * │  Committer.commit()      │
 * ├──────────────────────────┤
 * │  1. 创建父目录(如需要)     │
 * │  2. 重命名临时文件到目标   │
 * │  3. 删除临时文件(如果存在) │
 * └──────────────────────────┘
 *          ↓
 * 目标文件: /path/data.file
 * </pre>
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>原子性</b>: 重命名操作在大多数文件系统上是原子的</li>
 *   <li><b>隔离性</b>: 临时文件在提交前不可见</li>
 *   <li><b>失败恢复</b>: 失败时临时文件可被清理,不影响目标文件</li>
 *   <li><b>HDFS 兼容</b>: 遵循 HDFS 的写入语义</li>
 * </ul>
 *
 * <h2>临时文件命名</h2>
 * <ul>
 *   <li>临时目录: {@code _temporary/} (在目标文件的同级目录)</li>
 *   <li>临时文件名: {@code .tmp.{UUID}} (随机UUID避免冲突)</li>
 *   <li>完整路径示例: {@code /data/table/_temporary/.tmp.abc-123-def}</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 基本用法
 * Path targetPath = new Path("/data/output.parquet");
 * FileIO fileIO = FileIO.get(targetPath);
 *
 * // 创建两阶段输出流
 * TwoPhaseOutputStream out = new RenamingTwoPhaseOutputStream(
 *     fileIO, targetPath, false);
 *
 * // 写入数据(写到临时文件)
 * out.write(data);
 * out.flush();
 *
 * // 关闭并获取提交器
 * TwoPhaseOutputStream.Committer committer = out.closeForCommit();
 *
 * // 原子提交(重命名临时文件到目标路径)
 * committer.commit(fileIO);
 *
 * // 2. 带异常处理的完整示例
 * Path targetPath = new Path("/data/table/part-0001.parquet");
 * FileIO fileIO = FileIO.get(targetPath);
 * TwoPhaseOutputStream out = null;
 * TwoPhaseOutputStream.Committer committer = null;
 *
 * try {
 *     out = new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
 *
 *     // 写入数据
 *     writeParquetData(out, records);
 *
 *     // 关闭并准备提交
 *     committer = out.closeForCommit();
 *
 *     // 原子提交
 *     committer.commit(fileIO);
 *
 * } catch (Exception e) {
 *     // 写入失败,丢弃数据
 *     if (committer != null) {
 *         committer.discard(fileIO);
 *     } else if (out != null) {
 *         out.close();
 *     }
 *     throw e;
 * }
 *
 * // 3. 覆盖现有文件
 * // overwrite = true 允许覆盖已存在的目标文件
 * TwoPhaseOutputStream out = new RenamingTwoPhaseOutputStream(
 *     fileIO, targetPath, true);
 *
 * out.write(newData);
 * TwoPhaseOutputStream.Committer committer = out.closeForCommit();
 * committer.commit(fileIO); // 如果目标文件存在,会被覆盖
 *
 * // 4. 在 try-with-resources 中使用(需要手动提交)
 * TwoPhaseOutputStream.Committer committer;
 * try (TwoPhaseOutputStream out =
 *         new RenamingTwoPhaseOutputStream(fileIO, targetPath, false)) {
 *     out.write(data);
 *     committer = out.closeForCommit();
 * }
 * // out.close() 已被调用,但未提交
 * committer.commit(fileIO); // 显式提交
 * }</pre>
 *
 * <h2>文件布局示例</h2>
 * <pre>
 * 写入过程中:
 * /data/table/
 * ├── existing-file-1.parquet
 * ├── existing-file-2.parquet
 * └── _temporary/
 *     └── .tmp.abc-123-def  ← 临时文件(正在写入)
 *
 * 提交后:
 * /data/table/
 * ├── existing-file-1.parquet
 * ├── existing-file-2.parquet
 * └── new-file.parquet       ← 临时文件已重命名
 * (_temporary 目录被清理)
 * </pre>
 *
 * <h2>提交器实现</h2>
 * TempFileCommitter 负责执行实际的提交或丢弃操作:
 * <ul>
 *   <li><b>commit()</b>: 创建父目录 → 重命名临时文件 → 清理临时文件</li>
 *   <li><b>discard()</b>: 删除目标文件和临时文件</li>
 *   <li><b>clean()</b>: 删除整个临时目录</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>原子性保证</b>: 依赖文件系统的重命名原子性,HDFS/S3 等支持</li>
 *   <li><b>跨文件系统</b>: 临时文件和目标文件必须在同一文件系统上</li>
 *   <li><b>权限</b>: 需要对父目录有写和执行权限</li>
 *   <li><b>磁盘空间</b>: 需要两倍于文件大小的磁盘空间(临时+目标)</li>
 *   <li><b>清理</b>: 异常退出可能留下临时文件,需要定期清理</li>
 * </ul>
 *
 * <h2>与其他实现的对比</h2>
 * <table border="1">
 *   <tr>
 *     <th>实现</th>
 *     <th>提交方式</th>
 *     <th>适用文件系统</th>
 *     <th>磁盘开销</th>
 *   </tr>
 *   <tr>
 *     <td>RenamingTwoPhaseOutputStream</td>
 *     <td>重命名</td>
 *     <td>HDFS, 本地FS</td>
 *     <td>2x 文件大小</td>
 *   </tr>
 *   <tr>
 *     <td>MultiPartUploadTwoPhaseOutputStream</td>
 *     <td>分段上传</td>
 *     <td>S3, OSS, COS</td>
 *     <td>1x 文件大小</td>
 *   </tr>
 * </table>
 *
 * @see TwoPhaseOutputStream
 * @see MultiPartUploadTwoPhaseOutputStream
 * @since 1.0
 */
@Public
public class RenamingTwoPhaseOutputStream extends TwoPhaseOutputStream {
    /** 临时文件所在的目录名称. */
    private static final String TEMP_DIR_NAME = "_temporary";

    /** 最终要写入的目标路径. */
    private final Path targetPath;

    /** 临时文件的路径. */
    private final Path tempPath;

    /** 写入临时文件的底层输出流. */
    private final PositionOutputStream tempOutputStream;

    /**
     * 构造重命名式两阶段输出流.
     *
     * @param fileIO 文件 I/O 实现,不能为 null
     * @param targetPath 最终目标文件路径,不能为 null
     * @param overwrite 如果目标文件已存在是否覆盖。false 时会抛出异常
     * @throws IOException 如果 overwrite=false 且目标文件已存在,或创建临时文件失败
     */
    public RenamingTwoPhaseOutputStream(FileIO fileIO, Path targetPath, boolean overwrite)
            throws IOException {
        if (!overwrite && fileIO.exists(targetPath)) {
            throw new IOException("File " + targetPath + " already exists.");
        }
        this.targetPath = targetPath;
        this.tempPath = generateTempPath(targetPath);

        // Create temporary file
        this.tempOutputStream = fileIO.newOutputStream(tempPath, overwrite);
    }

    /**
     * 写入单个字节到临时文件.
     *
     * @param b 要写入的字节(只使用低8位)
     * @throws IOException 如果写入失败
     */
    @Override
    public void write(int b) throws IOException {
        tempOutputStream.write(b);
    }

    /**
     * 写入整个字节数组到临时文件.
     *
     * @param b 要写入的字节数组,不能为 null
     * @throws IOException 如果写入失败
     */
    @Override
    public void write(byte[] b) throws IOException {
        tempOutputStream.write(b);
    }

    /**
     * 写入字节数组的一部分到临时文件.
     *
     * @param b 包含要写入数据的字节数组,不能为 null
     * @param off 数据在数组中的起始偏移量
     * @param len 要写入的字节数
     * @throws IOException 如果写入失败
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        tempOutputStream.write(b, off, len);
    }

    /**
     * 刷新临时文件,确保数据写入磁盘.
     *
     * @throws IOException 如果刷新失败
     */
    @Override
    public void flush() throws IOException {
        tempOutputStream.flush();
    }

    /**
     * 获取当前写入位置.
     *
     * @return 当前写入位置(字节偏移量)
     * @throws IOException 如果获取位置失败
     */
    @Override
    public long getPos() throws IOException {
        return tempOutputStream.getPos();
    }

    /**
     * 关闭临时文件.
     *
     * <p><b>注意</b>: 这只是关闭流,不会执行提交操作。
     * 要提交数据,应该调用 {@link #closeForCommit()} 并使用返回的 Committer。
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        tempOutputStream.close();
    }

    /**
     * 关闭流并返回提交器.
     *
     * <p>这是两阶段提交的第一阶段。关闭临时文件后,返回一个 {@link Committer}
     * 对象,调用者可以使用它来提交或丢弃数据。
     *
     * @return 可用于提交或丢弃数据的提交器
     * @throws IOException 如果关闭失败
     */
    @Override
    public Committer closeForCommit() throws IOException {
        close();
        return new TempFileCommitter(tempPath, targetPath);
    }

    /**
     * 生成临时文件路径.
     *
     * <p>临时文件位于目标文件同级的 {@code _temporary} 目录下,文件名为 {@code .tmp.{UUID}}。
     *
     * @param targetPath 目标文件路径
     * @return 临时文件路径
     */
    private Path generateTempPath(Path targetPath) {
        String tempFileName = TEMP_DIR_NAME + "/.tmp." + UUID.randomUUID();
        return new Path(targetPath.getParent(), tempFileName);
    }

    /**
     * 临时文件提交器实现.
     *
     * <p>负责将临时文件重命名为目标文件,或在失败时清理临时文件。
     */
    private static class TempFileCommitter implements Committer {

        private static final long serialVersionUID = 1L;

        /** 临时文件路径. */
        private final Path tempPath;

        /** 目标文件路径. */
        private final Path targetPath;

        /**
         * 构造提交器.
         *
         * @param tempPath 临时文件路径
         * @param targetPath 目标文件路径
         */
        private TempFileCommitter(Path tempPath, Path targetPath) {
            this.tempPath = tempPath;
            this.targetPath = targetPath;
        }

        /**
         * 提交数据 - 将临时文件重命名为目标文件.
         *
         * <p>提交流程:
         * <ol>
         *   <li>创建目标文件的父目录(如果不存在)</li>
         *   <li>将临时文件重命名为目标文件</li>
         *   <li>删除临时文件(如果重命名后仍存在)</li>
         * </ol>
         *
         * @param fileIO 文件 I/O 实现
         * @throws IOException 如果重命名失败
         */
        @Override
        public void commit(FileIO fileIO) throws IOException {
            Path parentDir = targetPath.getParent();
            if (parentDir != null && !fileIO.exists(parentDir)) {
                fileIO.mkdirs(parentDir);
            }
            if (!fileIO.rename(tempPath, targetPath)) {
                throw new IOException("Failed to rename " + tempPath + " to " + targetPath);
            }
            if (fileIO.exists(tempPath)) {
                fileIO.deleteQuietly(tempPath);
            }
        }

        /**
         * 丢弃数据 - 删除目标文件和临时文件.
         *
         * <p>用于回滚操作,清理所有相关文件。
         *
         * @param fileIO 文件 I/O 实现
         * @throws IOException 删除失败时抛出(使用 deleteQuietly,通常不会抛出)
         */
        @Override
        public void discard(FileIO fileIO) throws IOException {
            if (fileIO.exists(targetPath)) {
                fileIO.deleteQuietly(targetPath);
            }
            if (fileIO.exists(tempPath)) {
                fileIO.deleteQuietly(tempPath);
            }
        }

        /**
         * 获取目标文件路径.
         *
         * @return 目标文件路径
         */
        @Override
        public Path targetPath() {
            return targetPath;
        }

        /**
         * 清理临时目录.
         *
         * <p>删除整个 {@code _temporary} 目录及其内容。
         *
         * @param fileIO 文件 I/O 实现
         */
        @Override
        public void clean(FileIO fileIO) {
            fileIO.deleteDirectoryQuietly(tempPath.getParent());
        }
    }
}
