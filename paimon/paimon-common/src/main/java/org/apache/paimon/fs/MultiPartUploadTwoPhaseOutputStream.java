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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 多部分上传两阶段输出流 - 基于分段上传的原子提交实现.
 *
 * <p>这是 {@link TwoPhaseOutputStream} 的一个实现,使用对象存储的多部分上传(Multipart Upload)
 * 功能实现两阶段提交。适用于 S3、OSS、COS 等支持分段上传的对象存储系统。
 *
 * <h2>多部分上传流程</h2>
 * <pre>
 * 第1阶段: 写入和上传
 * ┌──────────────────────────────────────┐
 * │ write(data)                          │
 * ├──────────────────────────────────────┤
 * │ 1. 数据写入内存缓冲区                  │
 * │ 2. 缓冲区满(>=10MB) → 上传为一个分片  │
 * │ 3. 重复直到所有数据写入                │
 * │ closeForCommit() → 上传最后一个分片   │
 * └──────────────────────────────────────┘
 *
 * 对象存储状态: 多个已上传的分片(未完成)
 * - Part 1: ETag-1
 * - Part 2: ETag-2
 * - Part 3: ETag-3
 *
 * 第2阶段: 完成或中止
 * ┌──────────────────────────────────────┐
 * │ Committer.commit()                   │
 * ├──────────────────────────────────────┤
 * │ completeMultipartUpload()            │
 * │ → 所有分片合并为最终对象              │
 * └──────────────────────────────────────┘
 *
 * 或
 *
 * ┌──────────────────────────────────────┐
 * │ Committer.discard()                  │
 * ├──────────────────────────────────────┤
 * │ abortMultipartUpload()               │
 * │ → 删除所有已上传的分片                │
 * └──────────────────────────────────────┘
 * </pre>
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>分片上传</b>: 数据按 10MB 分片逐步上传,支持大文件</li>
 *   <li><b>内存高效</b>: 每次只缓冲一个分片,避免大量内存占用</li>
 *   <li><b>原子提交</b>: 通过 completeMultipartUpload 原子地合并分片</li>
 *   <li><b>可恢复</b>: 上传失败可中止,已上传分片自动清理</li>
 *   <li><b>网络优化</b>: 支持断点续传和并发上传(子类实现)</li>
 * </ul>
 *
 * <h2>分片大小限制</h2>
 * 不同对象存储有不同的限制:
 * <table border="1">
 *   <tr>
 *     <th>存储系统</th>
 *     <th>单片大小范围</th>
 *     <th>最大分片数</th>
 *     <th>推荐大小</th>
 *   </tr>
 *   <tr>
 *     <td>AWS S3</td>
 *     <td>5 MiB ~ 5 GiB</td>
 *     <td>10,000</td>
 *     <td>10 MiB</td>
 *   </tr>
 *   <tr>
 *     <td>阿里云 OSS</td>
 *     <td>100 KiB ~ 5 GiB</td>
 *     <td>10,000</td>
 *     <td>10 MiB</td>
 *   </tr>
 *   <tr>
 *     <td>腾讯云 COS</td>
 *     <td>1 MiB ~ 5 GiB</td>
 *     <td>10,000</td>
 *     <td>10 MiB</td>
 *   </tr>
 * </table>
 *
 * <p>默认分片大小为 10 MiB,参考了 Flink 的配置,在内存占用和上传效率之间取得平衡。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 基本用法(子类实现)
 * class S3TwoPhaseOutputStream extends MultiPartUploadTwoPhaseOutputStream<PartETag, CompleteMultipartUploadResult> {
 *     public S3TwoPhaseOutputStream(S3MultiPartUploadStore store, Path path) throws IOException {
 *         super(store, new org.apache.hadoop.fs.Path(path.toString()), path);
 *     }
 *
 *     @Override
 *     public Committer committer() {
 *         return new S3Committer(uploadId, uploadedParts, objectName, position, targetPath);
 *     }
 * }
 *
 * // 使用两阶段输出流
 * Path targetPath = new Path("s3://bucket/data/file.parquet");
 * S3MultiPartUploadStore store = new S3MultiPartUploadStore(...);
 *
 * TwoPhaseOutputStream out = new S3TwoPhaseOutputStream(store, targetPath);
 *
 * // 写入大量数据(自动分片上传)
 * for (byte[] chunk : dataChunks) {
 *     out.write(chunk); // 超过10MB时自动上传一个分片
 * }
 *
 * // 关闭并准备提交
 * TwoPhaseOutputStream.Committer committer = out.closeForCommit();
 *
 * // 原子提交(合并所有分片)
 * committer.commit(fileIO);
 *
 * // 2. 异常处理
 * MultiPartUploadTwoPhaseOutputStream out = ...;
 * Committer committer = null;
 *
 * try {
 *     writeData(out);
 *     committer = out.closeForCommit();
 *     committer.commit(fileIO);
 * } catch (Exception e) {
 *     if (committer != null) {
 *         committer.discard(fileIO); // 中止上传,删除所有分片
 *     }
 *     throw e;
 * }
 * }</pre>
 *
 * <h2>工作流程详解</h2>
 * <pre>
 * 1. 构造阶段:
 *    startMultiPartUpload() → 获取 uploadId
 *
 * 2. 写入阶段:
 *    write(data) → buffer
 *    buffer 满 → 写临时文件 → uploadPart() → 清空 buffer
 *
 * 3. 关闭阶段:
 *    closeForCommit() → 上传最后一个分片 → 返回 Committer
 *
 * 4. 提交阶段:
 *    commit() → completeMultipartUpload(uploadId, parts)
 *    或
 *    discard() → abortMultipartUpload(uploadId)
 * </pre>
 *
 * <h2>内存管理</h2>
 * <ul>
 *   <li>使用 {@link ByteArrayOutputStream} 作为内存缓冲区</li>
 *   <li>缓冲区大小动态增长,最大为 partSizeThreshold (10MB)</li>
 *   <li>每上传一个分片后重置缓冲区,释放内存</li>
 *   <li>使用临时文件传输数据,避免大内存分配</li>
 * </ul>
 *
 * <h2>临时文件处理</h2>
 * <pre>
 * 上传流程:
 * 内存缓冲区 → 临时文件 → uploadPart() → 删除临时文件
 *
 * 临时文件命名: multi-part-{UUID}.tmp
 * 位置: 系统临时目录
 * 生命周期: uploadPart 完成后立即删除
 * </pre>
 *
 * <h2>类型参数</h2>
 * <ul>
 *   <li><b>T</b>: 分片标识类型(如 S3 的 PartETag, OSS 的 PartETag)</li>
 *   <li><b>C</b>: 完成上传的结果类型(如 CompleteMultipartUploadResult)</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>网络依赖</b>: 需要稳定的网络连接,上传失败会抛出异常</li>
 *   <li><b>临时文件清理</b>: 确保在 finally 块中删除临时文件</li>
 *   <li><b>分片限制</b>: 单个文件最多 10,000 个分片,即最大约 50TB</li>
 *   <li><b>费用</b>: 未完成的多部分上传会产生存储费用,需要定期清理</li>
 * </ul>
 *
 * <h2>与 RenamingTwoPhaseOutputStream 的对比</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>MultiPartUpload</th>
 *     <th>Renaming</th>
 *   </tr>
 *   <tr>
 *     <td>适用系统</td>
 *     <td>S3, OSS, COS</td>
 *     <td>HDFS, 本地FS</td>
 *   </tr>
 *   <tr>
 *     <td>磁盘开销</td>
 *     <td>1x (直接上传)</td>
 *     <td>2x (临时+目标)</td>
 *   </tr>
 *   <tr>
 *     <td>内存开销</td>
 *     <td>10 MB (分片大小)</td>
 *     <td>取决于缓冲</td>
 *   </tr>
 *   <tr>
 *     <td>大文件支持</td>
 *     <td>优秀(分片上传)</td>
 *     <td>一般(整体重命名)</td>
 *   </tr>
 *   <tr>
 *     <td>网络优化</td>
 *     <td>支持断点续传</td>
 *     <td>不适用</td>
 *   </tr>
 * </table>
 *
 * @param <T> 分片标识类型,用于记录每个上传分片的元数据
 * @param <C> 完成上传的结果类型
 * @see TwoPhaseOutputStream
 * @see RenamingTwoPhaseOutputStream
 * @see MultiPartUploadStore
 * @since 1.0
 */
public abstract class MultiPartUploadTwoPhaseOutputStream<T, C> extends TwoPhaseOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiPartUploadTwoPhaseOutputStream.class);

    private final ByteArrayOutputStream buffer;
    private final MultiPartUploadStore<T, C> multiPartUploadStore;

    protected final String objectName;
    protected final Path targetPath;
    protected final String uploadId;

    protected List<T> uploadedParts;
    protected long position;

    private boolean closed = false;
    private Committer committer;

    public MultiPartUploadTwoPhaseOutputStream(
            MultiPartUploadStore<T, C> multiPartUploadStore,
            org.apache.hadoop.fs.Path hadoopPath,
            Path path)
            throws IOException {
        this.multiPartUploadStore = multiPartUploadStore;
        this.buffer = new ByteArrayOutputStream();
        this.uploadedParts = new ArrayList<>();
        this.objectName = multiPartUploadStore.pathToObject(hadoopPath);
        this.targetPath = path;
        this.uploadId = multiPartUploadStore.startMultiPartUpload(objectName);
        this.position = 0;
    }

    // OSS limit:  100KB ~ 5GB
    // S3 limit:  5MiB ~ 5GiB
    // Considering memory usage, and referencing Flink's setting of 10MiB.
    public int partSizeThreshold() {
        return 10 << 20;
    }

    public abstract Committer committer();

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        buffer.write(b);
        position++;
        uploadPartIfLargerThanThreshold();
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        int remaining = len;
        int offset = off;
        while (remaining > 0) {
            uploadPartIfLargerThanThreshold();
            int currentSize = buffer.size();
            int space = partSizeThreshold() - currentSize;
            int count = Math.min(remaining, space);
            buffer.write(b, offset, count);
            offset += count;
            remaining -= count;
            position += count;
            uploadPartIfLargerThanThreshold();
        }
    }

    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        uploadPartIfLargerThanThreshold();
    }

    @Override
    public void close() throws IOException {
        if (!closed && this.committer == null) {
            this.committer = closeForCommit();
        }
    }

    @Override
    public Committer closeForCommit() throws IOException {
        if (closed && this.committer != null) {
            return this.committer;
        } else if (closed) {
            throw new IOException("Stream is already closed but committer is null");
        }
        closed = true;
        // Only last upload part can be smaller than part size threshold
        uploadPartUtil();
        return committer();
    }

    private void uploadPartIfLargerThanThreshold() throws IOException {
        if (buffer.size() >= partSizeThreshold()) {
            uploadPartUtil();
        }
    }

    private void uploadPartUtil() throws IOException {
        if (buffer.size() == 0) {
            return;
        }

        File tempFile = null;
        int partNumber = uploadedParts.size() + 1;
        try {
            tempFile = Files.createTempFile("multi-part-" + UUID.randomUUID(), ".tmp").toFile();
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                buffer.writeTo(fos);
                fos.flush();
            }
            T partETag =
                    multiPartUploadStore.uploadPart(
                            objectName,
                            uploadId,
                            partNumber,
                            tempFile,
                            checkedDownCast(tempFile.length()));
            uploadedParts.add(partETag);
            buffer.reset();
        } catch (Exception e) {
            throw new IOException(
                    "Failed to upload part " + partNumber + " for upload ID: " + uploadId, e);
        } finally {
            if (tempFile != null && tempFile.exists()) {
                if (!tempFile.delete()) {
                    LOG.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
                }
            }
        }
    }

    private static int checkedDownCast(long value) {
        int downCast = (int) value;
        if (downCast != value) {
            throw new IllegalArgumentException(
                    "Cannot downcast long value " + value + " to integer.");
        }
        return downCast;
    }
}
