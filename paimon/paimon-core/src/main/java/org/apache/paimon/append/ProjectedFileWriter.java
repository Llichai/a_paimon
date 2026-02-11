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

package org.apache.paimon.append;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.FileWriter;
import org.apache.paimon.utils.ProjectedRow;

import java.io.IOException;

/**
 * 投影文件写入器
 *
 * <p>ProjectedFileWriter 是一个委托({@link FileWriter})包装器,
 * 在写入前对每一行应用字段投影,只写入指定的字段子集。
 *
 * <p>使用场景:
 * 当物理文件 Schema 是逻辑写入 Schema 的子集时:
 * <ul>
 *   <li><b>BLOB 字段分离</b>:将 BLOB 字段写入独立文件,不包含普通字段
 *   <li><b>列裁剪</b>:只写入需要的列,减少存储空间
 *   <li><b>Schema 演化</b>:写入旧版本 Schema,兼容性处理
 * </ul>
 *
 * <p>工作原理:
 * <pre>
 * 逻辑Schema: id INT, name STRING, age INT, image BLOB
 * 投影索引:   [0, 3]  (只写 id 和 image)
 *
 * 写入流程:
 * 1. write(row) 接收完整行:
 *    row = [1, "Alice", 25, <blob_data>]
 *
 * 2. ProjectedRow 应用投影:
 *    projectedRow = [1, <blob_data>]
 *
 * 3. 底层 writer 写入投影后的行:
 *    writer.write(projectedRow)
 * </pre>
 *
 * <p>零拷贝优化:
 * 使用 {@link ProjectedRow} 避免对象分配:
 * <ul>
 *   <li>不创建新的 InternalRow 对象
 *   <li>通过索引映射直接访问原始行的字段
 *   <li>减少 GC 压力,提高性能
 * </ul>
 *
 * <p>委托模式:
 * 实现所有 {@link FileWriter} 方法,透明地转发调用:
 * <ul>
 *   <li>{@code write(record)}: 投影后写入
 *   <li>{@code recordCount()}: 返回底层写入器的记录数
 *   <li>{@code abort()}: 中止底层写入器
 *   <li>{@code result()}: 返回底层写入器的结果
 *   <li>{@code close()}: 关闭底层写入器
 * </ul>
 *
 * <p>类型参数:
 * <ul>
 *   <li>{@code <T extends FileWriter<InternalRow, R>>}: 底层写入器类型
 *   <li>{@code <R>}: 写入结果类型(如 {@link DataFileMeta})
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 假设表有字段: id INT, name STRING, image BLOB
 * RowType fullSchema = new RowType(
 *     new DataField(0, "id", new IntType()),
 *     new DataField(1, "name", new VarCharType()),
 *     new DataField(2, "image", new BlobType())
 * );
 *
 * // 只写入 id 和 image
 * int[] projection = {0, 2};
 *
 * // 创建底层写入器(只写 id 和 image)
 * RowDataFileWriter baseWriter = new RowDataFileWriter(...);
 *
 * // 包装为投影写入器
 * ProjectedFileWriter<RowDataFileWriter, DataFileMeta> writer =
 *     new ProjectedFileWriter<>(baseWriter, projection);
 *
 * // 写入完整行,自动投影
 * InternalRow row = GenericRow.of(1, "Alice", blobData);
 * writer.write(row); // 实际写入: [1, blobData]
 *
 * writer.close();
 * DataFileMeta result = writer.result();
 * }</pre>
 *
 * @see ProjectedRow 投影行包装器
 * @see MultipleBlobFileWriter BLOB 字段写入器
 * @see RollingBlobFileWriter 滚动 BLOB 文件写入器
 */
public class ProjectedFileWriter<T extends FileWriter<InternalRow, R>, R>
        implements FileWriter<InternalRow, R> {

    private final T writer;
    private final ProjectedRow projectedRow;

    public ProjectedFileWriter(T writer, int[] projection) {
        this.writer = writer;
        this.projectedRow = ProjectedRow.from(projection);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        projectedRow.replaceRow(record);
        writer.write(projectedRow);
    }

    @Override
    public long recordCount() {
        return writer.recordCount();
    }

    @Override
    public void abort() {
        writer.abort();
    }

    @Override
    public R result() throws IOException {
        return writer.result();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    public T writer() {
        return writer;
    }
}
