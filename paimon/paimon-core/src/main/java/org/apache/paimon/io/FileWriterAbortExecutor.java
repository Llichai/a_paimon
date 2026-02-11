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

package org.apache.paimon.io;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

/**
 * 文件写入器中止执行器,用于在写入失败时清理文件。
 *
 * <p>设计目的:
 * <ul>
 *   <li>避免持有整个写入器对象的引用
 *   <li>只保存必要的文件路径信息
 *   <li>减少内存占用(特别是滚动写入器有多个已关闭的写入器时)
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>滚动写入器关闭当前写入器后,保存中止执行器
 *   <li>如果后续写入失败,调用所有中止执行器清理文件
 *   <li>提交成功后,不需要调用中止
 * </ul>
 *
 * <p>示例用法:
 * <pre>{@code
 * // 在滚动写入器中
 * List<FileWriterAbortExecutor> abortExecutors = new ArrayList<>();
 *
 * // 写入过程
 * currentWriter.close();
 * abortExecutors.add(currentWriter.abortExecutor());  // 只保存路径引用
 *
 * // 如果出错,清理所有文件
 * for (FileWriterAbortExecutor executor : abortExecutors) {
 *     executor.abort();  // 删除文件
 * }
 * }</pre>
 *
 * @see RollingFileWriterImpl 滚动写入器实现
 */
public class FileWriterAbortExecutor {
    private final FileIO fileIO;
    private final Path path;

    public FileWriterAbortExecutor(FileIO fileIO, Path path) {
        this.fileIO = fileIO;
        this.path = path;
    }

    /**
     * 中止写入,删除已写入的文件。
     *
     * <p>使用静默删除(deleteQuietly),即使删除失败也不会抛出异常。
     */
    public void abort() {
        fileIO.deleteQuietly(path);
    }
}
