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

import java.io.IOException;
import java.io.Serializable;

/**
 * 两阶段输出流,提供了一种写入文件并获取提交器的方式。
 *
 * <p>两阶段输出流实现了事务性写入语义,将文件写入分为两个阶段:
 * <ol>
 *   <li><b>写入阶段:</b>数据被写入临时位置,此时对其他读取者不可见</li>
 *   <li><b>提交阶段:</b>调用 commit 后,数据变为可见(通常通过重命名或多部分上传完成)</li>
 * </ol>
 *
 * <p>这种机制确保了写入的原子性,避免了读取者看到部分写入的文件。
 * 这对于分布式文件系统和对象存储(如 S3、OSS)特别重要。
 *
 * <p><b>使用示例:</b>
 * <pre>{@code
 * try (TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(path, true)) {
 *     // 写入数据
 *     out.write(data);
 *
 *     // 关闭并获取提交器
 *     TwoPhaseOutputStream.Committer committer = out.closeForCommit();
 *
 *     // 提交使数据可见
 *     committer.commit(fileIO);
 * }
 * }</pre>
 */
public abstract class TwoPhaseOutputStream extends PositionOutputStream {
    /**
     * 关闭流以进行写入,并返回一个可用于使写入的数据可见的提交器。
     *
     * <p>调用此方法后,不应再使用流进行写入。返回的提交器可用于提交数据或丢弃数据。
     *
     * <p>在提交器调用 commit 之前,写入的数据对其他读取者是不可见的。
     * 这允许在确保数据完整性后再使其可见。
     *
     * @return 可用于提交数据的提交器
     * @throws IOException 如果关闭期间发生 I/O 错误
     */
    public abstract Committer closeForCommit() throws IOException;

    /**
     * 提交器接口,可以提交或丢弃写入的数据。
     *
     * <p>提交器提供了两种操作:
     * <ul>
     *   <li>{@link #commit(FileIO)}: 提交数据,使其永久可见</li>
     *   <li>{@link #discard(FileIO)}: 丢弃数据,清理临时文件</li>
     * </ul>
     *
     * <p>提交器是可序列化的,这允许在分布式环境中传递提交器,
     * 例如从 TaskManager 传递到 JobManager 进行最终提交。
     */
    public interface Committer extends Serializable {

        /**
         * 提交写入的数据,使其可见。
         *
         * <p>对于不同的文件系统,提交的实现方式不同:
         * <ul>
         *   <li><b>本地文件系统/HDFS:</b> 通常通过原子重命名操作实现</li>
         *   <li><b>S3/OSS:</b> 通常通过完成多部分上传实现</li>
         * </ul>
         *
         * <p>提交操作应该是幂等的,即多次调用应该是安全的。
         *
         * @param fileIO 文件 I/O 接口,用于执行文件操作
         * @throws IOException 如果提交期间发生 I/O 错误
         */
        void commit(FileIO fileIO) throws IOException;

        /**
         * 丢弃写入的数据,清理任何临时文件或资源。
         *
         * <p>当写入失败或决定不提交数据时,应调用此方法来清理临时资源。
         * 这可以防止临时文件的累积。
         *
         * @param fileIO 文件 I/O 接口,用于执行文件操作
         * @throws IOException 如果丢弃期间发生 I/O 错误
         */
        void discard(FileIO fileIO) throws IOException;

        /**
         * 获取目标文件路径。
         *
         * @return 数据将被提交到的目标路径
         */
        Path targetPath();

        /**
         * 清理与此提交器关联的所有资源。
         *
         * <p>与 {@link #discard(FileIO)} 不同,此方法用于清理已提交或已丢弃后的资源,
         * 例如清理多部分上传的元数据存储。
         *
         * @param fileIO 文件 I/O 接口,用于执行文件操作
         * @throws IOException 如果清理期间发生 I/O 错误
         */
        void clean(FileIO fileIO) throws IOException;
    }
}
