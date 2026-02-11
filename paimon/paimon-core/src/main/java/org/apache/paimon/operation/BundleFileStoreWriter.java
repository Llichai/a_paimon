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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BundleRecords;

/**
 * 捆绑文件存储写入器接口
 *
 * <p>该接口扩展了 {@link FileStoreWrite} 接口，支持批量捆绑写入功能。
 *
 * <h2>批量写入优化</h2>
 * <ul>
 *   <li><b>数据捆绑</b>：将多条记录打包成一个捆绑（Bundle）进行批量写入
 *   <li><b>减少I/O开销</b>：通过批量写入减少文件系统的I/O操作次数
 *   <li><b>提高吞吐量</b>：批量处理可以显著提高写入吞吐量
 * </ul>
 *
 * <h2>写入任务的排队和调度</h2>
 * <ul>
 *   <li><b>按分区-桶组织</b>：数据按照分区和桶进行组织写入
 *   <li><b>顺序保证</b>：同一分区-桶内的数据按照捆绑顺序写入
 *   <li><b>并行处理</b>：不同分区-桶的写入可以并行执行
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p>适用于以下场景：
 * <ul>
 *   <li>批量数据导入
 *   <li>大规模数据加载
 *   <li>需要高吞吐量的写入操作
 * </ul>
 *
 * @see FileStoreWrite 文件存储写入接口
 * @see BundleRecords 捆绑记录
 */
public interface BundleFileStoreWriter extends FileStoreWrite<InternalRow> {

    /**
     * 将批量数据按照分区和桶写入存储
     *
     * <p>该方法接收一个数据捆绑（Bundle），并将其中的数据写入到指定的分区和桶中。
     * 捆绑中的数据会被批量处理，从而提高写入效率。
     *
     * <h3>写入流程</h3>
     * <ol>
     *   <li>验证分区和桶的有效性
     *   <li>解包捆绑中的数据记录
     *   <li>批量写入到底层存储
     *   <li>更新写入统计信息
     * </ol>
     *
     * @param partition 数据所属的分区
     * @param bucket 数据所属的桶ID
     * @param bundle 要写入的数据捆绑，包含批量记录
     * @throws Exception 写入记录时抛出的异常，可能包括：
     *                   <ul>
     *                     <li>I/O异常
     *                     <li>分区或桶不存在异常
     *                     <li>数据格式错误异常
     *                   </ul>
     */
    void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception;
}
