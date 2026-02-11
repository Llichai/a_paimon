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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.types.RowType;

import java.util.List;

/**
 * 批量处理的 {@link TableWrite} 接口。建议用于一次性提交场景。
 *
 * <p>批量写入的特点：
 * <ul>
 *     <li><b>一次性提交</b>：所有写入完成后调用一次 {@link #prepareCommit()}
 *     <li><b>简单的标识符</b>：使用固定的提交标识符（{@link BatchWriteBuilder#COMMIT_IDENTIFIER}）
 *     <li><b>无状态恢复</b>：不支持从检查点恢复，适用于短期批处理任务
 *     <li><b>等待压缩</b>：默认等待所有压缩任务完成后再提交
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 1. 创建批量写入
 * BatchTableWrite write = builder.newWrite();
 *
 * // 2. 写入数据
 * write.write(row1);
 * write.write(row2);
 * write.write(row3);
 *
 * // 3. 准备提交（只调用一次）
 * List<CommitMessage> messages = write.prepareCommit();
 *
 * // 4. 提交
 * BatchTableCommit commit = builder.newCommit();
 * commit.commit(messages);
 *
 * // 5. 关闭
 * write.close();
 * commit.close();
 * }</pre>
 *
 * <p>与流式写入的区别：
 * <pre>
 * 特性              | 批量写入                | 流式写入
 * ------------------|------------------------|------------------------
 * 提交次数          | 一次                   | 多次
 * 提交标识符        | 固定                   | 递增
 * 等待压缩          | 是（确保数据完整）     | 可选（性能考虑）
 * 状态恢复          | 不支持                 | 支持
 * 使用场景          | ETL、批量导入          | 实时数据接入
 * </pre>
 *
 * @since 0.4.0
 */
@Public
public interface BatchTableWrite extends TableWrite {

    @Override
    BatchTableWrite withIOManager(IOManager ioManager);

    /**
     * 为 {@link TableCommit} 准备提交。收集本次写入的增量文件。
     *
     * <p>此方法会：
     * <ol>
     *     <li>等待所有后台压缩任务完成
     *     <li>关闭所有写入器
     *     <li>收集所有新增和删除的文件
     *     <li>生成 CommitMessage 列表
     * </ol>
     *
     * <p>注意：此方法只能调用一次，多次调用会抛出异常。
     *
     * @return 提交消息列表，包含所有文件变更信息
     * @throws Exception 准备提交过程中的异常
     * @see BatchTableCommit#commit
     */
    List<CommitMessage> prepareCommit() throws Exception;

    /**
     * 指定写入的行类型。
     *
     * <p>当前仅适用于无主键且启用了 row tracking 的表。
     *
     * <p>使用场景：
     * <ul>
     *     <li>部分列更新
     *     <li>模式演化时的类型转换
     *     <li>投影写入（只写入部分列）
     * </ul>
     *
     * @param writeType 写入的行类型
     * @return 当前 BatchTableWrite 实例
     */
    BatchTableWrite withWriteType(RowType writeType);
}
