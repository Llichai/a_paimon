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

import org.apache.paimon.operation.WriteRestore;

/**
 * 内部表写入接口，包含覆写设置功能。
 *
 * <p>这是一个内部接口，同时继承了 {@link StreamTableWrite} 和 {@link BatchTableWrite}，
 * 提供了额外的内部控制方法。
 *
 * <p>与公开接口的区别：
 * <ul>
 *     <li>{@link TableWrite}: 公开的写入接口，提供基础的写入功能
 *     <li>{@link StreamTableWrite}: 流式写入接口，添加了 prepareCommit(commitIdentifier)
 *     <li>{@link BatchTableWrite}: 批量写入接口，添加了 prepareCommit()
 *     <li>{@link InnerTableWrite}: 内部写入接口，添加了状态恢复和文件忽略功能
 * </ul>
 *
 * <p>内部方法用于：
 * <ul>
 *     <li>从检查点恢复写入状态
 *     <li>控制是否忽略之前的文件（用于覆写场景）
 *     <li>支持内部的写入优化
 * </ul>
 */
public interface InnerTableWrite extends StreamTableWrite, BatchTableWrite {

    /**
     * 设置写入恢复状态。
     *
     * <p>用于从检查点或保存点恢复写入状态，包括：
     * <ul>
     *     <li>恢复未完成的文件写入
     *     <li>恢复写入缓冲区状态
     *     <li>恢复分桶分配信息
     * </ul>
     *
     * @param writeRestore 写入恢复状态
     * @return 当前 InnerTableWrite 实例
     */
    InnerTableWrite withWriteRestore(WriteRestore writeRestore);

    /**
     * 设置是否忽略之前的文件。
     *
     * <p>在覆写场景下使用，用于控制是否忽略表中现有的文件：
     * <ul>
     *     <li>true: 忽略现有文件，适用于 INSERT OVERWRITE
     *     <li>false: 不忽略现有文件，适用于 INSERT INTO
     * </ul>
     *
     * @param ignorePreviousFiles 是否忽略之前的文件
     * @return 当前 InnerTableWrite 实例
     */
    InnerTableWrite withIgnorePreviousFiles(boolean ignorePreviousFiles);
}
