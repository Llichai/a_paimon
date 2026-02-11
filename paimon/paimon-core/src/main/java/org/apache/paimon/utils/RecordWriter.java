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

import org.apache.paimon.io.DataFileMeta;

import java.util.Collection;
import java.util.List;

/**
 * 记录写入器接口
 *
 * <p>RecordWriter 负责写入数据并处理正在进行中的文件（用于写入尚未暂存的数据）。
 * 通过 {@link #prepareCommit(boolean)} 方法返回准备提交的增量文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>写入记录：{@link #write(Object)} - 写入单条记录
 *   <li>触发压缩：{@link #compact(boolean)} - 触发文件压缩
 *   <li>准备提交：{@link #prepareCommit(boolean)} - 准备提交的增量文件
 *   <li>同步状态：{@link #sync()} - 同步写入器状态
 *   <li>文件管理：{@link #dataFiles()} - 获取所有数据文件
 * </ul>
 *
 * <p>写入流程：
 * <pre>
 * 1. write(record) - 写入记录到内存缓冲区
 * 2. compact() - 触发压缩（可选）
 * 3. prepareCommit() - 刷新缓冲区，生成增量文件
 * 4. 外部提交 - 将增量文件持久化到 manifest
 * 5. close() - 关闭写入器，清理未提交文件
 * </pre>
 *
 * <p>压缩机制：
 * <ul>
 *   <li>正常压缩：合并部分文件，减少文件数量
 *   <li>全量压缩：合并所有文件为一个文件
 *   <li>异步压缩：压缩任务提交后可能尚未完成
 *   <li>等待压缩：prepareCommit(true) 会等待压缩完成
 * </ul>
 *
 * <p>增量文件：
 * <ul>
 *   <li>新文件：当前快照周期新生成的文件
 *   <li>删除文件：被压缩或覆盖的旧文件
 *   <li>Changelog 文件：变更日志文件（可选）
 * </ul>
 *
 * <p>序列号管理：
 * <ul>
 *   <li>{@link #maxSequenceNumber()} - 返回写入器写入的最大序列号
 *   <li>用于快照的序列号跟踪和顺序保证
 * </ul>
 *
 * <p>线程安全：
 * <ul>
 *   <li>写入器内部有异步线程（如压缩线程）
 *   <li>读取数据前必须调用 {@link #sync()} 同步状态
 *   <li>文件读写相关结构是线程不安全的
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * RecordWriter<InternalRow> writer = ...;
 *
 * // 写入记录
 * for (InternalRow record : records) {
 *     writer.write(record);
 * }
 *
 * // 触发压缩（可选）
 * writer.compact(false);  // 正常压缩
 *
 * // 准备提交
 * CommitIncrement increment = writer.prepareCommit(true);  // 等待压缩完成
 *
 * // 提交增量文件
 * commit(increment);
 *
 * // 检查压缩状态
 * if (writer.compactNotCompleted()) {
 *     // 压缩尚未完成
 * }
 *
 * // 同步状态（在读取前）
 * writer.sync();
 *
 * // 获取所有文件
 * Collection<DataFileMeta> files = writer.dataFiles();
 *
 * // 关闭写入器
 * writer.close();
 * }</pre>
 *
 * @param <T> 记录类型
 * @see CommitIncrement
 * @see DataFileMeta
 */
public interface RecordWriter<T> {

    /**
     * 写入一条记录
     *
     * @param record 要写入的记录
     * @throws Exception 如果写入失败
     */
    void write(T record) throws Exception;

    /**
     * 触发文件压缩
     *
     * <p>注意：压缩过程只是提交，方法返回时可能尚未完成。
     *
     * @param fullCompaction true=触发全量压缩，false=正常压缩
     * @throws Exception 如果压缩提交失败
     */
    void compact(boolean fullCompaction) throws Exception;

    /**
     * 添加文件到内部的 {@link org.apache.paimon.compact.CompactManager}
     *
     * @param files 要添加的文件列表
     */
    void addNewFiles(List<DataFileMeta> files);

    /**
     * 获取此写入器维护的所有数据文件
     *
     * @return 数据文件集合
     */
    Collection<DataFileMeta> dataFiles();

    /**
     * 获取此写入器写入记录的最大序列号
     *
     * @return 最大序列号
     */
    long maxSequenceNumber();

    /**
     * 准备提交
     *
     * @param waitCompaction 是否需要等待当前压缩完成
     * @return 当前快照周期的增量文件
     * @throws Exception 如果准备提交失败
     */
    CommitIncrement prepareCommit(boolean waitCompaction) throws Exception;

    /**
     * 检查是否有未完成的压缩
     *
     * <p>包括：正在进行的压缩、待获取的压缩结果、或稍后应触发的压缩。
     *
     * @return true 如果有未完成的压缩
     */
    boolean compactNotCompleted();

    /**
     * 同步写入器状态
     *
     * <p>文件读写相关结构是线程不安全的，写入器内部有异步线程，
     * 在读取数据前应该同步状态。
     *
     * @throws Exception 如果同步失败
     */
    void sync() throws Exception;

    /**
     * 关闭写入器
     *
     * <p>此调用会删除新生成但未提交的文件。
     *
     * @throws Exception 如果关闭失败
     */
    void close() throws Exception;
}
