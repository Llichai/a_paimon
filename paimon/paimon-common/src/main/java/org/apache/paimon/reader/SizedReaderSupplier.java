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

package org.apache.paimon.reader;

/**
 * 带大小信息的记录读取器供应商。
 *
 * <p>该接口扩展了 {@link ReaderSupplier},增加了对数据大小的估算能力。
 *
 * <h2>核心功能</h2>
 *
 * <ul>
 *   <li>大小估算:提供读取器将要读取的数据量估计
 *   <li>延迟创建:继承 ReaderSupplier 的延迟初始化特性
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>任务调度:根据数据大小分配计算资源
 *   <li>负载均衡:按数据量均衡分布读取任务
 *   <li>进度跟踪:基于数据大小估算处理进度
 *   <li>资源规划:预先分配内存缓冲区
 * </ul>
 *
 * <h2>实现建议</h2>
 *
 * <p>估算大小可以基于:
 *
 * <ul>
 *   <li>文件大小:对于文件读取器,返回文件字节数
 *   <li>记录数量:预估的记录条数
 *   <li>压缩前大小:如果数据被压缩,返回原始大小
 * </ul>
 */
public interface SizedReaderSupplier<T> extends ReaderSupplier<T> {

    /**
     * 估算读取器将要读取的数据大小。
     *
     * <p>该方法应该快速返回,避免执行耗时的精确计算。
     *
     * @return 估算的数据大小(字节数),如果无法估算返回0
     */
    long estimateSize();
}
