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

package org.apache.paimon.operation.metrics;

import org.slf4j.Logger;

/**
 * 指标工具类
 *
 * <p>提供指标报告相关的实用方法。
 *
 * <h2>安全执行机制</h2>
 * <p>该类的主要功能是提供指标报告的安全执行机制：
 * <ul>
 *   <li><b>异常隔离</b>：捕获指标报告过程中的所有异常
 *   <li><b>日志记录</b>：记录异常信息用于调试
 *   <li><b>不中断主流程</b>：确保指标报告失败不影响业务逻辑
 * </ul>
 *
 * <h2>设计原则</h2>
 * <p>指标报告应该是非侵入式的：
 * <ul>
 *   <li>指标收集失败不应导致业务失败
 *   <li>异常被捕获并记录，不向上传播
 *   <li>保证系统的健壮性
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p>在以下场景中使用该工具类：
 * <ul>
 *   <li>更新指标计数器
 *   <li>报告指标测量值
 *   <li>触发指标事件
 *   <li>任何可能失败但不应影响主流程的指标操作
 * </ul>
 *
 * @see CommitMetrics 提交指标
 * @see CompactionMetrics 压缩指标
 * @see ScanMetrics 扫描指标
 */
public class MetricUtils {

    /**
     * 安全地执行指标报告操作
     *
     * <p>该方法会捕获执行过程中的所有异常，并记录警告日志，
     * 确保指标报告失败不会影响主业务流程。
     *
     * <h3>错误处理</h3>
     * <ul>
     *   <li>捕获所有 {@link Throwable}，包括 {@link Error}
     *   <li>记录警告级别的日志
     *   <li>不重新抛出异常
     * </ul>
     *
     * @param runnable 要执行的指标报告操作
     * @param logger 用于记录异常的日志器
     */
    public static void safeCall(Runnable runnable, Logger logger) {
        try {
            runnable.run();
        } catch (Throwable t) {
            logger.warn("Exception occurs when reporting metrics", t);
        }
    }
}
