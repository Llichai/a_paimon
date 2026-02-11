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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;

/**
 * 扫描结束异常，表示扫描已结束（流式扫描场景）。
 *
 * <p>该异常用于标识流式扫描已到达终点，不再有新的数据可读。
 * 通常在以下场景抛出：
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>流式扫描结束</b>: 当流式扫描达到结束条件（如扫描到指定快照）时抛出</li>
 *   <li><b>有界流扫描</b>: 设置了结束快照的流式扫描，扫描到该快照后抛出</li>
 *   <li><b>正常结束</b>: 这是一个正常的结束信号，不是错误</li>
 * </ul>
 *
 * <h3>处理方式</h3>
 * <pre>{@code
 * try {
 *     while (true) {
 *         Plan plan = scan.plan();
 *         // 处理 plan...
 *     }
 * } catch (EndOfScanException e) {
 *     // 扫描正常结束
 *     logger.info("Scan completed");
 * }
 * }</pre>
 *
 * <h3>与 OutOfRangeException 的区别</h3>
 * <ul>
 *   <li><b>EndOfScanException</b>: 正常的扫描结束（达到结束条件）</li>
 *   <li><b>OutOfRangeException</b>: 异常的扫描中断（快照被删除）</li>
 * </ul>
 *
 * @see TableScan#plan() 可能抛出此异常
 * @since 0.4.0
 */
@Public
public class EndOfScanException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EndOfScanException() {
        super();
    }
}
