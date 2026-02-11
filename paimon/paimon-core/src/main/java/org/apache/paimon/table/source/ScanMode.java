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

/**
 * 扫描模式枚举，定义扫描快照的哪个部分。
 *
 * <p>不同的扫描模式决定了读取快照的不同视角：
 *
 * <h3>三种扫描模式</h3>
 * <ul>
 *   <li><b>ALL</b>: 扫描快照的完整数据文件（全量扫描）
 *       <ul>
 *         <li>返回快照的所有数据（合并后的最终状态）</li>
 *         <li>适用于批量查询、全表扫描</li>
 *       </ul>
 *   </li>
 *   <li><b>DELTA</b>: 只扫描快照新增/变化的文件（增量扫描）
 *       <ul>
 *         <li>返回当前快照相对于上一个快照的变化</li>
 *         <li>适用于增量读取、CDC 场景</li>
 *       </ul>
 *   </li>
 *   <li><b>CHANGELOG</b>: 只扫描快照的 Changelog 文件（变更日志扫描）
 *       <ul>
 *         <li>返回 Changelog 文件记录的变更（INSERT、UPDATE、DELETE）</li>
 *         <li>适用于需要完整变更历史的场景</li>
 *         <li>需要启用 'changelog-producer' 配置</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>使用场景对比</h3>
 * <pre>
 * 模式         | 返回内容           | 使用场景
 * -----------|-------------------|------------------
 * ALL        | 快照的完整数据      | 批量查询、全表扫描
 * DELTA      | 快照的变化数据      | 增量读取、CDC
 * CHANGELOG  | Changelog 文件     | 完整变更历史
 * </pre>
 *
 * @see org.apache.paimon.operation.FileStoreScan#withScanMode(ScanMode) 设置扫描模式
 */
public enum ScanMode {

    /** 扫描快照的完整数据文件（全量扫描）。 */
    ALL,

    /** 只扫描快照新增/变化的文件（增量扫描）。 */
    DELTA,

    /** 只扫描快照的 Changelog 文件（变更日志扫描）。 */
    CHANGELOG
}
