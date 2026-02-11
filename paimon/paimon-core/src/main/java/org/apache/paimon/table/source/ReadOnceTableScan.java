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

import org.apache.paimon.manifest.PartitionEntry;

import java.util.List;

/**
 * 一次性读取扫描，用于只执行一次的批量扫描场景。
 *
 * <p>ReadOnceTableScan 是一个抽象基类，适用于只需要执行一次扫描的场景（如批处理）。
 * 该类确保 {@link #plan()} 方法只能被调用一次，第二次调用会抛出 {@link EndOfScanException}。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>批量读取</b>: 一次性读取完整快照</li>
 *   <li><b>系统表读取</b>: 读取元数据表（如快照表、分区表）</li>
 *   <li><b>一次性查询</b>: SELECT 查询（不需要持续读取）</li>
 * </ul>
 *
 * <h3>与流式扫描的区别</h3>
 * <ul>
 *   <li><b>ReadOnceTableScan</b>: 只执行一次扫描，第二次调用抛出异常</li>
 *   <li><b>StreamTableScan</b>: 可以多次调用 plan()，持续读取新数据</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * ReadOnceTableScan scan = new MyReadOnceTableScan();
 *
 * // 第一次调用成功
 * Plan plan1 = scan.plan();
 *
 * // 第二次调用抛出 EndOfScanException
 * try {
 *     Plan plan2 = scan.plan();
 * } catch (EndOfScanException e) {
 *     // 扫描已结束
 * }
 * }</pre>
 *
 * <h3>子类实现</h3>
 * <p>子类需要实现 {@link #innerPlan()} 方法，提供具体的扫描逻辑。
 *
 * @see InnerTableScan 表扫描接口
 * @see EndOfScanException 扫描结束异常
 */
public abstract class ReadOnceTableScan implements InnerTableScan {

    private boolean hasNext = true;

    /**
     * 生成扫描计划（只能调用一次）。
     *
     * <p>第一次调用返回扫描计划，第二次调用抛出 {@link EndOfScanException}。
     *
     * @return 扫描计划
     * @throws EndOfScanException 如果已经调用过一次
     */
    @Override
    public Plan plan() {
        if (hasNext) {
            hasNext = false;
            return innerPlan();
        } else {
            throw new EndOfScanException();
        }
    }

    /**
     * 生成内部扫描计划（由子类实现）。
     *
     * @return 扫描计划
     */
    protected abstract Plan innerPlan();

    /**
     * 列举分区条目（不支持）。
     *
     * <p>该方法在 ReadOnceTableScan 中不支持，子类可以根据需要覆盖。
     *
     * @throws UnsupportedOperationException 总是抛出
     */
    @Override
    public List<PartitionEntry> listPartitionEntries() {
        throw new UnsupportedOperationException();
    }
}
