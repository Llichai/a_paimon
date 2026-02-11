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

import java.util.Collections;
import java.util.List;

/**
 * 快照不存在时的扫描计划。
 *
 * <p>该类用于区分两种不同的"空计划"情况:
 * <ul>
 *   <li>快照不存在: 使用 {@link SnapshotNotExistPlan}(本类)
 *   <li>快照存在但无数据: 使用 {@link DataFilePlan} 包含空列表
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>表刚创建,还没有任何快照
 *   <li>指定的快照ID不存在
 *   <li>时间旅行查询的时间点早于所有快照
 *   <li>增量扫描的起始快照不存在
 * </ul>
 *
 * <p>与空数据计划的区别:
 * <ul>
 *   <li>SnapshotNotExistPlan: 快照本身不存在,无法进行扫描
 *   <li>DataFilePlan(empty): 快照存在,但数据文件为空(如表刚初始化)
 * </ul>
 *
 * <p>下游处理差异:
 * <ul>
 *   <li>SnapshotNotExistPlan: 可能需要等待或报错
 *   <li>空 DataFilePlan: 可以正常处理(返回空结果)
 * </ul>
 *
 * <p>设计模式:
 * <ul>
 *   <li>单例模式: 使用 INSTANCE 避免重复创建
 *   <li>私有构造函数: 强制使用单例实例
 * </ul>
 *
 * This is used to distinguish the case where the snapshot does not exist and the plan is empty.
 */
public class SnapshotNotExistPlan implements TableScan.Plan {

    /**
     * 单例实例。
     *
     * <p>因为快照不存在计划没有状态,使用单例可以:
     * <ul>
     *   <li>避免重复创建对象
     *   <li>节省内存开销
     *   <li>便于身份比较(可以使用 ==)
     * </ul>
     */
    public static final SnapshotNotExistPlan INSTANCE = new SnapshotNotExistPlan();

    /**
     * 私有构造函数,强制使用单例实例。
     */
    private SnapshotNotExistPlan() {
        // private
    }

    /**
     * 返回空的分片列表。
     *
     * <p>因为快照不存在,无法生成任何分片。
     *
     * @return 空列表
     */
    @Override
    public List<Split> splits() {
        return Collections.emptyList();
    }
}
