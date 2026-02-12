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

import org.apache.paimon.data.InternalRow;

import java.util.Comparator;

/**
 * {@link InternalRow} 的字段比较器接口。
 *
 * <p>该接口扩展了 {@link Comparator},专门用于比较 {@link InternalRow} 中的特定字段。
 * 与标准的 Comparator 不同,该接口增加了获取比较字段索引的能力,这在需要部分字段比较的场景中非常有用。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>字段选择</b> - 指定要参与比较的字段索引
 *   <li><b>行比较</b> - 比较两个 InternalRow 对象
 *   <li><b>排序支持</b> - 支持基于特定字段的排序操作
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>主键比较</b> - 仅比较主键字段
 *   <li><b>分区键比较</b> - 仅比较分区键字段
 *   <li><b>排序键比较</b> - 按指定字段排序
 *   <li><b>合并排序</b> - 在 MergeTree 中按键字段排序
 *   <li><b>去重</b> - 基于特定字段检测重复行
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建主键比较器（假设主键是第0、1字段）
 * FieldsComparator primaryKeyComparator = new FieldsComparator() {
 *     @Override
 *     public int[] compareFields() {
 *         return new int[] {0, 1};  // 比较第0和第1字段
 *     }
 *
 *     @Override
 *     public int compare(InternalRow row1, InternalRow row2) {
 *         // 依次比较每个字段
 *         for (int fieldIdx : compareFields()) {
 *             int result = compareField(row1, row2, fieldIdx);
 *             if (result != 0) {
 *                 return result;
 *             }
 *         }
 *         return 0;
 *     }
 * };
 *
 * // 2. 使用比较器排序
 * List<InternalRow> rows = new ArrayList<>();
 * Collections.sort(rows, primaryKeyComparator);
 *
 * // 3. 在 TreeMap 中使用
 * TreeMap<InternalRow, String> map = new TreeMap<>(primaryKeyComparator);
 * map.put(row, "value");
 *
 * // 4. 获取比较字段信息
 * int[] compareFields = primaryKeyComparator.compareFields();
 * System.out.println("比较字段: " + Arrays.toString(compareFields));
 * }</pre>
 *
 * <h2>实现建议</h2>
 * <ul>
 *   <li><b>性能优化</b> - 缓存 compareFields() 返回的数组,避免重复创建
 *   <li><b>短路求值</b> - 在 compare 中遇到非零结果立即返回
 *   <li><b>类型安全</b> - 确保字段索引在有效范围内
 *   <li><b>空值处理</b> - 正确处理字段为 null 的情况
 *   <li><b>一致性</b> - 确保 compare 与 equals/hashCode 一致
 * </ul>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>返回不变性</b> - compareFields() 应返回固定的字段索引数组
 *   <li><b>字段顺序</b> - 返回数组中字段的顺序决定了比较的优先级
 *   <li><b>合约遵守</b> - 实现必须遵守 Comparator 的通用合约
 * </ul>
 *
 * @see Comparator
 * @see InternalRow
 */
public interface FieldsComparator extends Comparator<InternalRow> {

    /**
     * 获取要比较的字段索引数组。
     *
     * <p>返回的数组指定了在比较两个 InternalRow 时需要考虑的字段位置。
     * 字段在数组中的顺序决定了比较的优先级,即先比较索引0处的字段,
     * 如果相等再比较索引1处的字段,依此类推。
     *
     * <p><b>性能提示</b>: 实现应缓存此数组,避免重复创建。
     *
     * <p><b>示例</b>:
     * <pre>{@code
     * // 返回 [0, 1] 表示先比较第0字段,再比较第1字段
     * return new int[] {0, 1};
     *
     * // 返回 [2] 表示只比较第2字段
     * return new int[] {2};
     * }</pre>
     *
     * @return 字段索引数组,不应为 null 或空数组
     */
    int[] compareFields();
}
