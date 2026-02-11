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

package org.apache.paimon.schema;

import org.apache.paimon.casting.CastFieldGetter;

import javax.annotation.Nullable;

/**
 * 索引类型转换映射
 *
 * <p>IndexCastMapping 包含了 Schema 演化时所需的两种映射关系：
 * <ul>
 *   <li>索引映射（Index Mapping）：字段位置的映射（新 Schema → 旧 Schema）
 *   <li>类型转换映射（Cast Mapping）：字段类型的转换器
 * </ul>
 *
 * <p>使用场景：
 * <pre>
 * 当表的 Schema 发生变更后，历史数据文件使用旧 Schema，需要将旧数据适配到新 Schema：
 *
 * 1. 索引映射：处理字段位置变化
 *    旧 Schema：[id INT, name STRING, age INT]
 *    新 Schema：[age INT, id INT, name STRING]
 *    索引映射：[2, 0, 1]
 *    - 新 Schema 的字段 0 (age) → 旧 Schema 的字段 2
 *    - 新 Schema 的字段 1 (id) → 旧 Schema 的字段 0
 *    - 新 Schema 的字段 2 (name) → 旧 Schema 的字段 1
 *
 * 2. 类型转换映射：处理字段类型变化
 *    旧 Schema：[id INT, age INT]
 *    新 Schema：[id BIGINT, age BIGINT]
 *    类型转换映射：[INT→BIGINT, INT→BIGINT]
 *    - 字段 0：INT 转换为 BIGINT
 *    - 字段 1：INT 转换为 BIGINT
 *
 * 3. 综合场景：位置和类型都变化
 *    旧 Schema：[id INT, name STRING, age INT]
 *    新 Schema：[age BIGINT, id BIGINT, email STRING, name STRING]
 *    索引映射：[2, 0, -1, 1]
 *    - 字段 0 (age)：从旧 Schema 字段 2 读取，INT → BIGINT
 *    - 字段 1 (id)：从旧 Schema 字段 0 读取，INT → BIGINT
 *    - 字段 2 (email)：旧 Schema 没有，返回 NULL（-1）
 *    - 字段 3 (name)：从旧 Schema 字段 1 读取，类型不变
 * </pre>
 *
 * <p>索引映射（Index Mapping）：
 * <ul>
 *   <li>数组长度等于新 Schema 的字段数
 *   <li>每个元素表示新 Schema 的字段在旧 Schema 中的位置
 *   <li>如果字段在旧 Schema 中不存在，映射为 -1（NULL_FIELD_INDEX）
 *   <li>如果所有字段位置都没变化（[0, 1, 2, ...]），返回 null（无需映射）
 * </ul>
 *
 * <p>类型转换映射（Cast Mapping）：
 * <ul>
 *   <li>数组长度等于新 Schema 的字段数
 *   <li>每个元素是 {@link CastFieldGetter}，包含字段读取器和类型转换器
 *   <li>如果字段类型没有变化，转换器是 IdentityCastExecutor（无操作）
 *   <li>如果所有字段类型都一致，返回 null（无需转换）
 * </ul>
 *
 * <p>使用流程：
 * <pre>{@code
 * // 1. 创建索引和类型转换映射
 * List<DataField> tableFields = newSchema.fields();
 * List<DataField> dataFields = oldSchema.fields();
 * IndexCastMapping mapping = SchemaEvolutionUtil.createIndexCastMapping(
 *     tableFields,
 *     dataFields
 * );
 *
 * // 2. 获取映射
 * int[] indexMapping = mapping.getIndexMapping();
 * CastFieldGetter[] castMapping = mapping.getCastMapping();
 *
 * // 3. 应用索引映射（重排字段）
 * InternalRow dataRow = ...; // 从数据文件读取的 Row
 * if (indexMapping != null) {
 *     // 使用 ProjectedRow 重新排列字段
 *     ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
 *     dataRow = projectedRow.replaceRow(dataRow);
 * }
 *
 * // 4. 应用类型转换映射（转换类型）
 * if (castMapping != null) {
 *     // 使用 CastedRow 转换字段类型
 *     CastedRow castedRow = CastedRow.from(castMapping);
 *     dataRow = castedRow.replaceRow(dataRow);
 * }
 *
 * // 5. 现在 dataRow 已经适配到新 Schema
 * }</pre>
 *
 * <p>性能优化：
 * <ul>
 *   <li>如果 Schema 没有变化，两个映射都返回 null，避免无意义的操作
 *   <li>索引映射使用 {@link org.apache.paimon.utils.ProjectedRow}，零拷贝投影
 *   <li>类型转换使用高效的 {@link org.apache.paimon.casting.CastExecutor}
 * </ul>
 *
 * <p>与其他组件的关系：
 * <ul>
 *   <li>{@link SchemaEvolutionUtil#createIndexCastMapping}：创建此映射
 *   <li>{@link org.apache.paimon.utils.ProjectedRow}：应用索引映射
 *   <li>{@link org.apache.paimon.casting.CastedRow}：应用类型转换映射
 *   <li>{@link org.apache.paimon.operation.SplitRead}：读取数据时使用此映射
 * </ul>
 *
 * @see SchemaEvolutionUtil Schema 演化工具
 * @see org.apache.paimon.casting.CastFieldGetter 字段读取和转换器
 * @see org.apache.paimon.utils.ProjectedRow 投影行
 * @see org.apache.paimon.casting.CastedRow 类型转换行
 */
public interface IndexCastMapping {

    /**
     * 获取索引映射
     *
     * <p>索引映射定义了新 Schema 的字段在旧 Schema 中的位置。
     *
     * <p>示例：
     * <pre>
     * 旧 Schema：[id INT, name STRING, age INT]
     * 新 Schema：[age INT, id INT, email STRING, name STRING]
     * 索引映射：[2, 0, -1, 1]
     * - 新字段 0 (age) → 旧字段 2
     * - 新字段 1 (id) → 旧字段 0
     * - 新字段 2 (email) → -1（不存在）
     * - 新字段 3 (name) → 旧字段 1
     * </pre>
     *
     * @return 索引映射数组，如果不需要映射则返回 null
     */
    @Nullable
    int[] getIndexMapping();

    /**
     * 获取类型转换映射
     *
     * <p>类型转换映射定义了如何读取和转换每个字段。
     *
     * <p>示例：
     * <pre>
     * 旧 Schema：[id INT, name STRING]
     * 新 Schema：[id BIGINT, name STRING]
     * 类型转换映射：
     * - 字段 0：FieldGetter(index=0) + CastExecutor(INT→BIGINT)
     * - 字段 1：FieldGetter(index=1) + CastExecutor(Identity)
     * </pre>
     *
     * @return 类型转换映射数组，如果不需要转换则返回 null
     */
    @Nullable
    CastFieldGetter[] getCastMapping();
}
