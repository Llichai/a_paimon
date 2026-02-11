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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 行类型生成器。
 *
 * <p>在某些场景下，行的变更类型（INSERT/UPDATE/DELETE）不是通过 Paimon 的
 * {@link RowKind} API 传递的，而是存储在数据的某个字段中。这个类负责从字段
 * 中提取 RowKind 信息。
 *
 * <p>典型使用场景：
 * <ul>
 *   <li><b>CDC（Change Data Capture）数据导入</b>：
 *       <ul>
 *         <li>CDC 工具（如 Debezium）通常将操作类型存储在字段中
 *         <li>例如：op='I' 表示 INSERT，op='U' 表示 UPDATE，op='D' 表示 DELETE
 *       </ul>
 *   <li><b>外部数据源集成</b>：
 *       <ul>
 *         <li>某些数据源不支持 Paimon 的 RowKind API
 *         <li>需要从数据字段中解析操作类型
 *       </ul>
 *   <li><b>格式转换</b>：
 *       <ul>
 *         <li>将其他格式的变更标记转换为 Paimon 的 RowKind
 *       </ul>
 * </ul>
 *
 * <p>配置方式：
 * <pre>
 * CREATE TABLE t (
 *   id BIGINT,
 *   name STRING,
 *   op_type STRING  -- 存储操作类型的字段
 * ) WITH (
 *   'rowkind.field' = 'op_type'  -- 指定 RowKind 字段
 * );
 * </pre>
 *
 * <p>支持的字符串表示：
 * <ul>
 *   <li>'I' 或 '+I'：INSERT
 *   <li>'U' 或 '+U'：UPDATE_AFTER
 *   <li>'-U'：UPDATE_BEFORE
 *   <li>'D' 或 '-D'：DELETE
 * </ul>
 *
 * <p>约束条件：
 * <ul>
 *   <li>RowKind 字段必须是字符串类型
 *   <li>RowKind 字段的值不能为 null
 *   <li>字符串必须是有效的 RowKind 表示
 * </ul>
 *
 * @see RowKind RowKind 枚举和字符串解析方法
 * @see CoreOptions#rowkindField() RowKind 字段配置
 */
public class RowKindGenerator {

    /** RowKind 字段在行中的索引位置 */
    private final int index;

    /**
     * 构造行类型生成器。
     *
     * @param field RowKind 字段名
     * @param rowType 行的数据类型
     * @throws RuntimeException 如果字段不存在
     * @throws IllegalArgumentException 如果字段类型不是字符串
     */
    public RowKindGenerator(String field, RowType rowType) {
        // 查找字段索引
        this.index = rowType.getFieldNames().indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format("Can not find rowkind %s in table schema: %s", field, rowType));
        }

        // 验证字段类型必须是字符串
        DataType fieldType = rowType.getTypeAt(index);
        checkArgument(
                fieldType.is(CHARACTER_STRING),
                "only support string type for rowkind, but %s is %s",
                field,
                fieldType);
    }

    /**
     * 从行中生成 RowKind。
     *
     * <p>从配置的字段中读取字符串值，并解析为 RowKind。
     *
     * @param row 待处理的行记录
     * @return 解析出的 RowKind
     * @throws RuntimeException 如果 RowKind 字段为 null
     */
    public RowKind generate(InternalRow row) {
        // RowKind 字段不能为 null
        if (row.isNullAt(index)) {
            throw new RuntimeException("Row kind cannot be null.");
        }

        // 从字符串解析 RowKind
        // 支持的格式：'I', '+I', 'U', '+U', '-U', 'D', '-D'
        return RowKind.fromShortString(row.getString(index).toString());
    }

    /**
     * 根据配置创建 RowKindGenerator。
     *
     * <p>静态工厂方法，根据表配置决定是否创建生成器：
     * <ul>
     *   <li>如果配置了 {@code rowkind.field}，创建生成器
     *   <li>否则返回 null，表示使用默认的 RowKind API
     * </ul>
     *
     * @param schema 表模式
     * @param options 表配置
     * @return RowKindGenerator 实例，如果未配置则返回 null
     */
    @Nullable
    public static RowKindGenerator create(TableSchema schema, CoreOptions options) {
        return options.rowkindField()
                .map(field -> new RowKindGenerator(field, schema.logicalRowType()))
                .orElse(null);
    }

    /**
     * 获取行的 RowKind。
     *
     * <p>统一的静态方法，自动选择 RowKind 来源：
     * <ul>
     *   <li>如果提供了生成器，从字段中提取
     *   <li>否则使用行自带的 RowKind
     * </ul>
     *
     * @param rowKindGenerator RowKind 生成器，可以为 null
     * @param row 待处理的行记录
     * @return 行的 RowKind
     */
    public static RowKind getRowKind(@Nullable RowKindGenerator rowKindGenerator, InternalRow row) {
        return rowKindGenerator == null ? row.getRowKind() : rowKindGenerator.generate(row);
    }
}
