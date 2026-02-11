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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.FirstRowMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE;
import static org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;
import static org.apache.paimon.table.SpecialFields.KEY_FIELD_ID_START;
import static org.apache.paimon.table.SpecialFields.KEY_FIELD_PREFIX;

/**
 * 主键表工具类 - 提供主键表相关的工具方法
 *
 * <p>PrimaryKeyTableUtils 提供了与主键表（Primary Key Table）相关的工具方法，包括：
 * <ul>
 *   <li>主键字段名称前缀处理
 *   <li>MergeFunction 工厂创建
 *   <li>主键字段提取器
 *   <li>主键 Upsert 删除能力校验
 * </ul>
 *
 * <p><b>主键字段命名规范：</b>
 * <ul>
 *   <li>主键字段在内部存储时会添加前缀 {@link SpecialFields#KEY_FIELD_PREFIX}（"_KEY_"）
 *   <li>主键字段 ID 会加上 {@link SpecialFields#KEY_FIELD_ID_START}（偏移量）
 *   <li>这样可以区分主键字段和值字段，避免冲突
 * </ul>
 *
 * <p><b>为什么需要主键字段前缀？</b>
 * <ul>
 *   <li>主键字段和值字段可能同名（如 id 既是主键又是值字段）
 *   <li>在 KeyValue 存储中，需要明确区分 Key 和 Value
 *   <li>通过前缀可以避免字段名冲突
 * </ul>
 *
 * <p><b>MergeFunction 类型：</b>
 * <ul>
 *   <li><b>DEDUPLICATE</b>：去重，保留最新记录
 *   <li><b>PARTIAL_UPDATE</b>：部分更新，只更新指定字段
 *   <li><b>AGGREGATE</b>：聚合，对字段进行聚合操作（SUM、MAX、MIN 等）
 *   <li><b>FIRST_ROW</b>：保留第一条记录（First-Win）
 * </ul>
 *
 * <p><b>主键 Upsert 删除能力：</b>
 * <ul>
 *   <li>某些 Merge Engine 默认不支持通过主键 Upsert 实现删除
 *   <li>需要配置特定选项才能启用删除能力
 *   <li>{@link #validatePKUpsertDeletable} 用于校验表是否支持主键删除
 * </ul>
 *
 * <p><b>示例用法：</b>
 * <pre>{@code
 * // 1. 添加主键字段前缀
 * RowType keyType = new RowType(...); // [id, name]
 * RowType prefixedKeyType = PrimaryKeyTableUtils.addKeyNamePrefix(keyType);
 * // 结果：[_KEY_id, _KEY_name]
 *
 * // 2. 创建 MergeFunction 工厂
 * TableSchema schema = ...;
 * MergeFunctionFactory<KeyValue> factory =
 *     PrimaryKeyTableUtils.createMergeFunctionFactory(schema);
 *
 * // 3. 校验主键删除能力
 * Table table = ...;
 * PrimaryKeyTableUtils.validatePKUpsertDeletable(table);
 * // 如果不支持，会抛出 UnsupportedOperationException
 * }</pre>
 *
 * @see org.apache.paimon.mergetree.compact.MergeFunctionFactory
 * @see SpecialFields
 */
public class PrimaryKeyTableUtils {

    /**
     * 为 RowType 的所有字段添加主键前缀
     *
     * @param type 原始 RowType
     * @return 添加前缀后的 RowType
     */
    public static RowType addKeyNamePrefix(RowType type) {
        return new RowType(addKeyNamePrefix(type.getFields()));
    }

    /**
     * 为字段列表添加主键前缀
     *
     * <p>转换规则：
     * <ul>
     *   <li>字段名：name → _KEY_name
     *   <li>字段 ID：id → id + KEY_FIELD_ID_START
     * </ul>
     *
     * @param keyFields 主键字段列表
     * @return 添加前缀后的字段列表
     */
    public static List<DataField> addKeyNamePrefix(List<DataField> keyFields) {
        return keyFields.stream()
                .map(f -> f.newName(KEY_FIELD_PREFIX + f.name()).newId(f.id() + KEY_FIELD_ID_START))
                .collect(Collectors.toList());
    }

    /**
     * 根据表 Schema 创建 MergeFunction 工厂
     *
     * <p>根据表的 merge-engine 配置，选择对应的 MergeFunction 实现：
     * <ul>
     *   <li><b>deduplicate</b> → {@link DeduplicateMergeFunction}
     *   <li><b>partial-update</b> → {@link PartialUpdateMergeFunction}
     *   <li><b>aggregate</b> → {@link AggregateMergeFunction}
     *   <li><b>first-row</b> → {@link FirstRowMergeFunction}
     * </ul>
     *
     * @param tableSchema 表的 Schema
     * @return MergeFunctionFactory 实例
     * @throws UnsupportedOperationException 如果 merge-engine 不支持
     */
    public static MergeFunctionFactory<KeyValue> createMergeFunctionFactory(
            TableSchema tableSchema) {
        RowType rowType = tableSchema.logicalRowType();
        Options conf = Options.fromMap(tableSchema.options());
        CoreOptions options = new CoreOptions(conf);
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();

        switch (mergeEngine) {
            case DEDUPLICATE:
                return DeduplicateMergeFunction.factory(conf);
            case PARTIAL_UPDATE:
                return PartialUpdateMergeFunction.factory(conf, rowType, tableSchema.primaryKeys());
            case AGGREGATE:
                return AggregateMergeFunction.factory(conf, rowType, tableSchema.primaryKeys());
            case FIRST_ROW:
                return FirstRowMergeFunction.factory(conf);
            default:
                throw new UnsupportedOperationException("Unsupported merge engine: " + mergeEngine);
        }
    }

    /**
     * 主键字段提取器 - 用于从 TableSchema 提取主键和值字段
     *
     * <p>该提取器实现了 {@link KeyValueFieldsExtractor} 接口，用于：
     * <ul>
     *   <li>提取主键字段（添加前缀）
     *   <li>提取值字段（保持原样）
     * </ul>
     *
     * <p>单例模式，通过 {@link #EXTRACTOR} 获取实例。
     */
    public static class PrimaryKeyFieldsExtractor implements KeyValueFieldsExtractor {

        private static final long serialVersionUID = 1L;

        /** 单例实例 */
        public static final PrimaryKeyFieldsExtractor EXTRACTOR = new PrimaryKeyFieldsExtractor();

        private PrimaryKeyFieldsExtractor() {}

        /**
         * 提取主键字段（添加前缀）
         *
         * @param schema 表 Schema
         * @return 添加前缀后的主键字段列表
         */
        @Override
        public List<DataField> keyFields(TableSchema schema) {
            return addKeyNamePrefix(schema.trimmedPrimaryKeysFields());
        }

        /**
         * 提取值字段（所有字段）
         *
         * @param schema 表 Schema
         * @return 值字段列表
         */
        @Override
        public List<DataField> valueFields(TableSchema schema) {
            return schema.fields();
        }
    }

    /**
     * 校验表是否支持通过主键 Upsert 实现删除
     *
     * <p>该方法检查：
     * <ol>
     *   <li>表是否有主键
     *   <li>Merge Engine 是否支持删除
     *   <li>是否配置了必要的删除选项
     * </ol>
     *
     * <p><b>各 Merge Engine 的删除支持：</b>
     * <ul>
     *   <li><b>DEDUPLICATE</b>：默认支持（直接删除记录）
     *   <li><b>PARTIAL_UPDATE</b>：
     *       <ul>
     *         <li>需要配置 {@code partial-update.remove-record-on-delete=true}
     *         <li>或配置 {@code partial-update.remove-record-on-sequence-group}
     *       </ul>
     *   <li><b>AGGREGATE</b>：
     *       <ul>
     *         <li>需要配置 {@code aggregation.remove-record-on-delete=true}
     *       </ul>
     *   <li><b>FIRST_ROW</b>：不支持删除
     * </ul>
     *
     * <p><b>示例：</b>
     * <pre>{@code
     * // PARTIAL_UPDATE 表启用删除
     * table.options().put("partial-update.remove-record-on-delete", "true");
     * PrimaryKeyTableUtils.validatePKUpsertDeletable(table); // 通过
     *
     * // AGGREGATE 表未启用删除
     * PrimaryKeyTableUtils.validatePKUpsertDeletable(table);
     * // 抛出异常：Merge engine aggregate doesn't support batch delete by default.
     * //         To support batch delete, please set aggregation.remove-record-on-delete to true.
     * }</pre>
     *
     * @param table 要校验的表
     * @throws UnsupportedOperationException 如果表不支持主键删除
     */
    public static void validatePKUpsertDeletable(Table table) {
        if (table.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "table '%s' can not support delete, because there is no primary key.",
                            table.getClass().getName()));
        }

        Options options = Options.fromMap(table.options());
        CoreOptions.MergeEngine mergeEngine = options.get(MERGE_ENGINE);

        switch (mergeEngine) {
            case DEDUPLICATE:
                return;
            case PARTIAL_UPDATE:
                if (options.get(PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE)
                        || options.get(PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP) != null) {
                    return;
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Merge engine %s doesn't support batch delete by default. To support batch delete, "
                                            + "please set %s to true when there is no %s or set %s.",
                                    mergeEngine,
                                    PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key(),
                                    SEQUENCE_GROUP,
                                    PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP));
                }
            case AGGREGATE:
                if (options.get(AGGREGATION_REMOVE_RECORD_ON_DELETE)) {
                    return;
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Merge engine %s doesn't support batch delete by default. To support batch delete, "
                                            + "please set %s to true.",
                                    mergeEngine, AGGREGATION_REMOVE_RECORD_ON_DELETE.key()));
                }
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Merge engine %s can not support batch delete.", mergeEngine));
        }
    }
}
