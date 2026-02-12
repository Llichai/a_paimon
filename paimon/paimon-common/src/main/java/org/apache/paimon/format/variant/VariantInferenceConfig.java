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

package org.apache.paimon.format.variant;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.variant.InferVariantShreddingSchema;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

/**
 * Variant 类型模式推断的配置类。
 *
 * <p>该配置类封装了 Variant 模式推断所需的所有参数，包括：
 * <ul>
 *   <li>是否启用模式推断
 *   <li>推断的最大宽度和深度限制
 *   <li>字段基数比率阈值
 *   <li>推断缓冲区大小
 * </ul>
 *
 * <p><b>工作原理</b>：
 * <p>当处理包含 Variant 类型的数据时，该配置决定是否需要从实际数据中推断出最优的存储模式。
 * 推断过程会分析一定数量的数据行，识别常见的字段和类型，然后生成碎片化模式。
 *
 * <p><b>配置项说明</b>：
 * <ul>
 *   <li>{@code variant-infer-shredding-schema}: 是否启用自动推断
 *   <li>{@code variant-shredding-schema}: 如果已配置固定模式，则不启用推断
 *   <li>{@code variant-shredding-max-schema-width}: 推断模式的最大列数
 *   <li>{@code variant-shredding-max-schema-depth}: 推断模式的最大嵌套深度
 *   <li>{@code variant-shredding-min-field-cardinality-ratio}: 最小字段出现频率
 *   <li>{@code variant-shredding-max-infer-buffer-row}: 推断时缓冲的最大行数
 * </ul>
 */
public class VariantInferenceConfig {

    /** 数据行类型 */
    private final RowType rowType;
    /** 配置选项 */
    private final Options options;

    /**
     * 构造函数。
     *
     * @param rowType 数据行类型
     * @param options 配置选项
     */
    public VariantInferenceConfig(RowType rowType, Options options) {
        this.rowType = rowType;
        this.options = options;
    }

    /**
     * 判断是否应该启用 Variant 模式推断。
     *
     * <p>启用条件：
     * <ol>
     *   <li>没有配置固定的 Variant 碎片化模式
     *   <li>配置项 {@code variant-infer-shredding-schema} 为 true
     *   <li>数据行类型中包含 Variant 类型字段
     * </ol>
     *
     * @return 如果应该启用推断返回 true，否则返回 false
     */
    public boolean shouldEnableInference() {
        if (options.contains(CoreOptions.VARIANT_SHREDDING_SCHEMA)) {
            return false;
        }

        if (!options.get(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA)) {
            return false;
        }

        return containsVariantFields(rowType);
    }

    /**
     * 检查行类型中是否包含 Variant 类型字段。
     *
     * @param rowType 要检查的行类型
     * @return 如果包含 Variant 字段返回 true，否则返回 false
     */
    private boolean containsVariantFields(RowType rowType) {
        for (DataField field : rowType.getFields()) {
            if (field.type() instanceof VariantType) {
                return true;
            }
        }
        return false;
    }

    /**
     * 创建模式推断器。
     *
     * @return Variant 碎片化模式推断器
     */
    public InferVariantShreddingSchema createInferrer() {
        return new InferVariantShreddingSchema(
                rowType,
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO));
    }

    /**
     * 获取推断时缓冲的最大行数。
     *
     * @return 最大缓冲行数
     */
    public int getMaxBufferRow() {
        return options.get(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW);
    }

    /**
     * 获取数据行类型。
     *
     * @return 行类型
     */
    public RowType rowType() {
        return rowType;
    }
}
