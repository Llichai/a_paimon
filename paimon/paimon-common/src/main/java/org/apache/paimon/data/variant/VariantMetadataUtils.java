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

package org.apache.paimon.data.variant;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Variant 元数据工具类。
 *
 * <p>用于标记和识别由 Variant 生成的 RowType，使用 DataField 的 description 字段编码 variant 元数据。
 *
 * <p><b>描述字段格式：</b>
 * <pre>
 * __VARIANT_METADATA&lt;path&gt;;&lt;failOnError&gt;;&lt;timeZoneId&gt;
 * </pre>
 *
 * <p><b>示例：</b>
 * <pre>
 * __VARIANT_METADATA$.a.b;true;UTC
 * </pre>
 *
 * <p><b>主要功能：</b>
 * <ul>
 *   <li>构建和解析 variant 元数据描述字符串
 *   <li>识别 RowType 是否源自 variant
 *   <li>提供 VariantRowTypeBuilder 用于构建带元数据的行类型
 * </ul>
 *
 * @since 1.0
 */
public class VariantMetadataUtils {

    /** 元数据键前缀，用于标识 variant 元数据。 */
    public static final String METADATA_KEY = "__VARIANT_METADATA";

    /** 元数据字段分隔符。 */
    public static final String DELIMITER = ";";

    /**
     * 构建 variant 元数据描述字符串。
     *
     * @param path variant 提取路径
     * @param failOnError 转换失败时是否抛出异常
     * @param timeZoneId 时区 ID
     * @return 元数据描述字符串
     */
    public static String buildVariantMetadata(String path, boolean failOnError, String timeZoneId) {
        return METADATA_KEY + path + DELIMITER + failOnError + DELIMITER + timeZoneId;
    }

    /**
     * 构建 variant 元数据描述字符串（使用默认配置）。
     *
     * <p>默认配置：failOnError=true, timeZoneId=UTC
     *
     * @param path variant 提取路径
     * @return 元数据描述字符串
     */
    public static String buildVariantMetadata(String path) {
        return buildVariantMetadata(path, true, "UTC");
    }

    /**
     * 检查 DataType 是否是由 variant 生成的 RowType。
     *
     * <p>通过检查所有字段的 description 是否都以 METADATA_KEY 开头来判断。
     *
     * @param dataType 数据类型
     * @return true 表示是 variant 生成的 RowType
     */
    public static boolean isVariantRowType(DataType dataType) {
        if (!(dataType instanceof RowType)) {
            return false;
        }

        RowType rowType = (RowType) dataType;
        if (rowType.getFields().isEmpty()) {
            return false;
        }
        for (DataField f : rowType.getFields()) {
            if (f.description() == null || !f.description().startsWith(METADATA_KEY)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 为顶层字段添加元数据，标记为 shredding schema（用于写入器）。
     *
     * @param rowType 原始行类型
     * @return 带元数据标记的行类型
     */
    public static RowType addVariantMetadata(RowType rowType) {
        List<DataField> fields = new ArrayList<>();
        for (DataField f : rowType.getFields()) {
            fields.add((f.newDescription(METADATA_KEY)));
        }
        return rowType.copy(fields);
    }

    /**
     * 从 variant 元数据描述中提取路径。
     *
     * @param description 元数据描述字符串
     * @return variant 提取路径
     */
    public static String path(String description) {
        return splitDescription(description)[0];
    }

    /**
     * 从 variant 元数据描述中提取 failOnError 标志。
     *
     * @param description 元数据描述字符串
     * @return true 表示转换失败时抛出异常
     */
    public static boolean failOnError(String description) {
        return Boolean.parseBoolean(splitDescription(description)[1]);
    }

    /**
     * 从 variant 元数据描述中提取时区 ID。
     *
     * @param description 元数据描述字符串
     * @return 时区 ID
     */
    public static ZoneId timeZoneId(String description) {
        return ZoneId.of(splitDescription(description)[2]);
    }

    /**
     * 分割元数据描述字符串。
     *
     * @param description 元数据描述字符串
     * @return 分割后的数组 [path, failOnError, timeZoneId]
     */
    private static String[] splitDescription(String description) {
        return description.substring(METADATA_KEY.length()).split(DELIMITER);
    }

    /**
     * Variant RowType 构建器。
     *
     * <p>用于创建带 variant 元数据的行类型。
     */
    public static class VariantRowTypeBuilder {

        /** 字段列表。 */
        private final List<DataField> fields = new ArrayList<>();

        /** 是否可空。 */
        private final boolean isNullable;

        /** 字段 ID 计数器。 */
        private final AtomicInteger fieldId;

        /**
         * 私有构造方法。
         *
         * @param isNullable 是否可空
         * @param fieldId 字段 ID 计数器
         */
        private VariantRowTypeBuilder(boolean isNullable, AtomicInteger fieldId) {
            this.isNullable = isNullable;
            this.fieldId = fieldId;
        }

        /**
         * 添加字段（使用默认配置）。
         *
         * @param type 字段类型
         * @param path variant 提取路径
         * @return 构建器自身
         */
        public VariantRowTypeBuilder field(DataType type, String path) {
            return field(type, path, true, "UTC");
        }

        /**
         * 添加字段。
         *
         * @param type 字段类型
         * @param path variant 提取路径
         * @param failOnError 转换失败时是否抛出异常
         * @param timeZoneId 时区 ID
         * @return 构建器自身
         */
        public VariantRowTypeBuilder field(
                DataType type, String path, boolean failOnError, String timeZoneId) {
            int id = fieldId.incrementAndGet();
            fields.add(
                    new DataField(
                            id,
                            String.valueOf(id),
                            type,
                            VariantMetadataUtils.buildVariantMetadata(
                                    path, failOnError, timeZoneId)));
            return this;
        }

        /**
         * 创建构建器实例。
         *
         * @return 构建器
         */
        public static VariantRowTypeBuilder builder() {
            return builder(true);
        }

        /**
         * 创建构建器实例。
         *
         * @param isNullable 是否可空
         * @return 构建器
         */
        public static VariantRowTypeBuilder builder(boolean isNullable) {
            return new VariantRowTypeBuilder(isNullable, new AtomicInteger(-1));
        }

        /**
         * 构建 RowType。
         *
         * @return RowType 实例
         */
        public RowType build() {
            return new RowType(isNullable, fields);
        }
    }
}
