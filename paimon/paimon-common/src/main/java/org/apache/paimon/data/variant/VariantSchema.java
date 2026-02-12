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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Variant切片模式定义。
 *
 * <p>定义有效的切片(shredding)模式,如
 * https://github.com/apache/parquet-format/blob/master/VariantShredding.md 所述。
 *
 * <h2>切片(Shredding)概念</h2>
 * <p>Shredding是将半结构化的Variant数据转换为列式存储的优化技术。通过将常见字段
 * "切片"到独立的列中,可以:
 * <ul>
 *   <li>提高查询性能(列式存储优势)</li>
 *   <li>改善数据压缩率</li>
 *   <li>支持高效的列式谓词下推</li>
 *   <li>保持schema演化的灵活性</li>
 * </ul>
 *
 * <h2>模式结构</h2>
 * <p>切片模式包含以下字段:
 * <ul>
 *   <li><b>value</b>: 存储原始Variant值(可选)</li>
 *   <li><b>typed_value</b>: 存储类型化的值(可选)</li>
 *   <li><b>metadata</b>: 仅在顶层包含元数据字段</li>
 * </ul>
 *
 * <p>如果typed_value是数组或结构体,它会递归包含自己的切片模式。
 *
 * <h2>支持的标量类型</h2>
 * <ul>
 *   <li>{@link StringType} - 字符串</li>
 *   <li>{@link IntegralType} - 整数(BYTE/SHORT/INT/LONG)</li>
 *   <li>{@link FloatType} - 单精度浮点</li>
 *   <li>{@link DoubleType} - 双精度浮点</li>
 *   <li>{@link BooleanType} - 布尔值</li>
 *   <li>{@link BinaryType} - 二进制</li>
 *   <li>{@link DecimalType} - 高精度小数</li>
 *   <li>{@link DateType} - 日期</li>
 *   <li>{@link TimestampType} - 时间戳</li>
 *   <li>{@link TimestampNTZType} - 无时区时间戳</li>
 *   <li>{@link UuidType} - UUID</li>
 * </ul>
 *
 * <h2>模式示例</h2>
 * <pre>
 * 原始Variant数据:
 * {
 *   "name": "Alice",
 *   "age": 30,
 *   "address": {
 *     "city": "Beijing",
 *     "zip": "100000"
 *   }
 * }
 *
 * 切片后的列式存储:
 * ┌──────────────────┬─────────┬──────────────┬──────────┐
 * │ metadata         │ value   │ typed_value  │  ...     │
 * ├──────────────────┼─────────┼──────────────┼──────────┤
 * │ {name,age,addr}  │ null    │ STRUCT(...)  │  ...     │
 * │   └─ name        │ null    │ "Alice"      │  ...     │
 * │   └─ age         │ null    │ 30           │  ...     │
 * │   └─ address     │ null    │ STRUCT(...)  │  ...     │
 * │       └─ city    │ null    │ "Beijing"    │  ...     │
 * │       └─ zip     │ null    │ "100000"     │  ...     │
 * └──────────────────┴─────────┴──────────────┴──────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建字符串类型的切片模式
 * VariantSchema stringSchema = new VariantSchema(
 *     0,                     // typed_value索引
 *     -1,                    // 无value字段
 *     1,                     // metadata索引(仅顶层)
 *     2,                     // 字段数量
 *     new StringType(),      // 标量类型
 *     null,                  // 非对象
 *     null                   // 非数组
 * );
 *
 * // 创建对象类型的切片模式
 * ObjectField[] fields = {
 *     new ObjectField("name", nameSchema),
 *     new ObjectField("age", ageSchema)
 * };
 * VariantSchema objectSchema = new VariantSchema(
 *     0, -1, 1, 2,
 *     null,                  // 非标量
 *     fields,                // 对象字段
 *     null                   // 非数组
 * );
 *
 * // 检查是否未切片
 * boolean unshredded = schema.isUnshredded();
 * }</pre>
 *
 * <h2>字段索引说明</h2>
 * <ul>
 *   <li>typedIdx: typed_value字段的索引,如果不存在则为-1</li>
 *   <li>variantIdx: value字段的索引,如果不存在则为-1</li>
 *   <li>topLevelMetadataIdx: metadata字段的索引,仅顶层有效,其他层为-1</li>
 *   <li>所有有效索引必须从0开始且连续</li>
 * </ul>
 *
 * <p>注意:此文件基于Spark项目源代码,遵循Apache License 2.0。
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/VariantShredding.md">
 *     Parquet Variant Shredding规范</a>
 */
public class VariantSchema {

    /**
     * 表示切片模式中对象的一个字段。
     *
     * <p>每个对象字段包含字段名和对应的切片模式。
     */
    public static final class ObjectField {
        /** 字段名称。 */
        public final String fieldName;
        /** 字段的切片模式。 */
        public final VariantSchema schema;

        /**
         * 构造对象字段。
         *
         * @param fieldName 字段名称
         * @param schema 字段的切片模式
         */
        public ObjectField(String fieldName, VariantSchema schema) {
            this.fieldName = fieldName;
            this.schema = schema;
        }

        public String fieldName() {
            return fieldName;
        }

        public VariantSchema schema() {
            return schema;
        }

        @Override
        public String toString() {
            return "ObjectField{" + "fieldName=" + fieldName + ", schema=" + schema + '}';
        }
    }

    /** 标量类型的基类。 */
    public abstract static class ScalarType {}

    /** 字符串类型。 */
    public static final class StringType extends ScalarType {}

    /**
     * 整数大小枚举。
     *
     * <p>定义不同大小的整数类型,用于优化存储。
     */
    public enum IntegralSize {
        /** 字节:8位整数(-128到127)。 */
        BYTE,
        /** 短整数:16位整数(-32768到32767)。 */
        SHORT,
        /** 整数:32位整数。 */
        INT,
        /** 长整数:64位整数。 */
        LONG
    }

    /**
     * 整数类型。
     *
     * <p>支持不同大小的整数,以便优化存储空间。
     */
    public static final class IntegralType extends ScalarType {
        /** 整数的大小。 */
        public final IntegralSize size;

        /**
         * 构造整数类型。
         *
         * @param size 整数大小
         */
        public IntegralType(IntegralSize size) {
            this.size = size;
        }
    }

    /** 单精度浮点类型。 */
    public static final class FloatType extends ScalarType {}

    /** 双精度浮点类型。 */
    public static final class DoubleType extends ScalarType {}

    /** 布尔类型。 */
    public static final class BooleanType extends ScalarType {}

    /** 二进制类型。 */
    public static final class BinaryType extends ScalarType {}

    /**
     * 高精度小数类型。
     *
     * <p>支持指定精度和小数位数的小数。
     */
    public static final class DecimalType extends ScalarType {
        /** 总精度(总位数)。 */
        public final int precision;
        /** 小数位数。 */
        public final int scale;

        /**
         * 构造小数类型。
         *
         * @param precision 总精度
         * @param scale 小数位数
         */
        public DecimalType(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }
    }

    /** 日期类型。 */
    public static final class DateType extends ScalarType {}

    /** 时间戳类型(带时区)。 */
    public static final class TimestampType extends ScalarType {}

    /** 时间戳类型(无时区)。 */
    public static final class TimestampNTZType extends ScalarType {}

    /** UUID类型。 */
    public static final class UuidType extends ScalarType {}

    // typed_value、value和metadata字段在模式中的索引。
    // 如果某个字段不在模式中,其值必须设置为-1表示无效。
    // 有效字段的索引应该从0开始并且是连续的。

    /** typed_value字段的索引,-1表示不存在。 */
    public int typedIdx;
    /** value字段的索引,-1表示不存在。 */
    public int variantIdx;
    /**
     * metadata字段的索引。
     * 仅在顶层模式中为非负值,在所有其他嵌套层级中为-1。
     */
    public final int topLevelMetadataIdx;
    /**
     * 模式中的字段数量。
     * 介于1到3之间,取决于value、typed_value和metadata中哪些存在。
     */
    public final int numFields;

    /** 标量模式(如果是标量类型)。 */
    public final ScalarType scalarSchema;
    /** 对象模式(如果是对象类型)。 */
    public final ObjectField[] objectSchema;
    /** 对象字段的快速查找映射,值是objectSchema中的索引。 */
    public final Map<String, Integer> objectSchemaMap;
    /** 数组元素的模式(如果是数组类型)。 */
    public final VariantSchema arraySchema;

    /**
     * 构造VariantSchema。
     *
     * @param typedIdx typed_value字段索引
     * @param variantIdx value字段索引
     * @param topLevelMetadataIdx metadata字段索引(仅顶层)
     * @param numFields 字段总数
     * @param scalarSchema 标量模式(可选)
     * @param objectSchema 对象模式(可选)
     * @param arraySchema 数组元素模式(可选)
     */
    public VariantSchema(
            int typedIdx,
            int variantIdx,
            int topLevelMetadataIdx,
            int numFields,
            ScalarType scalarSchema,
            ObjectField[] objectSchema,
            VariantSchema arraySchema) {
        this.typedIdx = typedIdx;
        this.numFields = numFields;
        this.variantIdx = variantIdx;
        this.topLevelMetadataIdx = topLevelMetadataIdx;
        this.scalarSchema = scalarSchema;
        this.objectSchema = objectSchema;
        if (objectSchema != null) {
            objectSchemaMap = new HashMap<>();
            for (int i = 0; i < objectSchema.length; i++) {
                objectSchemaMap.put(objectSchema[i].fieldName, i);
            }
        } else {
            objectSchemaMap = null;
        }

        this.arraySchema = arraySchema;
    }

    /**
     * 设置typed_value字段的索引。
     *
     * <p>设置typed_value索引后,会自动将variantIdx设置为-1,并更新元数据。
     *
     * @param typedIdx typed_value字段的新索引
     */
    public void setTypedIdx(int typedIdx) {
        this.typedIdx = typedIdx;
        this.variantIdx = -1;
        setMetadata();
    }

    /** 元数据字节数组。 */
    private byte[] metadata;

    /**
     * 获取元数据。
     *
     * @return 元数据字节数组
     */
    public byte[] getMetadata() {
        return metadata;
    }

    /**
     * 设置元数据。
     *
     * <p>将对象模式映射转换为JSON并生成Variant元数据。
     */
    public void setMetadata() {
        try {
            String s = new ObjectMapper().writeValueAsString(objectSchemaMap);
            metadata = GenericVariant.fromJson(s).metadata();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 判断Variant列是否未切片。
     *
     * <p>未切片意味着数据仅存储在value字段中,未进行列式优化。
     * 用户可以针对未切片的variant进行某些优化。
     *
     * @return true表示未切片,false表示已切片
     */
    public boolean isUnshredded() {
        return topLevelMetadataIdx >= 0 && variantIdx >= 0 && typedIdx < 0;
    }

    @Override
    public String toString() {
        return "VariantSchema{"
                + "typedIdx="
                + typedIdx
                + ", variantIdx="
                + variantIdx
                + ", topLevelMetadataIdx="
                + topLevelMetadataIdx
                + ", numFields="
                + numFields
                + ", scalarSchema="
                + scalarSchema
                + ", objectSchema="
                + objectSchema
                + ", arraySchema="
                + arraySchema
                + '}';
    }
}
