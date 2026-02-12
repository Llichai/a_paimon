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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * {@link DataType} 的访问者模式定义。
 *
 * <p>访问者模式将数据类型转换为 {@code R} 类型的实例。这是一种经典的设计模式,
 * 用于在不修改数据类型类的情况下添加新的操作。
 *
 * <h2>访问者模式的优势</h2>
 * <ul>
 *   <li>开闭原则 - 可以在不修改类型类的情况下添加新操作
 *   <li>类型安全 - 编译时检查所有类型是否都被处理
 *   <li>集中逻辑 - 相关操作集中在一个访问者类中
 *   <li>易于扩展 - 添加新操作只需实现新的访问者
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 实现一个访问者来提取类型的默认值
 * class DefaultValueExtractor implements DataTypeVisitor<Object> {
 *     @Override
 *     public Object visit(IntType intType) {
 *         return 0;
 *     }
 *
 *     @Override
 *     public Object visit(VarCharType varCharType) {
 *         return "";
 *     }
 *
 *     // ... 实现其他方法
 * }
 *
 * // 使用访问者
 * DataType type = new IntType();
 * DefaultValueExtractor visitor = new DefaultValueExtractor();
 * Object defaultValue = type.accept(visitor);
 * }</pre>
 *
 * <h2>常见的访问者实现</h2>
 * <ul>
 *   <li>{@link DataTypeDefaultVisitor} - 提供默认实现的抽象访问者
 *   <li>类型转换访问者 - 将一种类型转换为另一种
 *   <li>验证访问者 - 验证类型的有效性
 *   <li>序列化访问者 - 序列化类型定义
 * </ul>
 *
 * @param <R> 访问结果的类型
 * @since 0.4.0
 */
@Public
public interface DataTypeVisitor<R> {

    /**
     * 访问 CHAR 类型。
     *
     * @param charType CHAR 类型实例
     * @return 访问结果
     */
    R visit(CharType charType);

    /**
     * 访问 VARCHAR 类型。
     *
     * @param varCharType VARCHAR 类型实例
     * @return 访问结果
     */
    R visit(VarCharType varCharType);

    /**
     * 访问 BOOLEAN 类型。
     *
     * @param booleanType BOOLEAN 类型实例
     * @return 访问结果
     */
    R visit(BooleanType booleanType);

    /**
     * 访问 BINARY 类型。
     *
     * @param binaryType BINARY 类型实例
     * @return 访问结果
     */
    R visit(BinaryType binaryType);

    /**
     * 访问 VARBINARY 类型。
     *
     * @param varBinaryType VARBINARY 类型实例
     * @return 访问结果
     */
    R visit(VarBinaryType varBinaryType);

    /**
     * 访问 DECIMAL 类型。
     *
     * @param decimalType DECIMAL 类型实例
     * @return 访问结果
     */
    R visit(DecimalType decimalType);

    /**
     * 访问 TINYINT 类型。
     *
     * @param tinyIntType TINYINT 类型实例
     * @return 访问结果
     */
    R visit(TinyIntType tinyIntType);

    /**
     * 访问 SMALLINT 类型。
     *
     * @param smallIntType SMALLINT 类型实例
     * @return 访问结果
     */
    R visit(SmallIntType smallIntType);

    /**
     * 访问 INT 类型。
     *
     * @param intType INT 类型实例
     * @return 访问结果
     */
    R visit(IntType intType);

    /**
     * 访问 BIGINT 类型。
     *
     * @param bigIntType BIGINT 类型实例
     * @return 访问结果
     */
    R visit(BigIntType bigIntType);

    /**
     * 访问 FLOAT 类型。
     *
     * @param floatType FLOAT 类型实例
     * @return 访问结果
     */
    R visit(FloatType floatType);

    /**
     * 访问 DOUBLE 类型。
     *
     * @param doubleType DOUBLE 类型实例
     * @return 访问结果
     */
    R visit(DoubleType doubleType);

    /**
     * 访问 DATE 类型。
     *
     * @param dateType DATE 类型实例
     * @return 访问结果
     */
    R visit(DateType dateType);

    /**
     * 访问 TIME 类型。
     *
     * @param timeType TIME 类型实例
     * @return 访问结果
     */
    R visit(TimeType timeType);

    /**
     * 访问 TIMESTAMP 类型。
     *
     * @param timestampType TIMESTAMP 类型实例
     * @return 访问结果
     */
    R visit(TimestampType timestampType);

    /**
     * 访问本地时区 TIMESTAMP 类型。
     *
     * @param localZonedTimestampType 本地时区 TIMESTAMP 类型实例
     * @return 访问结果
     */
    R visit(LocalZonedTimestampType localZonedTimestampType);

    /**
     * 访问 VARIANT 类型。
     *
     * @param variantType VARIANT 类型实例
     * @return 访问结果
     */
    R visit(VariantType variantType);

    /**
     * 访问 BLOB 类型。
     *
     * @param blobType BLOB 类型实例
     * @return 访问结果
     */
    R visit(BlobType blobType);

    /**
     * 访问 ARRAY 类型。
     *
     * @param arrayType ARRAY 类型实例
     * @return 访问结果
     */
    R visit(ArrayType arrayType);

    /**
     * 访问 MULTISET 类型。
     *
     * @param multisetType MULTISET 类型实例
     * @return 访问结果
     */
    R visit(MultisetType multisetType);

    /**
     * 访问 MAP 类型。
     *
     * @param mapType MAP 类型实例
     * @return 访问结果
     */
    R visit(MapType mapType);

    /**
     * 访问 ROW 类型。
     *
     * @param rowType ROW 类型实例
     * @return 访问结果
     */
    R visit(RowType rowType);
}
