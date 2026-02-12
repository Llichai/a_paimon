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
 * {@link DataTypeVisitor} 的默认实现,将所有调用重定向到 {@link DataTypeDefaultVisitor#defaultMethod(DataType)}。
 *
 * <p>这个抽象类提供了一个方便的基类,用于实现不需要区分所有数据类型的访问者。
 * 子类可以只重写关心的特定类型的访问方法,而其他类型将使用默认方法处理。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>只需要特殊处理少数几种类型
 *   <li>大多数类型共享相同的处理逻辑
 *   <li>简化访问者的实现
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个访问者,只特殊处理数值类型,其他类型返回 null
 * class NumericTypeExtractor extends DataTypeDefaultVisitor<String> {
 *     @Override
 *     public String visit(IntType intType) {
 *         return "INTEGER";
 *     }
 *
 *     @Override
 *     public String visit(BigIntType bigIntType) {
 *         return "BIGINT";
 *     }
 *
 *     @Override
 *     protected String defaultMethod(DataType dataType) {
 *         return null; // 非数值类型返回 null
 *     }
 * }
 * }</pre>
 *
 * <h2>实现提示</h2>
 * <ul>
 *   <li>重写 {@link #defaultMethod(DataType)} 提供默认行为
 *   <li>只重写需要特殊处理的类型的 visit 方法
 *   <li>可以在默认方法中抛出异常表示不支持的类型
 * </ul>
 *
 * @param <R> 访问结果的类型
 * @since 0.4.0
 */
@Public
public abstract class DataTypeDefaultVisitor<R> implements DataTypeVisitor<R> {

    /** 访问 CHAR 类型,委托给默认方法。 */
    @Override
    public R visit(CharType charType) {
        return defaultMethod(charType);
    }

    /** 访问 VARCHAR 类型,委托给默认方法。 */
    @Override
    public R visit(VarCharType varCharType) {
        return defaultMethod(varCharType);
    }

    /** 访问 BOOLEAN 类型,委托给默认方法。 */
    @Override
    public R visit(BooleanType booleanType) {
        return defaultMethod(booleanType);
    }

    /** 访问 BINARY 类型,委托给默认方法。 */
    @Override
    public R visit(BinaryType binaryType) {
        return defaultMethod(binaryType);
    }

    /** 访问 VARBINARY 类型,委托给默认方法。 */
    @Override
    public R visit(VarBinaryType varBinaryType) {
        return defaultMethod(varBinaryType);
    }

    /** 访问 DECIMAL 类型,委托给默认方法。 */
    @Override
    public R visit(DecimalType decimalType) {
        return defaultMethod(decimalType);
    }

    /** 访问 TINYINT 类型,委托给默认方法。 */
    @Override
    public R visit(TinyIntType tinyIntType) {
        return defaultMethod(tinyIntType);
    }

    /** 访问 SMALLINT 类型,委托给默认方法。 */
    @Override
    public R visit(SmallIntType smallIntType) {
        return defaultMethod(smallIntType);
    }

    /** 访问 INT 类型,委托给默认方法。 */
    @Override
    public R visit(IntType intType) {
        return defaultMethod(intType);
    }

    /** 访问 BIGINT 类型,委托给默认方法。 */
    @Override
    public R visit(BigIntType bigIntType) {
        return defaultMethod(bigIntType);
    }

    /** 访问 FLOAT 类型,委托给默认方法。 */
    @Override
    public R visit(FloatType floatType) {
        return defaultMethod(floatType);
    }

    /** 访问 DOUBLE 类型,委托给默认方法。 */
    @Override
    public R visit(DoubleType doubleType) {
        return defaultMethod(doubleType);
    }

    /** 访问 DATE 类型,委托给默认方法。 */
    @Override
    public R visit(DateType dateType) {
        return defaultMethod(dateType);
    }

    /** 访问 TIME 类型,委托给默认方法。 */
    @Override
    public R visit(TimeType timeType) {
        return defaultMethod(timeType);
    }

    /** 访问 TIMESTAMP 类型,委托给默认方法。 */
    @Override
    public R visit(TimestampType timestampType) {
        return defaultMethod(timestampType);
    }

    /** 访问本地时区 TIMESTAMP 类型,委托给默认方法。 */
    @Override
    public R visit(LocalZonedTimestampType localZonedTimestampType) {
        return defaultMethod(localZonedTimestampType);
    }

    /** 访问 VARIANT 类型,委托给默认方法。 */
    @Override
    public R visit(VariantType variantType) {
        return defaultMethod(variantType);
    }

    /** 访问 BLOB 类型,委托给默认方法。 */
    @Override
    public R visit(BlobType blobType) {
        return defaultMethod(blobType);
    }

    /** 访问 ARRAY 类型,委托给默认方法。 */
    @Override
    public R visit(ArrayType arrayType) {
        return defaultMethod(arrayType);
    }

    /** 访问 MULTISET 类型,委托给默认方法。 */
    @Override
    public R visit(MultisetType multisetType) {
        return defaultMethod(multisetType);
    }

    /** 访问 MAP 类型,委托给默认方法。 */
    @Override
    public R visit(MapType mapType) {
        return defaultMethod(mapType);
    }

    /** 访问 ROW 类型,委托给默认方法。 */
    @Override
    public R visit(RowType rowType) {
        return defaultMethod(rowType);
    }

    /**
     * 默认访问方法,由所有类型的 visit 方法调用。
     *
     * <p>子类应重写此方法以提供默认行为,或为特定类型重写对应的 visit 方法。
     *
     * @param dataType 被访问的数据类型
     * @return 访问结果
     */
    protected abstract R defaultMethod(DataType dataType);
}
