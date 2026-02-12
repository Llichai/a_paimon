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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 根据给定的字段 ID 重新分配字段 ID 的访问者。
 *
 * <p>这个访问者用于为数据类型中的所有字段重新分配唯一的字段 ID。
 * 这在模式演化、类型复制或需要确保字段 ID 唯一性的场景中非常有用。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li>递归处理 - 处理嵌套的复杂类型
 *   <li>ID 自增 - 使用 AtomicInteger 确保 ID 唯一性
 *   <li>结构保持 - 保持原有类型结构,只更新 ID
 *   <li>深度复制 - 创建新的类型实例而不是修改原有实例
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>模式演化时重新分配字段 ID
 *   <li>合并多个表结构时避免 ID 冲突
 *   <li>从外部系统导入类型定义
 *   <li>类型克隆时生成新的 ID
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个包含多个字段的 Row 类型
 * RowType originalType = RowType.builder()
 *     .field("id", new IntType())
 *     .field("name", new VarCharType())
 *     .build();
 *
 * // 重新分配字段 ID,从 0 开始
 * AtomicInteger fieldId = new AtomicInteger(-1);
 * DataType reassignedType = ReassignFieldId.reassign(originalType, fieldId);
 * }</pre>
 *
 * @see DataTypeDefaultVisitor
 */
public class ReassignFieldId extends DataTypeDefaultVisitor<DataType> {

    /** 用于生成新字段 ID 的原子计数器。 */
    private final AtomicInteger fieldId;

    /**
     * 创建字段 ID 重分配访问者。
     *
     * @param fieldId 字段 ID 计数器,用于生成新的字段 ID
     */
    public ReassignFieldId(AtomicInteger fieldId) {
        this.fieldId = fieldId;
    }

    /**
     * 为给定的数据类型重新分配字段 ID。
     *
     * <p>这是一个静态工具方法,用于简化字段 ID 重分配的操作。
     *
     * @param input 输入的数据类型
     * @param fieldId 字段 ID 计数器,用于生成新的字段 ID
     * @return 重新分配字段 ID 后的新数据类型
     */
    public static DataType reassign(DataType input, AtomicInteger fieldId) {
        return input.accept(new ReassignFieldId(fieldId));
    }

    /**
     * 访问 ARRAY 类型,递归重分配元素类型的字段 ID。
     *
     * @param arrayType 数组类型
     * @return 重新分配字段 ID 后的新数组类型
     */
    @Override
    public DataType visit(ArrayType arrayType) {
        return new ArrayType(arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    /**
     * 访问 MULTISET 类型,递归重分配元素类型的字段 ID。
     *
     * @param multisetType 多重集类型
     * @return 重新分配字段 ID 后的新多重集类型
     */
    @Override
    public DataType visit(MultisetType multisetType) {
        return new MultisetType(
                multisetType.isNullable(), multisetType.getElementType().accept(this));
    }

    /**
     * 访问 MAP 类型,递归重分配键类型和值类型的字段 ID。
     *
     * @param mapType Map 类型
     * @return 重新分配字段 ID 后的新 Map 类型
     */
    @Override
    public DataType visit(MapType mapType) {
        return new MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    /**
     * 访问 ROW 类型,为每个字段重新分配字段 ID。
     *
     * <p>这是最复杂的情况,因为 ROW 类型包含多个命名字段,
     * 每个字段都需要新的唯一 ID。
     *
     * @param rowType 行类型
     * @return 重新分配字段 ID 后的新行类型
     */
    @Override
    public DataType visit(RowType rowType) {
        RowType.Builder builder = RowType.builder(rowType.isNullable(), fieldId);
        rowType.getFields()
                .forEach(
                        f ->
                                builder.field(
                                        f.name(),
                                        f.type().accept(this),
                                        f.description(),
                                        f.defaultValue()));
        return builder.build();
    }

    /**
     * 默认方法,对于简单类型直接返回原类型。
     *
     * <p>简单类型(如 INT, VARCHAR 等)不包含字段,因此不需要重分配 ID。
     *
     * @param dataType 数据类型
     * @return 原数据类型
     */
    @Override
    protected DataType defaultMethod(DataType dataType) {
        return dataType;
    }
}
