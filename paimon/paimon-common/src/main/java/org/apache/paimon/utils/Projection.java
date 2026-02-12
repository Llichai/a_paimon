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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.types.DataTypeRoot.ROW;

/**
 * 投影工具类,用于字段选择和重排序。
 *
 * <p>{@link Projection} 表示一个可能包含嵌套索引的列表,用于对数据类型进行投影操作。
 * 行投影包括两个主要功能:
 * <ul>
 *   <li>减少可访问的字段(字段裁剪)
 *   <li>重新排序字段(字段重排)
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>列裁剪优化 - 只读取需要的列,减少 I/O</li>
 *   <li>字段重排序 - 调整字段顺序以匹配目标 Schema</li>
 *   <li>嵌套字段访问 - 从嵌套结构中提取特定字段</li>
 *   <li>Schema 演化 - 处理 Schema 变更时的字段映射</li>
 * </ul>
 *
 * <h2>代码示例</h2>
 * <pre>{@code
 * // 示例 1: 顶层投影 - 选择和重排序字段
 * // 原始 Schema: [field0, field1, field2, field3, field4]
 * Projection proj = Projection.of(new int[]{4, 1, 0});
 * // 结果 Schema: [field4, field1, field0]
 *
 * // 示例 2: 嵌套投影 - 访问嵌套字段
 * // 假设 field0 是一个 ROW 类型,包含子字段
 * Projection nested = Projection.of(new int[][]{{0, 2, 1}});
 * // 访问路径: field0.subfield2.subsubfield1
 *
 * // 示例 3: 投影差集
 * Projection p1 = Projection.of(new int[]{4, 1, 0, 3, 2});
 * Projection p2 = Projection.of(new int[]{4, 2});
 * Projection result = p1.difference(p2); // [1, 0, 2]
 *
 * // 示例 4: 投影补集
 * Projection p = Projection.of(new int[]{4, 2});
 * Projection complement = p.complement(5); // [0, 1, 3]
 *
 * // 示例 5: 投影数组
 * String[] array = {"a", "b", "c", "d", "e"};
 * Projection proj = Projection.of(new int[]{4, 1, 0});
 * String[] projected = proj.project(array); // ["e", "b", "a"]
 * }</pre>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>使用二分查找优化差集和补集运算</li>
 *   <li>支持空投影的单例模式</li>
 *   <li>延迟计算投影结果</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>这是一个密封类(sealed class),只能通过静态工厂方法创建</li>
 *   <li>嵌套投影不能转换为顶层投影</li>
 *   <li>差集运算要求被减数不能是嵌套投影</li>
 *   <li>投影索引会在差集运算后自动重新缩放</li>
 * </ul>
 *
 * @see RowType
 * @see InternalRow
 */
public abstract class Projection {

    // sealed class - 密封类,禁止外部继承
    private Projection() {}

    /**
     * 对 RowType 应用投影,生成新的 RowType。
     *
     * @param rowType 要投影的行类型
     * @return 投影后的新行类型
     */
    public abstract RowType project(RowType rowType);

    /**
     * 对数组应用投影。
     *
     * <p>根据投影索引选择和重排数组元素。
     *
     * @param array 要投影的数组
     * @param <T> 数组元素类型
     * @return 投影后的新数组
     */
    public <T> T[] project(T[] array) {
        int[] project = toTopLevelIndexes();
        @SuppressWarnings("unchecked")
        T[] ret = (T[]) Array.newInstance(array.getClass().getComponentType(), project.length);
        for (int i = 0; i < project.length; i++) {
            ret[i] = array[project[i]];
        }
        return ret;
    }

    /**
     * 对列表应用投影。
     *
     * <p>根据投影索引选择和重排列表元素。
     *
     * @param list 要投影的列表
     * @param <T> 列表元素类型
     * @return 投影后的新列表
     */
    public <T> List<T> project(List<T> list) {
        int[] project = toTopLevelIndexes();
        List<T> ret = new ArrayList<>();
        for (int i : project) {
            ret.add(list.get(i));
        }
        return ret;
    }

    /**
     * 判断此投影是否为嵌套投影。
     *
     * <p>嵌套投影包含多层索引路径,用于访问嵌套结构中的字段。
     *
     * @return 如果是嵌套投影返回 {@code true},否则返回 {@code false}
     */
    public abstract boolean isNested();

    /**
     * 执行投影的差集运算。
     *
     * <p>该操作返回一个新的 {@link Projection},保留当前实例的顺序,
     * 但移除了 {@code other} 中的索引。索引会自动重新缩放。
     *
     * <p>示例:
     * <pre>{@code
     * [4, 1, 0, 3, 2] - [4, 2] = [1, 0, 2]
     * }</pre>
     *
     * <p>注意索引 3 在被减数中变成了 2,因为它被重新缩放以正确投影
     * 长度为 3 的 {@link InternalRow}。
     *
     * @param other 要减去的投影(减数)
     * @return 差集投影
     * @throws IllegalArgumentException 当 {@code other} 是嵌套投影时
     */
    public abstract Projection difference(Projection other);

    /**
     * 计算此投影的补集。
     *
     * <p>返回的投影是从 0 到 {@code fieldsNumber} 的有序投影,
     * 不包含此 {@link Projection} 中的索引。
     *
     * <p>示例:
     * <pre>{@code
     * [4, 2].complement(5) = [0, 1, 3]
     * }</pre>
     *
     * @param fieldsNumber 全集大小(字段总数)
     * @return 补集投影
     * @throws IllegalStateException 如果此投影是嵌套投影
     */
    public abstract Projection complement(int fieldsNumber);

    /**
     * 将此实例转换为顶层索引数组。
     *
     * <p>数组表示原始 {@link DataType} 字段的映射关系。
     * 例如,{@code [0, 2, 1]} 指定按以下顺序包含:
     * 第1个字段、第3个字段、第2个字段。
     *
     * @return 顶层索引数组
     * @throws IllegalStateException 如果此投影是嵌套投影
     */
    public abstract int[] toTopLevelIndexes();

    /**
     * 将此实例转换为嵌套投影索引路径数组。
     *
     * <p>数组表示原始 {@link DataType} 字段的映射关系,包括嵌套行。
     * 例如,{@code [[0, 2, 1], ...]} 指定包含顶层行中第1个字段的
     * 第3个字段的第2个字段。
     *
     * @return 嵌套索引路径的二维数组
     */
    public abstract int[][] toNestedIndexes();

    /**
     * 创建一个空投影。
     *
     * <p>空投影不投影任何字段,返回空的 {@link DataType}。
     *
     * @return 空投影实例(单例)
     */
    public static Projection empty() {
        return EmptyProjection.INSTANCE;
    }

    /**
     * 创建指定索引的投影。
     *
     * <p>这是顶层投影,用于选择和重排序顶层字段。
     *
     * @param indexes 字段索引数组
     * @return 投影实例
     * @see #toTopLevelIndexes()
     */
    public static Projection of(int[] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new TopLevelProjection(indexes);
    }

    /**
     * 创建指定索引路径的嵌套投影。
     *
     * <p>这是嵌套投影,用于访问嵌套结构中的字段。
     *
     * @param indexes 嵌套索引路径的二维数组
     * @return 嵌套投影实例
     * @see #toNestedIndexes()
     */
    public static Projection of(int[][] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new NestedProjection(indexes);
    }

    /**
     * 创建字段范围投影。
     *
     * <p>投影从 {@code startInclusive} 到 {@code endExclusive} 的连续字段。
     *
     * @param startInclusive 起始索引(包含)
     * @param endExclusive 结束索引(不包含)
     * @return 范围投影实例
     */
    public static Projection range(int startInclusive, int endExclusive) {
        return new TopLevelProjection(IntStream.range(startInclusive, endExclusive).toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Projection)) {
            return false;
        }
        Projection other = (Projection) o;
        if (!this.isNested() && !other.isNested()) {
            return Arrays.equals(this.toTopLevelIndexes(), other.toTopLevelIndexes());
        }
        return Arrays.deepEquals(this.toNestedIndexes(), other.toNestedIndexes());
    }

    @Override
    public int hashCode() {
        if (isNested()) {
            return Arrays.deepHashCode(toNestedIndexes());
        }
        return Arrays.hashCode(toTopLevelIndexes());
    }

    @Override
    public String toString() {
        if (isNested()) {
            return "Nested projection = " + Arrays.deepToString(toNestedIndexes());
        }
        return "Top level projection = " + Arrays.toString(toTopLevelIndexes());
    }

    /**
     * 空投影实现 - 不投影任何字段。
     *
     * <p>使用单例模式,避免重复创建实例。
     */
    private static class EmptyProjection extends Projection {

        /** 空投影单例实例 */
        static final EmptyProjection INSTANCE = new EmptyProjection();

        private EmptyProjection() {}

        @Override
        public RowType project(RowType rowType) {
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection projection) {
            return this;
        }

        @Override
        public Projection complement(int fieldsNumber) {
            return new TopLevelProjection(IntStream.range(0, fieldsNumber).toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            return new int[0];
        }

        @Override
        public int[][] toNestedIndexes() {
            return new int[0][];
        }
    }

    /**
     * 嵌套投影实现 - 支持访问嵌套结构中的字段。
     *
     * <p>每个元素是一个索引路径,表示从顶层到目标字段的访问路径。
     */
    private static class NestedProjection extends Projection {

        /** 嵌套索引路径数组 */
        final int[][] projection;

        /** 是否为真正的嵌套投影(路径长度 > 1) */
        final boolean nested;

        NestedProjection(int[][] projection) {
            this.projection = projection;
            this.nested = Arrays.stream(projection).anyMatch(arr -> arr.length > 1);
        }

        @Override
        public RowType project(RowType rowType) {
            final List<DataField> updatedFields = new ArrayList<>();
            Set<String> nameDomain = new HashSet<>();
            int duplicateCount = 0;
            for (int[] indexPath : this.projection) {
                DataField field = rowType.getFields().get(indexPath[0]);
                StringBuilder builder =
                        new StringBuilder(rowType.getFieldNames().get(indexPath[0]));
                for (int index = 1; index < indexPath.length; index++) {
                    Preconditions.checkArgument(
                            field.type().getTypeRoot() == ROW, "Row data type expected.");
                    RowType rowtype = ((RowType) field.type());
                    builder.append("_").append(rowtype.getFieldNames().get(indexPath[index]));
                    field = rowtype.getFields().get(indexPath[index]);
                }
                String path = builder.toString();
                while (nameDomain.contains(path)) {
                    path = builder.append("_$").append(duplicateCount++).toString();
                }
                updatedFields.add(field.newName(path));
                nameDomain.add(path);
            }
            return new RowType(rowType.isNullable(), updatedFields);
        }

        @Override
        public boolean isNested() {
            return nested;
        }

        @Override
        public Projection difference(Projection other) {
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between nested projection and nested projection");
            }
            if (other instanceof EmptyProjection) {
                return this;
            }
            if (!this.isNested()) {
                return new TopLevelProjection(toTopLevelIndexes()).difference(other);
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<int[]> resultProjection =
                    Arrays.stream(projection).collect(Collectors.toCollection(ArrayList::new));

            ListIterator<int[]> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int[] indexArr = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                int searchResult = Arrays.binarySearch(indexesToExclude, indexArr[0]);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1);
                    if (offset != 0) {
                        indexArr[0] = indexArr[0] - offset;
                    }
                }
            }

            return new NestedProjection(resultProjection.toArray(new int[0][]));
        }

        @Override
        public Projection complement(int fieldsNumber) {
            if (isNested()) {
                throw new IllegalStateException("Cannot perform complement of a nested projection");
            }
            return new TopLevelProjection(toTopLevelIndexes()).complement(fieldsNumber);
        }

        @Override
        public int[] toTopLevelIndexes() {
            if (isNested()) {
                throw new IllegalStateException(
                        "Cannot convert a nested projection to a top level projection");
            }
            return Arrays.stream(projection).mapToInt(arr -> arr[0]).toArray();
        }

        @Override
        public int[][] toNestedIndexes() {
            return projection;
        }
    }

    /**
     * 顶层投影实现 - 只投影顶层字段。
     *
     * <p>用于简单的字段选择和重排序,不涉及嵌套访问。
     */
    private static class TopLevelProjection extends Projection {

        /** 顶层字段索引数组 */
        final int[] projection;

        TopLevelProjection(int[] projection) {
            this.projection = projection;
        }

        @Override
        public RowType project(RowType rowType) {
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection other) {
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between top level projection and nested projection");
            }
            if (other instanceof EmptyProjection) {
                return this;
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<Integer> resultProjection =
                    Arrays.stream(projection)
                            .boxed()
                            .collect(Collectors.toCollection(ArrayList::new));

            ListIterator<Integer> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int index = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                int searchResult = Arrays.binarySearch(indexesToExclude, index);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1);
                    if (offset != 0) {
                        resultProjectionIterator.set(index - offset);
                    }
                }
            }

            return new TopLevelProjection(resultProjection.stream().mapToInt(i -> i).toArray());
        }

        @Override
        public Projection complement(int fieldsNumber) {
            int[] indexesToExclude = Arrays.copyOf(projection, projection.length);
            Arrays.sort(indexesToExclude);

            return new TopLevelProjection(
                    IntStream.range(0, fieldsNumber)
                            .filter(i -> Arrays.binarySearch(indexesToExclude, i) < 0)
                            .toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            return projection;
        }

        @Override
        public int[][] toNestedIndexes() {
            return Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        }
    }
}
