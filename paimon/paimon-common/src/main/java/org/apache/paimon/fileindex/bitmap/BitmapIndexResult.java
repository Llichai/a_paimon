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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringBitmap32;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Bitmap 索引过滤结果。
 *
 * <p>基于 {@link RoaringBitmap32} 的索引查询结果,支持精确的行号级别过滤和高效的位图运算。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li>延迟计算:使用 {@link LazyField} 延迟计算位图,避免不必要的开销</li>
 *   <li>精确结果:位图明确标识哪些行满足条件,无误判</li>
 *   <li>高效运算:支持 AND、OR、AND_NOT 等位图运算</li>
 * </ul>
 *
 * <h3>位图运算</h3>
 * <ul>
 *   <li>{@link #and(FileIndexResult)}: AND 运算,两个条件都满足</li>
 *   <li>{@link #or(FileIndexResult)}: OR 运算,任一条件满足</li>
 *   <li>{@link #andNot(RoaringBitmap32)}: AND_NOT 运算,排除指定行</li>
 *   <li>{@link #limit(int)}: 限制结果行数</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 查询 status = 'ACTIVE'
 * BitmapIndexResult result1 = reader.visitEqual(statusField, "ACTIVE");
 *
 * // 查询 city IN ('Beijing', 'Shanghai')
 * BitmapIndexResult result2 = reader.visitIn(cityField, Arrays.asList("Beijing", "Shanghai"));
 *
 * // 组合查询: status = 'ACTIVE' AND city IN (...)
 * BitmapIndexResult combined = result1.and(result2);
 *
 * // 排除已删除的行
 * BitmapIndexResult final = combined.andNot(deletionBitmap);
 *
 * // 限制返回前100行
 * BitmapIndexResult limited = final.limit(100);
 * }</pre>
 *
 * <h3>性能特性</h3>
 * <ul>
 *   <li>空间复杂度: O(n/8),RoaringBitmap 压缩后通常更小</li>
 *   <li>时间复杂度:
 *     <ul>
 *       <li>AND/OR/AND_NOT: O(n),n 为位图大小</li>
 *       <li>remain(): O(1),检查位图是否为空</li>
 *       <li>limit(k): O(k),遍历前 k 个元素</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * @see RoaringBitmap32
 * @see BitmapFileIndex
 */
/** bitmap file index result. */
public class BitmapIndexResult extends LazyField<RoaringBitmap32> implements FileIndexResult {

    /**
     * 构造 Bitmap 索引结果。
     *
     * @param supplier 位图供应器,支持延迟计算
     */
    public BitmapIndexResult(Supplier<RoaringBitmap32> supplier) {
        super(supplier);
    }

    /**
     * 是否需要保留文件。
     *
     * <p>当位图非空时返回 true,表示有行满足条件。
     *
     * @return true 如果位图非空,需要扫描文件
     */
    @Override
    public boolean remain() {
        return !get().isEmpty();
    }

    /**
     * AND 运算。
     *
     * <p>如果另一个结果也是 BitmapIndexResult,执行位图 AND 运算;
     * 否则使用默认的逻辑 AND。
     *
     * @param fileIndexResult 另一个过滤结果
     * @return AND 运算后的结果
     */
    @Override
    public FileIndexResult and(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof BitmapIndexResult) {
            return new BitmapIndexResult(
                    () -> RoaringBitmap32.and(get(), ((BitmapIndexResult) fileIndexResult).get()));
        }
        return FileIndexResult.super.and(fileIndexResult);
    }

    /**
     * OR 运算。
     *
     * <p>如果另一个结果也是 BitmapIndexResult,执行位图 OR 运算;
     * 否则使用默认的逻辑 OR。
     *
     * @param fileIndexResult 另一个过滤结果
     * @return OR 运算后的结果
     */
    @Override
    public FileIndexResult or(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof BitmapIndexResult) {
            return new BitmapIndexResult(
                    () -> RoaringBitmap32.or(get(), ((BitmapIndexResult) fileIndexResult).get()));
        }
        return FileIndexResult.super.or(fileIndexResult);
    }

    /**
     * AND_NOT 运算。
     *
     * <p>从当前位图中移除 deletion 位图中的元素,常用于排除已删除的行。
     *
     * @param deletion 要排除的行号位图
     * @return AND_NOT 运算后的结果
     */
    public BitmapIndexResult andNot(RoaringBitmap32 deletion) {
        return new BitmapIndexResult(() -> RoaringBitmap32.andNot(get(), deletion));
    }

    /**
     * 限制结果行数。
     *
     * <p>只保留前 limit 个满足条件的行。
     *
     * @param limit 最大行数
     * @return 限制后的结果
     */
    public FileIndexResult limit(int limit) {
        return new BitmapIndexResult(() -> get().limit(limit));
    }

    /**
     * 判断两个结果是否相等。
     *
     * <p>比较底层的位图是否相同。
     *
     * @param o 要比较的对象
     * @return true 如果位图相同
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitmapIndexResult that = (BitmapIndexResult) o;
        return Objects.equals(this.get(), that.get());
    }
}
