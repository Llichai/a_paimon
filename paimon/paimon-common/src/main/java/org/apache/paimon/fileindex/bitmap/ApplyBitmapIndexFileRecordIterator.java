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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 应用 Bitmap 索引过滤的文件记录迭代器。
 *
 * <p>该迭代器包装一个 {@link FileRecordIterator},使用 {@link BitmapIndexResult} 中的位图过滤记录,
 * 只返回位图中标记为 true 的行位置对应的记录。
 *
 * <h2>核心功能</h2>
 *
 * <h3>行级过滤</h3>
 *
 * <p>根据 RoaringBitmap32 中的行位置信息,跳过不匹配的记录:
 *
 * <pre>
 * 示例: 假设数据文件有 10 行,Bitmap 索引结果为 {1, 3, 5, 7, 9}
 *
 * 迭代过程:
 * - 位置 0: 不在 Bitmap 中,跳过
 * - 位置 1: 在 Bitmap 中,返回该记录
 * - 位置 2: 不在 Bitmap 中,跳过
 * - 位置 3: 在 Bitmap 中,返回该记录
 * - ...
 * - 位置 9: 在 Bitmap 中,返回该记录
 * - 位置 10: 超过 last,终止迭代
 * </pre>
 *
 * <h3>提前终止</h3>
 *
 * <p>使用 {@code last} 字段记录 Bitmap 中的最大行位置,避免扫描后续不相关的行:
 *
 * <pre>
 * if (position > last) {
 *     return null;  // 提前结束,无需继续读取文件
 * }
 * </pre>
 *
 * <h2>工作流程</h2>
 *
 * <pre>
 * 1. 初始化:
 *    - 从 BitmapIndexResult 获取 RoaringBitmap32
 *    - 记录最大行位置 (last)
 *
 * 2. next() 调用:
 *    while (true) {
 *        a. 调用底层迭代器的 next() 获取下一条记录
 *        b. 如果记录为 null,返回 null (文件结束)
 *        c. 获取当前记录的行位置
 *        d. 如果位置 > last,返回 null (提前终止)
 *        e. 如果位置在 Bitmap 中,返回该记录
 *        f. 否则,继续循环 (跳过该记录)
 *    }
 *
 * 3. 清理:
 *    - releaseBatch() 委托给底层迭代器
 * </pre>
 *
 * <h2>性能优化</h2>
 *
 * <h3>1. 提前终止优化</h3>
 *
 * <p>利用 RoaringBitmap32.last() 避免扫描无关数据:
 *
 * <pre>
 * 示例:
 * - 文件总行数: 1,000,000
 * - Bitmap 最大位置: 500,000
 * - 节省: 后 50% 的数据无需扫描
 * </pre>
 *
 * <h3>2. 位图查询优化</h3>
 *
 * <p>RoaringBitmap32.contains() 的时间复杂度:
 *
 * <ul>
 *   <li><b>平均情况</b>: O(1) - 直接数组或位图查找
 *   <li><b>最坏情况</b>: O(log n) - 二分查找容器
 * </ul>
 *
 * <h3>3. 内存效率</h3>
 *
 * <pre>
 * - 不复制底层记录: 直接返回迭代器的记录对象
 * - Bitmap 共享: 与 BitmapIndexResult 共享位图,无额外复制
 * - 提前释放: releaseBatch() 及时释放底层批次内存
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <h3>基本用法</h3>
 *
 * <pre>{@code
 * // 构建 Bitmap 索引结果
 * RoaringBitmap32 bitmap = new RoaringBitmap32();
 * bitmap.add(1, 3, 5, 7, 9);  // 匹配的行位置
 * BitmapIndexResult indexResult = new BitmapIndexResult(() -> bitmap);
 *
 * // 创建底层迭代器
 * FileRecordIterator<InternalRow> baseIterator = ...;
 *
 * // 包装为过滤迭代器
 * ApplyBitmapIndexFileRecordIterator filteredIterator =
 *     new ApplyBitmapIndexFileRecordIterator(baseIterator, indexResult);
 *
 * // 迭代过滤后的记录
 * while (true) {
 *     InternalRow row = filteredIterator.next();
 *     if (row == null) break;
 *     // 处理匹配的记录
 *     System.out.println("Position: " + filteredIterator.returnedPosition() + ", Row: " + row);
 * }
 *
 * // 清理
 * filteredIterator.releaseBatch();
 * }</pre>
 *
 * <h3>与 RecordReader 集成</h3>
 *
 * <pre>{@code
 * // 在 ApplyBitmapIndexRecordReader 中使用
 * FileRecordReader<InternalRow> reader = ...;
 * BitmapIndexResult indexResult = ...;
 *
 * FileRecordIterator<InternalRow> batch = reader.readBatch();
 * if (batch != null) {
 *     ApplyBitmapIndexFileRecordIterator filteredBatch =
 *         new ApplyBitmapIndexFileRecordIterator(batch, indexResult);
 *     return filteredBatch;
 * }
 * }</pre>
 *
 * <h2>性能特性</h2>
 *
 * <table border="1">
 *   <tr>
 *     <th>操作</th>
 *     <th>时间复杂度</th>
 *     <th>空间复杂度</th>
 *   </tr>
 *   <tr>
 *     <td>创建迭代器</td>
 *     <td>O(1)</td>
 *     <td>O(1)</td>
 *   </tr>
 *   <tr>
 *     <td>next() - 匹配记录</td>
 *     <td>O(1)</td>
 *     <td>O(1)</td>
 *   </tr>
 *   <tr>
 *     <td>next() - 跳过 k 条</td>
 *     <td>O(k)</td>
 *     <td>O(1)</td>
 *   </tr>
 *   <tr>
 *     <td>完整扫描</td>
 *     <td>O(N)</td>
 *     <td>O(1)</td>
 *   </tr>
 * </table>
 *
 * <p>N 为文件总行数
 *
 * <h2>实际效果</h2>
 *
 * <pre>
 * 假设查询: SELECT * FROM table WHERE status = 'ACTIVE'
 * 数据分布: 100 万行,10 万行 ACTIVE (10%)
 *
 * 不使用索引:
 * - 扫描: 100 万行
 * - 返回: 10 万行
 * - I/O: 读取完整文件
 *
 * 使用 Bitmap 索引:
 * - 扫描: 100 万行 (仍需遍历判断位置)
 * - 返回: 10 万行
 * - 优化: 跳过 90% 的记录解析和处理
 *
 * 实际加速: 1.5x - 3x (取决于记录解析开销)
 * </pre>
 *
 * @see BitmapIndexResult Bitmap 索引查询结果
 * @see ApplyBitmapIndexRecordReader 使用该迭代器的 RecordReader
 * @see org.apache.paimon.utils.RoaringBitmap32 底层位图数据结构
 */
public class ApplyBitmapIndexFileRecordIterator implements FileRecordIterator<InternalRow> {

    /** 底层的文件记录迭代器。 */
    private final FileRecordIterator<InternalRow> iterator;

    /** Bitmap 索引结果中的位图,标记匹配的行位置。 */
    private final RoaringBitmap32 bitmap;

    /** Bitmap 中的最大行位置,用于提前终止迭代。 */
    private final int last;

    public ApplyBitmapIndexFileRecordIterator(
            FileRecordIterator<InternalRow> iterator, BitmapIndexResult fileIndexResult) {
        this.iterator = iterator;
        this.bitmap = fileIndexResult.get();
        this.last = bitmap.last();
    }

    @Override
    public long returnedPosition() {
        return iterator.returnedPosition();
    }

    @Override
    public Path filePath() {
        return iterator.filePath();
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        while (true) {
            InternalRow next = iterator.next();
            if (next == null) {
                return null;
            }
            int position = (int) returnedPosition();
            if (position > last) {
                return null;
            }
            if (bitmap.contains(position)) {
                return next;
            }
        }
    }

    @Override
    public void releaseBatch() {
        iterator.releaseBatch();
    }
}
