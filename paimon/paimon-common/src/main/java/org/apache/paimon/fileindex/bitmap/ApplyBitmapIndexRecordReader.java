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
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 应用 Bitmap 索引过滤的记录读取器。
 *
 * <p>该读取器包装一个 {@link FileRecordReader},将其返回的每个批次(batch)用 {@link BitmapIndexResult}
 * 进行过滤,只返回匹配位图条件的记录。
 *
 * <h2>核心功能</h2>
 *
 * <h3>批次级过滤</h3>
 *
 * <p>为底层 RecordReader 返回的每个批次创建一个 {@link ApplyBitmapIndexFileRecordIterator}:
 *
 * <pre>
 * FileRecordIterator<InternalRow> batch = reader.readBatch();
 * if (batch != null) {
 *     return new ApplyBitmapIndexFileRecordIterator(batch, fileIndexResult);
 * }
 * </pre>
 *
 * <h3>透明过滤</h3>
 *
 * <p>对于调用者来说,该 RecordReader 的行为与普通 RecordReader 完全相同,只是返回的记录已经过滤:
 *
 * <pre>
 * // 使用方式与普通 RecordReader 相同
 * while (true) {
 *     FileRecordIterator<InternalRow> batch = reader.readBatch();
 *     if (batch == null) break;
 *
 *     while (true) {
 *         InternalRow row = batch.next();
 *         if (row == null) break;
 *         // 这里的 row 已经过 Bitmap 索引过滤
 *     }
 * }
 * </pre>
 *
 * <h2>工作流程</h2>
 *
 * <pre>
 * 1. 初始化:
 *    - 保存底层 RecordReader
 *    - 保存 BitmapIndexResult
 *
 * 2. readBatch():
 *    a. 调用底层 reader.readBatch() 获取批次
 *    b. 如果批次为 null,返回 null (文件结束)
 *    c. 创建 ApplyBitmapIndexFileRecordIterator 包装批次
 *    d. 返回包装后的迭代器
 *
 * 3. close():
 *    - 关闭底层 RecordReader
 * </pre>
 *
 * <h2>使用场景</h2>
 *
 * <h3>1. 查询优化</h3>
 *
 * <p>在读取数据文件时应用 Bitmap 索引过滤,减少不必要的记录处理:
 *
 * <pre>{@code
 * // 查询: SELECT * FROM table WHERE city = 'Beijing'
 *
 * // 1. 查询 Bitmap 索引
 * BitmapFileIndex bitmapIndex = ...;
 * BitmapIndexResult result = bitmapIndex.visitEqual(
 *     new FieldRef(0, "city", DataTypes.STRING()),
 *     BinaryString.fromString("Beijing")
 * );
 *
 * // 2. 创建过滤读取器
 * FileRecordReader<InternalRow> baseReader = createRecordReader(dataFile);
 * ApplyBitmapIndexRecordReader filteredReader =
 *     new ApplyBitmapIndexRecordReader(baseReader, result);
 *
 * // 3. 读取过滤后的数据
 * while (true) {
 *     FileRecordIterator<InternalRow> batch = filteredReader.readBatch();
 *     if (batch == null) break;
 *
 *     while (true) {
 *         InternalRow row = batch.next();
 *         if (row == null) break;
 *         // 处理北京的记录
 *     }
 *     batch.releaseBatch();
 * }
 *
 * filteredReader.close();
 * }</pre>
 *
 * <h3>2. 多条件组合</h3>
 *
 * <p>可以组合多个 Bitmap 索引结果:
 *
 * <pre>{@code
 * // 查询: SELECT * FROM table WHERE city = 'Beijing' AND status = 'ACTIVE'
 *
 * // 查询两个索引
 * BitmapIndexResult cityResult = cityIndex.visitEqual(..., "Beijing");
 * BitmapIndexResult statusResult = statusIndex.visitEqual(..., "ACTIVE");
 *
 * // 组合结果 (AND 操作)
 * BitmapIndexResult combined = cityResult.and(statusResult);
 *
 * // 应用组合过滤
 * ApplyBitmapIndexRecordReader filteredReader =
 *     new ApplyBitmapIndexRecordReader(baseReader, combined);
 * }</pre>
 *
 * <h2>性能特性</h2>
 *
 * <h3>开销分析</h3>
 *
 * <table border="1">
 *   <tr>
 *     <th>组件</th>
 *     <th>开销</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>包装批次</td>
 *     <td>O(1)</td>
 *     <td>每个批次创建一个迭代器对象</td>
 *   </tr>
 *   <tr>
 *     <td>位图查询</td>
 *     <td>O(1) 每行</td>
 *     <td>RoaringBitmap32.contains()</td>
 *   </tr>
 *   <tr>
 *     <td>内存开销</td>
 *     <td>O(1)</td>
 *     <td>只包装,不复制数据</td>
 *   </tr>
 * </table>
 *
 * <h3>性能提升</h3>
 *
 * <pre>
 * 场景: 选择率 10% (100 万行中 10 万行匹配)
 *
 * 不使用索引:
 * - 解析: 100 万行
 * - 过滤: 100 万次谓词评估
 * - 返回: 10 万行
 *
 * 使用 Bitmap 索引:
 * - 解析: 100 万行 (仍需遍历文件)
 * - 过滤: 100 万次位图查询 (更快)
 * - 返回: 10 万行
 *
 * 优势:
 * - 避免复杂的谓词评估 (如字符串比较、函数调用)
 * - 减少后续处理的数据量
 * - 提前终止扫描 (如果 Bitmap 有明确的 last 位置)
 * </pre>
 *
 * <h2>限制和注意事项</h2>
 *
 * <h3>1. 仍需扫描文件</h3>
 *
 * <p>Bitmap 索引不支持随机访问,仍需顺序扫描文件:
 *
 * <pre>
 * - Bitmap 只提供"行 X 是否匹配"的信息
 * - 不提供"行 X 在文件中的字节偏移"
 * - 因此必须顺序读取所有行,判断位置是否在 Bitmap 中
 * </pre>
 *
 * <h3>2. 最适合中等选择率</h3>
 *
 * <pre>
 * 选择率 < 1%:   考虑使用其他索引 (如 B+树)
 * 选择率 1-50%:  Bitmap 索引最优
 * 选择率 > 90%:  索引效果有限
 * </pre>
 *
 * <h3>3. 与批次大小的关系</h3>
 *
 * <pre>
 * - 批次越大: 包装开销越低 (摊薄)
 * - 批次越小: 更早释放内存,但包装开销相对高
 * - 推荐批次大小: 1024 - 4096 行
 * </pre>
 *
 * <h2>与其他组件的集成</h2>
 *
 * <pre>
 * ApplyBitmapIndexRecordReader
 *   ↓ 包装
 * FileRecordReader (ORC/Parquet/Paimon 格式)
 *   ↓ 返回批次
 * FileRecordIterator
 *   ↓ 包装
 * ApplyBitmapIndexFileRecordIterator
 *   ↓ 过滤
 * InternalRow (只返回匹配的行)
 * </pre>
 *
 * @see ApplyBitmapIndexFileRecordIterator 批次级的过滤迭代器
 * @see BitmapIndexResult Bitmap 索引查询结果
 * @see FileRecordReader 底层的记录读取器
 */
public class ApplyBitmapIndexRecordReader implements FileRecordReader<InternalRow> {

    /** 底层的文件记录读取器。 */
    private final FileRecordReader<InternalRow> reader;

    /** Bitmap 索引过滤结果。 */
    private final BitmapIndexResult fileIndexResult;

    public ApplyBitmapIndexRecordReader(
            FileRecordReader<InternalRow> reader, BitmapIndexResult fileIndexResult) {
        this.reader = reader;
        this.fileIndexResult = fileIndexResult;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> batch = reader.readBatch();
        if (batch == null) {
            return null;
        }

        return new ApplyBitmapIndexFileRecordIterator(batch, fileIndexResult);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
