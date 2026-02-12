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

package org.apache.paimon.fileindex.bsi;

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

/**
 * BitSliceIndex(位切片索引)文件索引工厂。
 *
 * <p>该工厂负责创建 {@link BitSliceIndexBitmapFileIndex} 实例,用于构建和查询基于位切片的 Bitmap 索引。
 *
 * <h2>BSI 索引概述</h2>
 *
 * <p>BitSliceIndex(BSI)是一种高效的数值索引技术,将数值按位切片存储为多个 Bitmap。
 * 相比传统的 Bitmap 索引,BSI 在处理数值范围查询和聚合计算时具有显著优势。
 *
 * <h3>核心特性</h3>
 *
 * <ul>
 *   <li><b>范围查询优化</b>: 时间复杂度 O(log V),V 为值域大小
 *   <li><b>聚合计算</b>: 支持 SUM、AVG、MIN、MAX 等聚合函数
 *   <li><b>空间高效</b>: 空间复杂度 O(N × log V),使用 RoaringBitmap 压缩
 *   <li><b>TopK 查询</b>: 直接从索引计算 TopK 结果,无需扫描原始数据
 * </ul>
 *
 * <h2>适用场景</h2>
 *
 * <table border="1">
 *   <tr>
 *     <th>查询类型</th>
 *     <th>BSI 性能</th>
 *     <th>推荐度</th>
 *   </tr>
 *   <tr>
 *     <td>范围查询 (age > 30 AND age < 50)</td>
 *     <td>O(log V)</td>
 *     <td>⭐⭐⭐⭐⭐</td>
 *   </tr>
 *   <tr>
 *     <td>聚合计算 (SUM(amount), AVG(price))</td>
 *     <td>O(N)</td>
 *     <td>⭐⭐⭐⭐⭐</td>
 *   </tr>
 *   <tr>
 *     <td>TopK 查询 (TOP 10 BY price)</td>
 *     <td>O(K × log V)</td>
 *     <td>⭐⭐⭐⭐⭐</td>
 *   </tr>
 *   <tr>
 *     <td>等值查询 (id = 123)</td>
 *     <td>O(log V)</td>
 *     <td>⭐⭐⭐ (Bitmap 更优)</td>
 *   </tr>
 * </table>
 *
 * <h2>支持的数据类型</h2>
 *
 * <ul>
 *   <li><b>整数类型</b>: TINYINT, SMALLINT, INT, BIGINT
 *   <li><b>浮点类型</b>: FLOAT, DOUBLE (转换为 long bits)
 *   <li><b>时间类型</b>: DATE, TIME, TIMESTAMP (存储为数值)
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <h3>创建 BSI 索引</h3>
 *
 * <pre>{@code
 * // 1. 获取工厂实例
 * BitSliceIndexBitmapFileIndexFactory factory = new BitSliceIndexBitmapFileIndexFactory();
 *
 * // 2. 创建索引器
 * Options options = new Options();
 * FileIndexer indexer = factory.create(DataTypes.INT(), options);
 *
 * // 3. 写入索引
 * FileIndexWriter writer = indexer.createWriter();
 * writer.write(GenericRow.of(25));   // age = 25
 * writer.write(GenericRow.of(35));   // age = 35
 * writer.write(GenericRow.of(45));   // age = 45
 * byte[] serialized = writer.serializedBytes();
 * }</pre>
 *
 * <h3>查询 BSI 索引</h3>
 *
 * <pre>{@code
 * // 范围查询: 30 <= age < 50
 * FileIndexReader reader = indexer.createReader(inputStream, 0, length);
 * FieldRef fieldRef = new FieldRef(0, "age", DataTypes.INT());
 *
 * // 下界: age >= 30
 * FileIndexResult lowerResult = reader.visitGreaterOrEqual(fieldRef, 30);
 *
 * // 上界: age < 50
 * FileIndexResult upperResult = reader.visitLessThan(fieldRef, 50);
 *
 * // 组合结果
 * FileIndexResult result = lowerResult.and(upperResult);
 *
 * if (result.remain()) {
 *     // 需要读取数据文件进一步过滤
 *     BitmapIndexResult bitmapResult = (BitmapIndexResult) result;
 *     RoaringBitmap32 bitmap = bitmapResult.get();
 *     // bitmap 包含所有满足条件的行位置
 * }
 * }</pre>
 *
 * <h2>性能对比</h2>
 *
 * <h3>BSI vs 其他索引</h3>
 *
 * <pre>
 * 场景: 100 万行数据,查询 price BETWEEN 1000 AND 5000
 *
 * 不使用索引:
 * - 扫描行数: 100 万
 * - 时间: ~1000ms
 *
 * Bloom Filter:
 * - 不支持范围查询
 * - 时间: N/A
 *
 * 传统 Bitmap (每个值一个 Bitmap):
 * - 需要合并 4000 个 Bitmap (1000-5000)
 * - 时间: ~500ms
 *
 * BSI 索引:
 * - 位切片数量: log2(max_price) ≈ 20
 * - 时间: ~50ms (10x 提升!)
 * </pre>
 *
 * <h2>配置选项</h2>
 *
 * <p>BSI 索引支持的配置选项(通过 Options 传递):
 *
 * <pre>
 * 当前版本 BSI 索引没有额外配置选项,使用默认设置即可。
 * </pre>
 *
 * <h2>SPI 注册</h2>
 *
 * <p>该工厂通过 Java SPI 机制自动注册,标识符为 {@code "bsi"}:
 *
 * <pre>
 * META-INF/services/org.apache.paimon.fileindex.FileIndexerFactory:
 * org.apache.paimon.fileindex.bsi.BitSliceIndexBitmapFileIndexFactory
 * </pre>
 *
 * <h2>SQL 集成</h2>
 *
 * <p>在 SQL 中使用 BSI 索引:
 *
 * <pre>{@code
 * -- 创建表时指定 BSI 索引
 * CREATE TABLE orders (
 *     order_id BIGINT,
 *     amount DECIMAL(10,2),
 *     quantity INT
 * ) WITH (
 *     'file.index.columns' = 'quantity,amount',
 *     'file.index.quantity.columns' = 'bsi',
 *     'file.index.amount.columns' = 'bsi'
 * );
 *
 * -- 查询会自动使用 BSI 索引
 * SELECT * FROM orders WHERE quantity BETWEEN 10 AND 100;
 * SELECT AVG(amount) FROM orders WHERE quantity > 50;
 * }</pre>
 *
 * @see BitSliceIndexBitmapFileIndex BSI 索引的具体实现
 * @see FileIndexerFactory 索引工厂接口
 */
public class BitSliceIndexBitmapFileIndexFactory implements FileIndexerFactory {

    /** BSI 索引的类型标识符,用于 SQL 和配置中引用。 */
    public static final String BSI_INDEX = "bsi";

    @Override
    public String identifier() {
        return BSI_INDEX;
    }

    @Override
    public FileIndexer create(DataType dataType, Options options) {
        return new BitSliceIndexBitmapFileIndex(dataType, options);
    }
}
