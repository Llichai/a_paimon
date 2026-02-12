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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

/**
 * RangeBitmap(范围位图)文件索引工厂。
 *
 * <p>该工厂负责创建 {@link RangeBitmapFileIndex} 实例,用于构建和查询基于字典编码 + BSI 的范围位图索引。
 *
 * <h2>RangeBitmap 索引概述</h2>
 *
 * <p>RangeBitmap 是 Paimon 中最通用的索引类型,结合了字典编码和位切片索引(BSI)的优势,
 * 可以为任意可比较类型(包括字符串)提供高效的范围查询和 TopN 查询支持。
 *
 * <h3>核心架构</h3>
 *
 * <pre>
 * 原始数据:    ["Beijing", "Shanghai", "Beijing", "Guangzhou", ...]
 *                  ↓
 * 字典编码:    [0, 1, 0, 2, ...]    (Beijing=0, Shanghai=1, Guangzhou=2)
 *                  ↓
 * BSI 索引:    构建数值索引,支持范围查询
 * </pre>
 *
 * <h3>核心特性</h3>
 *
 * <ul>
 *   <li><b>通用性</b>: 支持所有可比较类型,包括字符串、数值、时间等
 *   <li><b>范围查询</b>: 时间复杂度 O(log D),D 为字典大小
 *   <li><b>TopN 查询</b>: 直接从索引计算结果,支持 NULLS_FIRST/NULLS_LAST
 *   <li><b>分块字典</b>: 使用 ChunkedDictionary 控制内存使用
 * </ul>
 *
 * <h2>适用场景</h2>
 *
 * <table border="1">
 *   <tr>
 *     <th>数据类型</th>
 *     <th>查询模式</th>
 *     <th>RangeBitmap 性能</th>
 *     <th>推荐度</th>
 *   </tr>
 *   <tr>
 *     <td>字符串</td>
 *     <td>范围查询 (city >= 'B' AND city < 'C')</td>
 *     <td>O(log D)</td>
 *     <td>⭐⭐⭐⭐⭐</td>
 *   </tr>
 *   <tr>
 *     <td>字符串</td>
 *     <td>TopN (ORDER BY name LIMIT 10)</td>
 *     <td>O(K × log D)</td>
 *     <td>⭐⭐⭐⭐⭐</td>
 *   </tr>
 *   <tr>
 *     <td>数值</td>
 *     <td>范围查询 (price BETWEEN 100 AND 500)</td>
 *     <td>O(log D)</td>
 *     <td>⭐⭐⭐⭐ (BSI 更优)</td>
 *   </tr>
 *   <tr>
 *     <td>时间</td>
 *     <td>范围查询 (date BETWEEN '2024-01-01' AND '2024-12-31')</td>
 *     <td>O(log D)</td>
 *     <td>⭐⭐⭐⭐</td>
 *   </tr>
 *   <tr>
 *     <td>任意类型</td>
 *     <td>等值查询 (status = 'ACTIVE')</td>
 *     <td>O(1)</td>
 *     <td>⭐⭐⭐ (Bitmap 更优)</td>
 *   </tr>
 * </table>
 *
 * <h2>与其他索引的对比</h2>
 *
 * <table border="1">
 *   <tr>
 *     <th>索引类型</th>
 *     <th>支持类型</th>
 *     <th>范围查询</th>
 *     <th>TopN 查询</th>
 *     <th>最佳场景</th>
 *   </tr>
 *   <tr>
 *     <td>Bitmap</td>
 *     <td>全部</td>
 *     <td>❌</td>
 *     <td>❌</td>
 *     <td>等值查询、低基数列</td>
 *   </tr>
 *   <tr>
 *     <td>BSI</td>
 *     <td>仅数值</td>
 *     <td>✅ 快</td>
 *     <td>✅</td>
 *     <td>数值范围查询、聚合</td>
 *   </tr>
 *   <tr>
 *     <td>RangeBitmap</td>
 *     <td>全部可比较类型</td>
 *     <td>✅</td>
 *     <td>✅</td>
 *     <td>字符串范围、TopN</td>
 *   </tr>
 *   <tr>
 *     <td>Bloom Filter</td>
 *     <td>全部</td>
 *     <td>❌</td>
 *     <td>❌</td>
 *     <td>等值查询、去重</td>
 *   </tr>
 * </table>
 *
 * <h2>支持的数据类型</h2>
 *
 * <p>RangeBitmap 支持所有可比较类型:
 *
 * <ul>
 *   <li><b>字符串类型</b>: CHAR, VARCHAR
 *   <li><b>数值类型</b>: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE
 *   <li><b>时间类型</b>: DATE, TIME, TIMESTAMP, LOCAL_ZONED_TIMESTAMP
 *   <li><b>布尔类型</b>: BOOLEAN
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <h3>1. 字符串范围查询</h3>
 *
 * <pre>{@code
 * // 创建工厂和索引器
 * RangeBitmapFileIndexFactory factory = new RangeBitmapFileIndexFactory();
 * FileIndexer indexer = factory.create(DataTypes.STRING(), new Options());
 *
 * // 写入索引
 * FileIndexWriter writer = indexer.createWriter();
 * writer.write(GenericRow.of(BinaryString.fromString("Beijing")));
 * writer.write(GenericRow.of(BinaryString.fromString("Shanghai")));
 * writer.write(GenericRow.of(BinaryString.fromString("Guangzhou")));
 * byte[] serialized = writer.serializedBytes();
 *
 * // 查询: city >= "B" AND city < "H"
 * FileIndexReader reader = indexer.createReader(inputStream, 0, length);
 * FieldRef fieldRef = new FieldRef(0, "city", DataTypes.STRING());
 *
 * FileIndexResult result1 = reader.visitGreaterOrEqual(
 *     fieldRef, BinaryString.fromString("B"));
 * FileIndexResult result2 = reader.visitLessThan(
 *     fieldRef, BinaryString.fromString("H"));
 * FileIndexResult finalResult = result1.and(result2);
 *
 * if (finalResult.remain()) {
 *     // 返回 Beijing, Guangzhou (Shanghai 不在范围内)
 *     BitmapIndexResult bitmap = (BitmapIndexResult) finalResult;
 *     // 使用 bitmap 过滤数据
 * }
 * }</pre>
 *
 * <h3>2. TopN 查询</h3>
 *
 * <pre>{@code
 * // 查询: SELECT * FROM table ORDER BY name DESC LIMIT 10
 * FileIndexReader reader = indexer.createReader(inputStream, 0, length);
 * FieldRef fieldRef = new FieldRef(0, "name", DataTypes.STRING());
 *
 * // TopN 查询(降序,NULL 排最后)
 * FileIndexResult result = reader.visitTopN(
 *     fieldRef,
 *     10,           // 取前 10 条
 *     false,        // 降序
 *     false         // NULLS LAST
 * );
 *
 * BitmapIndexResult bitmap = (BitmapIndexResult) result;
 * // bitmap 包含 TopN 的行位置
 * }</pre>
 *
 * <h3>3. 时间范围查询</h3>
 *
 * <pre>{@code
 * // 查询: WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'
 * FileIndexer indexer = factory.create(DataTypes.DATE(), new Options());
 *
 * // 写入日期(内部存储为距离 epoch 的天数)
 * writer.write(GenericRow.of(18628));  // 2021-01-01
 * writer.write(GenericRow.of(19723));  // 2024-01-01
 * writer.write(GenericRow.of(20088));  // 2024-12-31
 *
 * // 查询 2024 年的数据
 * FileIndexResult result1 = reader.visitGreaterOrEqual(fieldRef, 19723);
 * FileIndexResult result2 = reader.visitLessOrEqual(fieldRef, 20088);
 * FileIndexResult finalResult = result1.and(result2);
 * }</pre>
 *
 * <h2>性能分析</h2>
 *
 * <h3>空间复杂度</h3>
 *
 * <pre>
 * - 字典大小: O(D × S), D 为不同值数量, S 为单个值的平均字节数
 * - BSI 大小: O(N × log D), N 为行数, D 为字典大小
 * - 总大小: 通常 < 原始数据的 10%
 * </pre>
 *
 * <h3>时间复杂度</h3>
 *
 * <table border="1">
 *   <tr>
 *     <th>操作</th>
 *     <th>时间复杂度</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>构建索引</td>
 *     <td>O(N × log D)</td>
 *     <td>N 为行数, D 为字典大小</td>
 *   </tr>
 *   <tr>
 *     <td>等值查询</td>
 *     <td>O(log D)</td>
 *     <td>二分查找字典 + 获取 Bitmap</td>
 *   </tr>
 *   <tr>
 *     <td>范围查询</td>
 *     <td>O(log D × log V)</td>
 *     <td>二分查找边界 + BSI 范围查询</td>
 *   </tr>
 *   <tr>
 *     <td>TopN 查询</td>
 *     <td>O(K × log D)</td>
 *     <td>K 为 TopN 的 N 值</td>
 *   </tr>
 * </table>
 *
 * <h2>配置选项</h2>
 *
 * <p>RangeBitmap 索引支持的配置选项:
 *
 * <pre>
 * 当前版本使用默认配置,字典采用 ChunkedDictionary 实现,
 * 自动控制内存使用和查询性能。
 * </pre>
 *
 * <h2>SPI 注册</h2>
 *
 * <p>该工厂通过 Java SPI 机制自动注册,标识符为 {@code "range-bitmap"}:
 *
 * <pre>
 * META-INF/services/org.apache.paimon.fileindex.FileIndexerFactory:
 * org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndexFactory
 * </pre>
 *
 * <h2>SQL 集成</h2>
 *
 * <p>在 SQL 中使用 RangeBitmap 索引:
 *
 * <pre>{@code
 * -- 创建表时指定 RangeBitmap 索引
 * CREATE TABLE users (
 *     user_id BIGINT,
 *     username STRING,
 *     city STRING,
 *     register_date DATE
 * ) WITH (
 *     'file.index.columns' = 'city,username,register_date',
 *     'file.index.city.columns' = 'range-bitmap',
 *     'file.index.username.columns' = 'range-bitmap',
 *     'file.index.register_date.columns' = 'range-bitmap'
 * );
 *
 * -- 以下查询会自动使用 RangeBitmap 索引:
 *
 * -- 字符串范围查询
 * SELECT * FROM users WHERE city BETWEEN 'A' AND 'M';
 *
 * -- TopN 查询
 * SELECT * FROM users ORDER BY username LIMIT 100;
 *
 * -- 时间范围查询
 * SELECT * FROM users WHERE register_date BETWEEN '2024-01-01' AND '2024-12-31';
 * }</pre>
 *
 * <h2>最佳实践</h2>
 *
 * <h3>1. 选择合适的列</h3>
 *
 * <pre>
 * ✅ 适合:
 * - 字符串列,经常进行范围或 TopN 查询
 * - 中等基数列 (100 - 10000 不同值)
 * - 需要排序的列
 *
 * ❌ 不适合:
 * - 高基数列 (> 100000 不同值),字典太大
 * - 只做等值查询的列,Bitmap 更简单高效
 * - 低基数列 (< 10 不同值),直接扫描即可
 * </pre>
 *
 * <h3>2. 与其他索引组合</h3>
 *
 * <pre>{@code
 * -- 为不同列使用不同索引
 * CREATE TABLE orders (
 *     order_id BIGINT,
 *     status STRING,      -- 低基数,使用 Bitmap
 *     amount DECIMAL,     -- 数值,使用 BSI
 *     customer_name STRING  -- 字符串范围,使用 RangeBitmap
 * ) WITH (
 *     'file.index.status.columns' = 'bitmap',
 *     'file.index.amount.columns' = 'bsi',
 *     'file.index.customer_name.columns' = 'range-bitmap'
 * );
 * }</pre>
 *
 * @see RangeBitmapFileIndex RangeBitmap 索引的具体实现
 * @see org.apache.paimon.fileindex.rangebitmap.dictionary.ChunkedDictionary 分块字典实现
 * @see FileIndexerFactory 索引工厂接口
 */
public class RangeBitmapFileIndexFactory implements FileIndexerFactory {

    /** RangeBitmap 索引的类型标识符,用于 SQL 和配置中引用。 */
    public static final String RANGE_BITMAP = "range-bitmap";

    @Override
    public String identifier() {
        return RANGE_BITMAP;
    }

    @Override
    public FileIndexer create(DataType dataType, Options options) {
        return new RangeBitmapFileIndex(dataType, options);
    }
}
