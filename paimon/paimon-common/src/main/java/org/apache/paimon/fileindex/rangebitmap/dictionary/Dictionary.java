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

package org.apache.paimon.fileindex.rangebitmap.dictionary;

/**
 * 字典接口,建立键(Key)与编码(Code)之间的双向映射关系。
 *
 * <p>字典是 RangeBitmap 索引的核心组件,用于将任意可比较类型的值(如字符串)映射为连续的整数编码,
 * 然后基于这些编码构建位切片索引(BSI),从而支持高效的范围查询和 TopN 查询。
 *
 * <h2>核心功能</h2>
 *
 * <h3>双向映射</h3>
 *
 * <ul>
 *   <li><b>Key → Code</b>: {@link #find(Object)} 将值映射为整数编码
 *   <li><b>Code → Key</b>: {@link #find(int)} 将整数编码反向映射为值
 * </ul>
 *
 * <h3>编码规则</h3>
 *
 * <pre>
 * - 编码从 0 开始: 第一个值编码为 0,第二个值编码为 1,以此类推
 * - 编码顺序: 按照值的自然顺序(Comparable)排序后编码
 * - 连续性: 编码是连续的整数,中间没有间隙
 * - 查找失败: 返回 -1 表示键不存在于字典中
 * </pre>
 *
 * <h2>使用场景</h2>
 *
 * <h3>RangeBitmap 索引流程</h3>
 *
 * <pre>
 * 1. 构建阶段:
 *    - 收集所有不同的值 (distinct values)
 *    - 按自然顺序排序
 *    - 分配连续编码: value1=0, value2=1, value3=2, ...
 *    - 构建字典和 BSI 索引
 *
 * 2. 查询阶段:
 *    - 将查询条件中的值转换为编码
 *    - 在 BSI 索引上执行范围查询
 *    - 返回匹配的行位置 Bitmap
 * </pre>
 *
 * <h3>示例: 字符串范围查询</h3>
 *
 * <pre>{@code
 * // 原始数据: ["Beijing", "Shanghai", "Guangzhou", "Beijing", "Shenzhen"]
 * // 字典构建:
 * Dictionary dict = ...;
 * // 排序后编码:
 * // "Beijing"    -> code 0
 * // "Guangzhou"  -> code 1
 * // "Shanghai"   -> code 2
 * // "Shenzhen"   -> code 3
 *
 * // 查询: city >= "G" AND city < "Sh"
 * // 步骤 1: 将边界值转换为编码
 * int lowerCode = dict.find("G");  // 返回 -1 (不存在), 实际使用二分查找找到 >= "G" 的第一个值
 * // 通过二分查找,找到 >= "G" 的第一个编码: "Guangzhou" = 1
 * int upperCode = dict.find("Sh"); // 返回 -1, 找到 < "Sh" 的最后一个编码: "Shanghai" = 2
 *
 * // 步骤 2: 在 BSI 上查询编码范围 [1, 2]
 * // 返回: code=1 (Guangzhou) 和 code=2 (Shanghai) 对应的行位置
 * // 结果: 行 1, 行 2
 * }</pre>
 *
 * <h2>实现类型</h2>
 *
 * <p>Paimon 提供两种字典实现:
 *
 * <table border="1">
 *   <tr>
 *     <th>实现类</th>
 *     <th>存储结构</th>
 *     <th>查找性能</th>
 *     <th>适用场景</th>
 *   </tr>
 *   <tr>
 *     <td>ChunkedDictionary</td>
 *     <td>分块存储</td>
 *     <td>O(log B + log E)</td>
 *     <td>大型字典,控制内存</td>
 *   </tr>
 *   <tr>
 *     <td>FixedLengthChunk</td>
 *     <td>定长数组</td>
 *     <td>O(log n)</td>
 *     <td>数值、日期等定长类型</td>
 *   </tr>
 *   <tr>
 *     <td>VariableLengthChunk</td>
 *     <td>偏移量数组</td>
 *     <td>O(log n)</td>
 *     <td>字符串等变长类型</td>
 *   </tr>
 * </table>
 *
 * <p>B = 块数量, E = 块内条目数量, n = 总条目数量
 *
 * <h2>性能特性</h2>
 *
 * <h3>空间复杂度</h3>
 *
 * <pre>
 * - 定长类型 (INT, LONG): O(D × sizeof(type))
 * - 变长类型 (STRING): O(D × avg_length + D × 4)  (数据 + 偏移量)
 * - 总体: O(D × S), D = 不同值数量, S = 单个值的平均字节数
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
 *     <td>find(Object key)</td>
 *     <td>O(log D)</td>
 *     <td>二分查找,D = 字典大小</td>
 *   </tr>
 *   <tr>
 *     <td>find(int code)</td>
 *     <td>O(1) 或 O(log B)</td>
 *     <td>直接访问或块级查找</td>
 *   </tr>
 *   <tr>
 *     <td>构建字典</td>
 *     <td>O(D log D)</td>
 *     <td>排序 + 构建</td>
 *   </tr>
 * </table>
 *
 * <h2>使用限制</h2>
 *
 * <h3>1. 值的可比较性</h3>
 *
 * <p>字典中的值必须实现 {@link Comparable} 接口:
 *
 * <pre>
 * ✅ 支持: STRING, INT, LONG, DATE, TIMESTAMP, etc.
 * ❌ 不支持: ARRAY, MAP, ROW (复杂类型)
 * </pre>
 *
 * <h3>2. 字典大小限制</h3>
 *
 * <pre>
 * - 编码使用 int 类型: 最多支持 2^31 - 1 个不同值
 * - 实际限制: 通常建议 < 1000000 个不同值
 * - 超过限制: 考虑使用其他索引类型或分区
 * </pre>
 *
 * <h3>3. 内存控制</h3>
 *
 * <pre>
 * - 字典完全加载到内存中
 * - 大型字典建议使用 ChunkedDictionary 分块加载
 * - 估算内存: dictionary_size = D × (value_size + 4)
 * </pre>
 *
 * @see org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.ChunkedDictionary 分块字典实现
 * @see Appender 字典构建器接口
 */
public interface Dictionary {

    /**
     * 根据键查找对应的编码。
     *
     * <p>在字典中查找给定键对应的整数编码。编码按照键的自然顺序分配。
     *
     * <h3>查找逻辑</h3>
     *
     * <pre>
     * - 如果键存在于字典: 返回对应的编码 (>= 0)
     * - 如果键不存在: 返回 -1
     * </pre>
     *
     * <h3>使用示例</h3>
     *
     * <pre>{@code
     * Dictionary dict = ...; // 包含 "Alice"=0, "Bob"=1, "Charlie"=2
     *
     * int code1 = dict.find("Alice");    // 返回 0
     * int code2 = dict.find("Bob");      // 返回 1
     * int code3 = dict.find("David");    // 返回 -1 (不存在)
     * }</pre>
     *
     * <h3>性能</h3>
     *
     * <ul>
     *   <li><b>时间复杂度</b>: O(log D), D 为字典大小
     *   <li><b>实现方式</b>: 二分查找排序数组
     * </ul>
     *
     * @param key 要查找的键,必须与字典中的键类型一致
     * @return 键对应的编码(>=0),如果键不存在则返回 -1
     */
    int find(Object key);

    /**
     * 根据编码查找对应的键。
     *
     * <p>反向查找操作,根据整数编码获取原始键。
     *
     * <h3>查找逻辑</h3>
     *
     * <pre>
     * - 如果编码有效 (0 <= code < size): 返回对应的键
     * - 如果编码无效: 行为未定义 (可能抛出异常或返回 null)
     * </pre>
     *
     * <h3>使用示例</h3>
     *
     * <pre>{@code
     * Dictionary dict = ...; // 包含 "Alice"=0, "Bob"=1, "Charlie"=2
     *
     * Object key1 = dict.find(0);   // 返回 "Alice"
     * Object key2 = dict.find(1);   // 返回 "Bob"
     * Object key3 = dict.find(10);  // 未定义行为
     * }</pre>
     *
     * <h3>性能</h3>
     *
     * <ul>
     *   <li><b>时间复杂度</b>: 通常 O(1),直接数组访问
     *   <li><b>ChunkedDictionary</b>: O(log B), B 为块数量
     * </ul>
     *
     * @param code 编码值,必须在有效范围内 [0, size)
     * @return 编码对应的键,如果编码无效则行为未定义
     */
    Object find(int code);

    /**
     * 字典构建器接口。
     *
     * <p>用于构建字典的辅助接口,支持按排序顺序添加键值对,最终序列化为字节数组。
     *
     * <h2>使用流程</h2>
     *
     * <pre>
     * 1. 创建 Appender 实例
     * 2. 按排序顺序调用 sortedAppend() 添加键值对
     * 3. 调用 serialize() 获取序列化字节
     * 4. 将字节数组写入索引文件
     * </pre>
     *
     * <h2>使用示例</h2>
     *
     * <pre>{@code
     * // 构建字符串字典
     * Dictionary.Appender appender = createAppender();
     *
     * // 按排序顺序添加 (必须预先排序!)
     * appender.sortedAppend("Alice", 0);
     * appender.sortedAppend("Bob", 1);
     * appender.sortedAppend("Charlie", 2);
     *
     * // 序列化
     * byte[] bytes = appender.serialize();
     *
     * // 写入文件
     * outputStream.write(bytes);
     * }</pre>
     *
     * <h2>重要约束</h2>
     *
     * <ul>
     *   <li><b>顺序性</b>: 必须按编码升序调用 sortedAppend()
     *   <li><b>唯一性</b>: 每个编码只能调用一次
     *   <li><b>连续性</b>: 编码必须从 0 开始连续递增
     * </ul>
     */
    interface Appender {

        /**
         * 按排序顺序添加键值对。
         *
         * <p>将一个键和对应的编码添加到字典中。调用方必须确保按编码升序调用此方法。
         *
         * <h3>调用约束</h3>
         *
         * <pre>
         * - 编码必须连续: 0, 1, 2, 3, ...
         * - 编码必须递增: code(n+1) = code(n) + 1
         * - 键必须排序: key(n) < key(n+1) (自然顺序)
         * </pre>
         *
         * <h3>错误示例</h3>
         *
         * <pre>{@code
         * // ❌ 错误: 编码不连续
         * appender.sortedAppend("Alice", 0);
         * appender.sortedAppend("Bob", 2);  // 缺少 code=1
         *
         * // ❌ 错误: 键未排序
         * appender.sortedAppend("Bob", 0);
         * appender.sortedAppend("Alice", 1);  // "Alice" < "Bob",违反顺序
         *
         * // ✅ 正确
         * appender.sortedAppend("Alice", 0);
         * appender.sortedAppend("Bob", 1);
         * appender.sortedAppend("Charlie", 2);
         * }</pre>
         *
         * @param key 键,必须按自然顺序排序
         * @param code 编码,必须从 0 开始连续递增
         */
        void sortedAppend(Object key, int code);

        /**
         * 序列化字典为字节数组。
         *
         * <p>将构建好的字典序列化为紧凑的字节表示,用于存储到索引文件中。
         *
         * <h3>序列化格式</h3>
         *
         * <p>格式取决于具体实现:
         *
         * <ul>
         *   <li><b>FixedLengthChunk</b>: 直接存储值数组
         *   <li><b>VariableLengthChunk</b>: 偏移量数组 + 值数据
         *   <li><b>ChunkedDictionary</b>: 块元数据 + 块数据
         * </ul>
         *
         * <h3>使用示例</h3>
         *
         * <pre>{@code
         * // 构建字典
         * Dictionary.Appender appender = createAppender();
         * for (int i = 0; i < sortedKeys.length; i++) {
         *     appender.sortedAppend(sortedKeys[i], i);
         * }
         *
         * // 序列化
         * byte[] serialized = appender.serialize();
         *
         * // 估算大小
         * System.out.println("Dictionary size: " + serialized.length + " bytes");
         * }</pre>
         *
         * @return 序列化后的字节数组
         */
        byte[] serialize();
    }
}
