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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.Timestamp;

/**
 * 字典接口,用于解码字典编码的列向量值。
 *
 * <p>字典编码是一种常用的列式存储压缩技术,通过将重复的值映射到整数 ID,
 * 可以显著减少存储空间和提高查询性能。此接口定义了从字典 ID 解码回原始值的方法。
 *
 * <h2>字典编码原理</h2>
 * <pre>
 * 原始数据: ["Beijing", "Shanghai", "Beijing", "Beijing", "Shanghai"]
 *
 * 字典:
 *   ID 0 -> "Beijing"
 *   ID 1 -> "Shanghai"
 *
 * 编码后: [0, 1, 0, 0, 1]  (仅存储ID,大幅减少存储空间)
 * </pre>
 *
 * <h2>适用场景</h2>
 * <ul>
 *   <li>低基数列(distinct值数量少)
 *   <li>重复值较多的列
 *   <li>字符串列的压缩存储
 * </ul>
 *
 * <h2>性能优势</h2>
 * <ul>
 *   <li><b>存储优化:</b> 减少存储空间(特别是字符串类型)
 *   <li><b>查询加速:</b> 过滤操作可以直接在字典 ID 上进行
 *   <li><b>聚合优化:</b> GROUP BY 可以基于 ID 进行,避免字符串比较
 * </ul>
 *
 * <h2>实现建议</h2>
 * <ul>
 *   <li>字典通常在列块级别构建
 *   <li>字典大小有限制,超过阈值时回退到普通编码
 *   <li>可以结合其他压缩算法(如 RLE, bit-packing)进一步压缩
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 */
public interface Dictionary {

    /**
     * 将字典 ID 解码为整型值。
     *
     * @param id 字典 ID
     * @return 解码后的整型值
     */
    int decodeToInt(int id);

    /**
     * 将字典 ID 解码为长整型值。
     *
     * @param id 字典 ID
     * @return 解码后的长整型值
     */
    long decodeToLong(int id);

    /**
     * 将字典 ID 解码为单精度浮点值。
     *
     * @param id 字典 ID
     * @return 解码后的单精度浮点值
     */
    float decodeToFloat(int id);

    /**
     * 将字典 ID 解码为双精度浮点值。
     *
     * @param id 字典 ID
     * @return 解码后的双精度浮点值
     */
    double decodeToDouble(int id);

    /**
     * 将字典 ID 解码为二进制数据。
     *
     * @param id 字典 ID
     * @return 解码后的字节数组
     */
    byte[] decodeToBinary(int id);

    /**
     * 将字典 ID 解码为时间戳值。
     *
     * @param id 字典 ID
     * @return 解码后的时间戳对象
     */
    Timestamp decodeToTimestamp(int id);
}
