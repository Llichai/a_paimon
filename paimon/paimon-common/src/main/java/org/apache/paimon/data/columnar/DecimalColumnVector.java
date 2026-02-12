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

import org.apache.paimon.data.Decimal;

/**
 * 十进制数列向量接口,用于访问高精度十进制数据。
 *
 * <p>此接口提供对 DECIMAL/NUMERIC 类型数据的访问,支持任意精度和小数位数的数值。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>财务计算中的货币金额
 *   <li>需要精确小数运算的科学计算
 *   <li>高精度的统计分析
 * </ul>
 *
 * <h2>存储方式</h2>
 * <ul>
 *   <li>小精度(precision ≤ 18): 存储为 long 类型
 *   <li>中等精度(precision ≤ 38): 存储为 long 或自定义格式
 *   <li>大精度(precision > 38): 存储为字节数组
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.Decimal 十进制数表示类
 */
public interface DecimalColumnVector extends ColumnVector {
    /**
     * 获取指定位置的十进制数值。
     *
     * @param i 行索引(从0开始)
     * @param precision 精度(总位数)
     * @param scale 小数位数
     * @return 十进制数对象
     */
    Decimal getDecimal(int i, int precision, int scale);
}
