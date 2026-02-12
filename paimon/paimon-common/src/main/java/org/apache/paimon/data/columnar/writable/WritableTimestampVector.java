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

package org.apache.paimon.data.columnar.writable;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.TimestampColumnVector;

/**
 * 可写时间戳列向量接口。
 *
 * <p>WritableTimestampVector 扩展了只读的 {@link TimestampColumnVector},提供时间戳数据的写入能力。
 * 时间戳类型用于存储精确到纳秒的日期时间信息。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setTimestamp()在指定位置写入时间戳
 *   <li><b>追加写入</b>: 通过appendTimestamp()追加时间戳值
 *   <li><b>填充操作</b>: 通过fill()用指定时间戳填充整个向量
 * </ul>
 *
 * <h2>时间戳存储结构</h2>
 * {@link Timestamp} 类型包含两个部分:
 * <ul>
 *   <li><b>毫秒部分</b>: 64位长整数,表示自1970-01-01 00:00:00 UTC的毫秒数
 *   <li><b>纳秒部分</b>: 32位整数,表示毫秒内的纳秒偏移(0-999,999)
 * </ul>
 * <p>总精度: 纳秒级(10^-9秒),可精确表示任意时刻到纳秒。
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>存储事件发生时间(如订单创建时间、日志时间戳)
 *   <li>存储数据修改时间(如记录更新时间、文件修改时间)
 *   <li>存储高精度时序数据(如金融交易时间、传感器采样时间)
 *   <li>存储SQL TIMESTAMP类型数据
 *   <li>存储系统审计日志的时间信息
 * </ul>
 *
 * <h2>时间戳精度</h2>
 * <ul>
 *   <li><b>纳秒精度</b>: Timestamp支持最高纳秒级精度(10^-9秒)
 *   <li><b>毫秒精度</b>: 常用精度,足够大多数业务场景
 *   <li><b>微秒精度</b>: 适合高频交易、性能分析等场景
 *   <li><b>秒精度</b>: Unix时间戳,适合一般日期时间存储
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的时间戳向量
 * WritableTimestampVector vector = new HeapTimestampVector(100);
 *
 * // 存储当前时间
 * Timestamp now = Timestamp.fromEpochMillis(System.currentTimeMillis());
 * vector.setTimestamp(0, now);
 *
 * // 存储指定时间(2024-01-01 12:00:00)
 * LocalDateTime ldt = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
 * long millis = ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
 * Timestamp ts = Timestamp.fromEpochMillis(millis);
 * vector.setTimestamp(1, ts);
 *
 * // 存储高精度时间戳(带纳秒)
 * Timestamp nanoTs = Timestamp.fromEpochMillis(millis, 123456);  // 毫秒+纳秒
 * vector.appendTimestamp(nanoTs);
 *
 * // 批量追加时间戳
 * for (int i = 0; i < 100; i++) {
 *     long timestamp = System.currentTimeMillis() + i * 1000;  // 每秒一个
 *     vector.appendTimestamp(Timestamp.fromEpochMillis(timestamp));
 * }
 *
 * // 存储交易时间(微秒精度)
 * long microsSinceEpoch = System.currentTimeMillis() * 1000;
 * Timestamp tradeTime = Timestamp.fromMicros(microsSinceEpoch);
 * vector.appendTimestamp(tradeTime);
 *
 * // 填充默认时间戳
 * Timestamp defaultTs = Timestamp.fromEpochMillis(0);  // 1970-01-01 00:00:00
 * vector.fill(defaultTs);
 * }</pre>
 *
 * <h2>时间戳创建方式</h2>
 * <pre>{@code
 * // 1. 从毫秒创建
 * Timestamp ts1 = Timestamp.fromEpochMillis(System.currentTimeMillis());
 *
 * // 2. 从毫秒+纳秒创建
 * Timestamp ts2 = Timestamp.fromEpochMillis(millis, nanos);
 *
 * // 3. 从微秒创建
 * Timestamp ts3 = Timestamp.fromMicros(micros);
 *
 * // 4. 从秒+纳秒创建
 * Timestamp ts4 = Timestamp.fromInstant(Instant.now());
 *
 * // 5. 从LocalDateTime创建
 * LocalDateTime ldt = LocalDateTime.now();
 * Timestamp ts5 = Timestamp.fromLocalDateTime(ldt);
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setTimestamp()更高效
 *   <li>如果不需要纳秒精度,可以只使用毫秒部分
 *   <li>批量操作时重用Timestamp对象,减少对象创建
 * </ul>
 *
 * <h2>时区处理</h2>
 * <ul>
 *   <li>Timestamp内部存储UTC时间(无时区信息)
 *   <li>显示时需要根据目标时区转换
 *   <li>建议统一使用UTC存储,显示时转换为本地时区
 *   <li>避免混用不同时区的时间戳
 * </ul>
 *
 * <h2>NULL值处理</h2>
 * <ul>
 *   <li>时间戳向量支持NULL值,表示"未知"或"不适用"
 *   <li>NULL与epoch时间(1970-01-01 00:00:00)是不同的
 *   <li>通过setNullAt()方法设置NULL
 *   <li>通过isNullAt()方法判断是否为NULL
 * </ul>
 *
 * <h2>与其他时间类型的比较</h2>
 * <ul>
 *   <li>vs Long(毫秒): Timestamp支持纳秒精度,更适合高精度时间
 *   <li>vs LocalDateTime: Timestamp无时区信息,更适合跨时区场景
 *   <li>vs Instant: Timestamp是Paimon特有类型,优化了存储和序列化
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableTimestampVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see TimestampColumnVector 只读时间戳列向量接口
 * @see Timestamp 时间戳数据类型
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableTimestampVector extends WritableColumnVector, TimestampColumnVector {

    /**
     * 在指定位置设置时间戳值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param timestamp 要设置的时间戳对象
     */
    void setTimestamp(int rowId, Timestamp timestamp);

    /**
     * 追加一个时间戳值。
     *
     * <p>在当前追加位置写入时间戳,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param timestamp 要追加的时间戳对象
     */
    void appendTimestamp(Timestamp timestamp);

    /**
     * 用指定时间戳填充整个列向量。
     *
     * <p>填充后,向量中所有位置都包含相同的时间戳值。
     *
     * @param value 填充用的时间戳
     */
    void fill(Timestamp value);
}
