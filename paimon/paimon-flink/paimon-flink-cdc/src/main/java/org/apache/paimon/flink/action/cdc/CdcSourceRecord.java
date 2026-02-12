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

package org.apache.paimon.flink.action.cdc;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * CDC 数据变更记录容器
 *
 * <p>CdcSourceRecord 表示从 CDC 源系统（如 MySQL、PostgreSQL、MongoDB 等）捕获的单条数据变更记录。
 * 它包含了消息来源、键和值三个主要成分，用于支持多种 CDC 源的数据处理。
 *
 * <p>数据结构说明:
 * <ul>
 *   <li><b>topic</b>: 消息主题（可选），表示消息来自的队列或分区（如 Kafka topic、Pulsar topic）
 *   <li><b>key</b>: 消息键（可选），通常为主键标识，用于路由到特定的分区或进程
 *   <li><b>value</b>: 消息值（必需），包含实际的数据变更内容（INSERT/UPDATE/DELETE）
 * </ul>
 *
 * <p>适用场景:
 * <ul>
 *   <li>Debezium CDC: 值为 JSON 格式的变更事件
 *   <li>Canal/Aliyun CDC: 值为特定格式的变更日志
 *   <li>Kafka/Pulsar: topic 用于标识消息来源
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建 CDC 记录（来自 Kafka）
 * CdcSourceRecord record = new CdcSourceRecord(
 *     "mysql_topic",
 *     "pk_value",
 *     "{\"before\": {...}, \"after\": {...}, \"op\": \"insert\"}"
 * );
 *
 * // 简化构造（无 topic/key）
 * CdcSourceRecord record = new CdcSourceRecord(jsonPayload);
 * }</pre>
 *
 * @see org.apache.paimon.flink.action.cdc.format.DataFormat
 * @see org.apache.paimon.flink.action.cdc.format.AbstractRecordParser
 */
public class CdcSourceRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 消息来源主题（可选）。用于标识消息来自哪个队列或分区，如 Kafka topic 名称 */
    @Nullable private final String topic;

    /** 消息键（可选）。通常用于标识记录的主键，影响数据流的分配和顺序 */
    @Nullable private final Object key;

    /** 消息值（必需）。包含实际的 CDC 数据变更内容（INSERT/UPDATE/DELETE 操作的详细数据） */
    // TODO 使用泛型支持更多场景
    private final Object value;

    /**
     * 构造函数 - 创建完整的 CDC 记录
     *
     * @param topic 消息来源主题（可选，可为 null）
     * @param key 消息键（可选，可为 null）
     * @param value 消息值（必需，不能为 null）
     */
    public CdcSourceRecord(@Nullable String topic, @Nullable Object key, Object value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    /**
     * 构造函数 - 仅包含消息值（简化版）
     *
     * <p>此构造函数适用于不需要 topic 和 key 的场景。
     *
     * @param value 消息值（必需）
     */
    public CdcSourceRecord(Object value) {
        this(null, null, value);
    }

    /**
     * 获取消息来源主题
     *
     * @return 主题名称，如果未设置则为 null
     */
    @Nullable
    public String getTopic() {
        return topic;
    }

    /**
     * 获取消息键
     *
     * @return 消息键，如果未设置则为 null
     */
    @Nullable
    public Object getKey() {
        return key;
    }

    /**
     * 获取消息值
     *
     * @return 包含数据变更的消息值对象
     */
    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        // 检查类型是否相同
        if (!(o instanceof CdcSourceRecord)) {
            return false;
        }

        // 比较所有字段值是否相同
        CdcSourceRecord that = (CdcSourceRecord) o;
        return Objects.equals(topic, that.topic)
                && Objects.equals(key, that.key)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        // 计算哈希码，用于 HashSet/HashMap 等集合操作
        return Objects.hash(topic, key, value);
    }

    @Override
    public String toString() {
        // 返回便于调试的字符串表示，格式: topic: key value
        return topic + ": " + key + " " + value;
    }
}
