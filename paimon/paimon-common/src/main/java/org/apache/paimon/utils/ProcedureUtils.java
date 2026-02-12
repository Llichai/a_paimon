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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ExpireConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/**
 * 存储过程工具类。
 *
 * <p>为存储过程提供配置选项的填充和处理功能,主要用于分区过期和快照过期相关的操作。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>分区配置</b> - 填充分区过期相关的配置选项
 *   <li><b>快照配置</b> - 填充快照过期相关的配置选项
 *   <li><b>选项合并</b> - 合并用户提供的额外选项
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>分区过期</b> - 执行分区过期存储过程时配置选项
 *   <li><b>快照过期</b> - 执行快照过期存储过程时配置选项
 *   <li><b>运维操作</b> - 支持表的运维管理操作
 * </ul>
 */
public class ProcedureUtils {

    /**
     * 填充分区过期配置选项。
     *
     * <p>根据提供的参数构建分区过期的动态配置选项,包括过期策略、时间格式、过期时间等。
     * 检查间隔会被自动设置为 0,表示专用的分区过期操作。
     *
     * @param expireStrategy 过期策略,如 "values-time" 或 "update-time"
     * @param timestampFormatter 时间戳格式化器,用于解析分区值中的时间
     * @param timestampPattern 时间戳模式,用于提取分区值中的时间部分
     * @param expirationTime 过期时间,如 "7 d" 表示 7 天
     * @param maxExpires 单次最多过期的分区数,用于限制删除规模
     * @param options 额外的选项,格式为 "key1=value1,key2=value2"
     * @return 包含所有配置选项的映射
     */
    public static Map<String, String> fillInPartitionOptions(
            String expireStrategy,
            String timestampFormatter,
            String timestampPattern,
            String expirationTime,
            Integer maxExpires,
            String options) {

        HashMap<String, String> dynamicOptions = new HashMap<>();
        putAllOptions(dynamicOptions, options);
        putIfNotEmpty(
                dynamicOptions, CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        putIfNotEmpty(
                dynamicOptions,
                CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(),
                timestampFormatter);
        putIfNotEmpty(
                dynamicOptions, CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);
        putIfNotEmpty(dynamicOptions, CoreOptions.PARTITION_EXPIRATION_TIME.key(), expirationTime);
        // Set check interval to 0 for dedicated partition expiration.
        putIfNotEmpty(dynamicOptions, CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "0");
        putIfNotEmpty(
                dynamicOptions,
                CoreOptions.PARTITION_EXPIRATION_MAX_NUM.key(),
                maxExpires == null ? null : String.valueOf(maxExpires));
        return dynamicOptions;
    }

    /**
     * 将额外选项添加到动态配置中。
     *
     * <p>解析逗号分隔的键值对字符串,并将其添加到配置映射中。
     *
     * @param dynamicOptions 目标配置映射
     * @param options 选项字符串,格式为 "key1=value1,key2=value2"
     */
    public static void putAllOptions(HashMap<String, String> dynamicOptions, String options) {
        if (!StringUtils.isNullOrWhitespaceOnly(options)) {
            dynamicOptions.putAll(ParameterUtils.parseCommaSeparatedKeyValues(options));
        }
    }

    /**
     * 如果值非空则添加到配置映射中。
     *
     * <p>只有当值不为 null 且不是空白字符串时,才将键值对添加到映射中。
     *
     * @param dynamicOptions 目标配置映射
     * @param key 配置键
     * @param value 配置值,可为 null 或空白
     */
    public static void putIfNotEmpty(
            HashMap<String, String> dynamicOptions, String key, String value) {
        if (!StringUtils.isNullOrWhitespaceOnly(value)) {
            dynamicOptions.put(key, value);
        }
    }

    /**
     * 填充快照过期配置选项。
     *
     * <p>根据提供的参数和表选项构建快照过期的配置。如果参数为 null,则使用表的默认配置。
     * 支持通过时间戳字符串 (olderThanStr) 指定保留时间。
     *
     * @param tableOptions 表的核心配置选项
     * @param retainMax 最多保留的快照数,null 则使用表配置
     * @param retainMin 最少保留的快照数,null 则使用表配置
     * @param olderThanStr 时间戳字符串,表示早于此时间的快照将被删除,格式如 "2024-01-01 00:00:00"
     * @param maxDeletes 单次最多删除的快照数,null 则使用表配置
     * @return 快照过期配置构建器
     */
    public static ExpireConfig.Builder fillInSnapshotOptions(
            CoreOptions tableOptions,
            Integer retainMax,
            Integer retainMin,
            String olderThanStr,
            Integer maxDeletes) {

        ExpireConfig.Builder builder = ExpireConfig.builder();
        builder.snapshotRetainMax(
                        Optional.ofNullable(retainMax).orElse(tableOptions.snapshotNumRetainMax()))
                .snapshotRetainMin(
                        Optional.ofNullable(retainMin).orElse(tableOptions.snapshotNumRetainMin()))
                .snapshotMaxDeletes(
                        Optional.ofNullable(maxDeletes).orElse(tableOptions.snapshotExpireLimit()))
                .snapshotTimeRetain(tableOptions.snapshotTimeRetain());
        if (!StringUtils.isNullOrWhitespaceOnly(olderThanStr)) {
            long olderThanMills =
                    DateTimeUtils.parseTimestampData(olderThanStr, 3, TimeZone.getDefault())
                            .getMillisecond();
            builder.snapshotTimeRetain(
                    Duration.ofMillis(System.currentTimeMillis() - olderThanMills));
        }
        return builder;
    }
}
