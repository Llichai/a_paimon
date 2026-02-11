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

package org.apache.paimon.partition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 分区过期策略。
 *
 * <p>定义了分区过期的抽象策略,用于确定哪些分区应该被清理。
 * 支持多种过期策略,如基于更新时间或分区值时间的策略。
 */
public abstract class PartitionExpireStrategy {

    /** 分区键列表 */
    protected final List<String> partitionKeys;
    /** 分区默认名称 */
    protected final String partitionDefaultName;
    /** 行数据到对象数组的转换器 */
    private final RowDataToObjectArrayConverter toObjectArrayConverter;

    /**
     * 构造分区过期策略。
     *
     * @param partitionType 分区类型
     * @param partitionDefaultName 分区默认名称
     */
    public PartitionExpireStrategy(RowType partitionType, String partitionDefaultName) {
        this.toObjectArrayConverter = new RowDataToObjectArrayConverter(partitionType);
        this.partitionKeys = partitionType.getFieldNames();
        this.partitionDefaultName = partitionDefaultName;
    }

    /**
     * 将分区值数组转换为分区字符串映射。
     *
     * @param array 分区值数组
     * @return 分区键到分区值的映射
     */
    public Map<String, String> toPartitionString(Object[] array) {
        Map<String, String> map = new LinkedHashMap<>(partitionKeys.size());
        for (int i = 0; i < partitionKeys.size(); i++) {
            map.put(partitionKeys.get(i), array[i].toString());
        }
        return map;
    }

    /**
     * 将分区值数组转换为分区值列表。
     *
     * @param array 分区值数组
     * @return 分区值列表,null值会被替换为默认名称
     */
    public List<String> toPartitionValue(Object[] array) {
        List<String> list = new ArrayList<>(partitionKeys.size());
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (array[i] != null) {
                list.add(array[i].toString());
            } else {
                list.add(partitionDefaultName);
            }
        }
        return list;
    }

    /**
     * 转换分区为对象数组。
     *
     * @param partition 二进制行格式的分区
     * @return 分区对象数组
     */
    public Object[] convertPartition(BinaryRow partition) {
        return toObjectArrayConverter.convert(partition);
    }

    /**
     * 选择已过期的分区。
     *
     * @param scan 文件存储扫描器
     * @param expirationTime 过期时间阈值
     * @return 已过期的分区条目列表
     */
    public abstract List<PartitionEntry> selectExpiredPartitions(
            FileStoreScan scan, LocalDateTime expirationTime);

    /**
     * 创建分区过期策略。
     *
     * @param options 核心配置选项
     * @param partitionType 分区类型
     * @param catalogLoader catalog加载器
     * @param identifier 表标识符
     * @return 分区过期策略实例
     */
    public static PartitionExpireStrategy createPartitionExpireStrategy(
            CoreOptions options,
            RowType partitionType,
            @Nullable CatalogLoader catalogLoader,
            @Nullable Identifier identifier) {
        Optional<PartitionExpireStrategyFactory> custom =
                PartitionExpireStrategyFactory.INSTANCE.get();
        if (custom.isPresent()) {
            try {
                return custom.get().create(catalogLoader, identifier, options, partitionType);
            } catch (UnsupportedOperationException ignored) {
            }
        }

        String strategy = options.partitionExpireStrategy();
        switch (strategy) {
            case "update-time":
                return new PartitionUpdateTimeExpireStrategy(options, partitionType);
            case "values-time":
                return new PartitionValuesTimeExpireStrategy(options, partitionType);
            default:
                throw new IllegalArgumentException("Unknown partitionExpireStrategy: " + strategy);
        }
    }
}
