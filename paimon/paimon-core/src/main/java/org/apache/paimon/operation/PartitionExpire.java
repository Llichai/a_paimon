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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;
import org.apache.paimon.table.PartitionHandler;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * 分区过期管理器
 *
 * <p>负责根据时间策略自动删除过期的分区数据。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li>基于时间的分区过期：根据 {@code expirationTime} 判断分区是否过期
 *   <li>定期检查机制：按 {@code checkInterval} 定期检查过期分区
 *   <li>批量删除：支持批量删除过期分区以提高效率
 *   <li>数量限制：支持设置单次删除的最大分区数
 * </ul>
 *
 * <h2>过期策略</h2>
 * <p>使用 {@link PartitionExpireStrategy} 确定分区是否过期：
 * <ul>
 *   <li><b>分区时间策略</b>：基于分区创建时间判断（使用 Manifest 中的时间戳）
 *   <li><b>分区值策略</b>：基于分区字段值判断（如日期分区字段）
 * </ul>
 *
 * <h2>时间判断逻辑</h2>
 * <pre>
 * 过期判断公式：
 * expireDateTime = currentDateTime - expirationTime
 * 如果 partitionTime < expireDateTime，则该分区过期
 *
 * 示例：
 * expirationTime = 7天
 * currentDateTime = 2024-01-10
 * expireDateTime = 2024-01-03
 * 分区 dt=2024-01-02 将被删除（< 2024-01-03）
 * 分区 dt=2024-01-04 将被保留（>= 2024-01-03）
 * </pre>
 *
 * <h2>检查时机</h2>
 * <ol>
 *   <li><b>定期检查</b>：每隔 {@code checkInterval} 检查一次
 *   <li><b>立即检查</b>：{@code checkInterval} 为 0 时，每次调用都检查
 *   <li><b>结束时检查</b>：{@code endInputCheckPartitionExpire} 为 true 时，在输入结束时强制检查
 * </ol>
 *
 * <h2>删除流程</h2>
 * <ol>
 *   <li>根据策略选择过期的分区
 *   <li>限制删除数量（不超过 {@code maxExpireNum}）
 *   <li>批量删除（每批最多 {@code expireBatchSize} 个）
 *   <li>如果提供了 {@code partitionHandler}，调用外部删除逻辑
 *   <li>否则调用 {@code commit.dropPartitions} 在表内删除
 * </ol>
 *
 * @see PartitionExpireStrategy 分区过期策略接口
 * @see FileStoreCommit#dropPartitions 分区删除方法
 */
public class PartitionExpire {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionExpire.class);

    private static final String DELIMITER = ",";

    /** 过期时间：分区创建后多久过期 */
    private final Duration expirationTime;

    /** 检查间隔：多久检查一次过期分区 */
    private final Duration checkInterval;

    private final FileStoreScan scan;
    private final FileStoreCommit commit;

    /** 分区处理器：用于删除外部表的分区（如 Hive 表） */
    @Nullable private final PartitionHandler partitionHandler;

    /** 上次检查时间：避免频繁检查 */
    private LocalDateTime lastCheck;

    /** 过期策略：确定分区是否过期的策略 */
    private final PartitionExpireStrategy strategy;

    /** 是否在输入结束时检查分区过期 */
    private final boolean endInputCheckPartitionExpire;

    /** 单次最多删除多少个分区 */
    private final int maxExpireNum;

    /** 批量删除的批次大小 */
    private final int expireBatchSize;

    public PartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable PartitionHandler partitionHandler,
            boolean endInputCheckPartitionExpire,
            int maxExpireNum,
            int expireBatchSize) {
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.strategy = strategy;
        this.scan = scan;
        this.commit = commit;
        this.partitionHandler = partitionHandler;
        // 避免流式作业执行时间过短导致分区过期功能无法执行
        // 随机初始化上次检查时间，避免所有任务同时检查
        long rndSeconds = 0;
        long checkIntervalSeconds = checkInterval.toMillis() / 1000;
        if (checkIntervalSeconds > 0) {
            rndSeconds = ThreadLocalRandom.current().nextLong(checkIntervalSeconds);
        }
        this.lastCheck = LocalDateTime.now().minusSeconds(rndSeconds);
        this.endInputCheckPartitionExpire = endInputCheckPartitionExpire;
        this.maxExpireNum = maxExpireNum;
        this.expireBatchSize = expireBatchSize;
    }

    public PartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable PartitionHandler partitionHandler,
            int maxExpireNum,
            int expireBatchSize) {
        this(
                expirationTime,
                checkInterval,
                strategy,
                scan,
                commit,
                partitionHandler,
                false,
                maxExpireNum,
                expireBatchSize);
    }

    /**
     * 执行分区过期检查和删除
     *
     * @param commitIdentifier 提交标识符
     * @return 删除的分区列表，如果未到检查时间则返回 null
     */
    public List<Map<String, String>> expire(long commitIdentifier) {
        return expire(LocalDateTime.now(), commitIdentifier);
    }

    /**
     * 判断是否使用分区值过期策略
     */
    public boolean isValueExpiration() {
        return strategy instanceof PartitionValuesTimeExpireStrategy;
    }

    /**
     * 判断所有分区是否都已过期
     *
     * <p>用于分区值过期策略，检查一批分区是否全部过期。
     *
     * @param partitions 待检查的分区列表
     * @return 如果所有分区都过期则返回 true
     */
    public boolean isValueAllExpired(Collection<BinaryRow> partitions) {
        PartitionValuesTimeExpireStrategy valuesStrategy =
                (PartitionValuesTimeExpireStrategy) strategy;
        LocalDateTime expireDateTime = LocalDateTime.now().minus(expirationTime);
        for (BinaryRow partition : partitions) {
            if (!valuesStrategy.isExpired(expireDateTime, partition)) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    void setLastCheck(LocalDateTime time) {
        lastCheck = time;
    }

    /**
     * 执行分区过期检查和删除（内部实现）
     *
     * <h3>检查条件：</h3>
     * <ul>
     *   <li>{@code checkInterval} 为 0：每次都检查</li>
     *   <li>当前时间超过上次检查时间 + 检查间隔：定期检查</li>
     *   <li>{@code endInputCheckPartitionExpire} 为 true 且 commitIdentifier 为 MAX_VALUE：结束时检查</li>
     * </ul>
     *
     * @param now 当前时间
     * @param commitIdentifier 提交标识符
     * @return 删除的分区列表，如果未到检查时间则返回 null
     */
    @VisibleForTesting
    List<Map<String, String>> expire(LocalDateTime now, long commitIdentifier) {
        if (checkInterval.isZero()
                || now.isAfter(lastCheck.plus(checkInterval))
                || (endInputCheckPartitionExpire && Long.MAX_VALUE == commitIdentifier)) {
            List<Map<String, String>> expired =
                    doExpire(now.minus(expirationTime), commitIdentifier);
            lastCheck = now;
            return expired;
        }
        return null;
    }

    /**
     * 执行实际的过期和删除操作
     *
     * <h3>处理流程：</h3>
     * <ol>
     *   <li>使用策略选择过期的分区</li>
     *   <li>转换分区值为分区字符串</li>
     *   <li>限制删除数量</li>
     *   <li>批量删除过期分区</li>
     * </ol>
     *
     * @param expireDateTime 过期时间阈值
     * @param commitIdentifier 提交标识符
     * @return 删除的分区列表
     */
    private List<Map<String, String>> doExpire(
            LocalDateTime expireDateTime, long commitIdentifier) {
        // 选择过期的分区
        List<PartitionEntry> partitionEntries =
                strategy.selectExpiredPartitions(scan, expireDateTime);
        List<List<String>> expiredPartValues = new ArrayList<>(partitionEntries.size());
        for (PartitionEntry partition : partitionEntries) {
            Object[] array = strategy.convertPartition(partition.partition());
            expiredPartValues.add(strategy.toPartitionValue(array));
        }

        List<Map<String, String>> expired = new ArrayList<>();
        if (!expiredPartValues.isEmpty()) {
            // 转换分区值为分区字符串，并限制分区数量
            expired = convertToPartitionString(expiredPartValues);
            LOG.info("Expire Partitions: {}", expired);
            // 批量删除
            if (expireBatchSize > 0 && expireBatchSize < expired.size()) {
                // 分批删除，避免一次性删除过多分区
                Lists.partition(expired, expireBatchSize)
                        .forEach(
                                expiredBatchPartitions ->
                                        doBatchExpire(expiredBatchPartitions, commitIdentifier));
            } else {
                // 一次性删除所有过期分区
                doBatchExpire(expired, commitIdentifier);
            }
        }
        return expired;
    }

    /**
     * 批量删除过期分区
     *
     * @param expiredBatchPartitions 待删除的分区批次
     * @param commitIdentifier 提交标识符
     */
    private void doBatchExpire(
            List<Map<String, String>> expiredBatchPartitions, long commitIdentifier) {
        if (partitionHandler != null) {
            // 使用外部分区处理器删除（如 Hive 表）
            try {
                partitionHandler.dropPartitions(expiredBatchPartitions);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 使用提交器在表内删除
            commit.dropPartitions(expiredBatchPartitions, commitIdentifier);
        }
    }

    /**
     * 转换分区值为分区字符串映射
     *
     * <p>限制删除数量不超过 {@code maxExpireNum}。
     *
     * @param expiredPartValues 过期分区值列表
     * @return 分区字符串映射列表
     */
    private List<Map<String, String>> convertToPartitionString(
            List<List<String>> expiredPartValues) {
        return expiredPartValues.stream()
                .map(values -> String.join(DELIMITER, values))
                .sorted()
                // 使用 split(DELIMITER, -1) 保留尾部空字符串
                .map(s -> s.split(DELIMITER, -1))
                .map(strategy::toPartitionString)
                .limit(Math.min(expiredPartValues.size(), maxExpireNum))
                .collect(Collectors.toList());
    }
}
