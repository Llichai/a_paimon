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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.PARTITION_EXPIRATION_STRATEGY;

/**
 * 基于分区值时间的过期策略。
 *
 * <p>将从分区值中提取的时间与当前时间进行比较,判断分区是否过期。
 */
public class PartitionValuesTimeExpireStrategy extends PartitionExpireStrategy {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionValuesTimeExpireStrategy.class);

    /** 分区时间提取器 */
    private final PartitionTimeExtractor timeExtractor;

    /**
     * 构造分区值时间过期策略。
     *
     * @param options 核心配置选项
     * @param partitionType 分区类型
     */
    public PartitionValuesTimeExpireStrategy(CoreOptions options, RowType partitionType) {
        super(partitionType, options.partitionDefaultName());
        String timePattern = options.partitionTimestampPattern();
        String timeFormatter = options.partitionTimestampFormatter();
        this.timeExtractor = new PartitionTimeExtractor(timePattern, timeFormatter);
    }

    /**
     * 选择已过期的分区。
     *
     * @param scan 文件存储扫描器
     * @param expirationTime 过期时间阈值
     * @return 已过期的分区条目列表
     */
    @Override
    public List<PartitionEntry> selectExpiredPartitions(
            FileStoreScan scan, LocalDateTime expirationTime) {
        return scan.withPartitionFilter(new PartitionValuesTimePredicate(expirationTime))
                .readPartitionEntries();
    }

    /**
     * 判断分区是否过期。
     *
     * @param expireDateTime 过期时间阈值
     * @param partition 分区数据
     * @return 过期返回true,否则返回false
     */
    public boolean isExpired(LocalDateTime expireDateTime, BinaryRow partition) {
        return new PartitionValuesTimePredicate(expireDateTime).test(partition);
    }

    /**
     * 使用分区日期格式值的过期分区谓词。
     */
    private class PartitionValuesTimePredicate implements PartitionPredicate {

        /** 过期日期时间 */
        private final LocalDateTime expireDateTime;

        /**
         * 构造分区值时间谓词。
         *
         * @param expireDateTime 过期日期时间
         */
        private PartitionValuesTimePredicate(LocalDateTime expireDateTime) {
            this.expireDateTime = expireDateTime;
        }

        @Override
        public boolean test(BinaryRow partition) {
            Object[] array = convertPartition(partition);
            try {
                LocalDateTime partTime = timeExtractor.extract(partitionKeys, Arrays.asList(array));
                return expireDateTime.isAfter(partTime);
            } catch (DateTimeParseException e) {
                LOG.warn(
                        "Can't extract datetime from partition {}. If you want to configure partition expiration, please:\n"
                                + "  1. Check the expiration configuration.\n"
                                + "  2. Manually delete the partition using the drop-partition command if the partition"
                                + " value is non-date formatted.\n"
                                + "  3. Use 'update-time' expiration strategy by set '{}', which supports non-date formatted partition.",
                        formatPartitionInfo(array),
                        PARTITION_EXPIRATION_STRATEGY.key());
                return false;
            } catch (NullPointerException e) {
                // there might exist NULL partition value
                LOG.warn(
                        "This partition {} cannot be expired because it contains null value. "
                                + "You can try to drop it manually or use 'update-time' expiration strategy by set '{}'.",
                        formatPartitionInfo(array),
                        PARTITION_EXPIRATION_STRATEGY.key());
                return false;
            }
        }

        private String formatPartitionInfo(Object[] array) {
            return IntStream.range(0, partitionKeys.size())
                    .mapToObj(i -> partitionKeys.get(i) + ":" + array[i])
                    .collect(Collectors.joining(","));
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PartitionValuesTimePredicate that = (PartitionValuesTimePredicate) o;
            return Objects.equals(expireDateTime, that.expireDateTime);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(expireDateTime);
        }
    }
}
