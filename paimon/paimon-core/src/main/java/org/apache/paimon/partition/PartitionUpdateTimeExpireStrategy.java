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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.types.RowType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 基于分区最后更新时间的过期策略。
 *
 * <p>将分区的最后更新时间与当前时间进行比较,判断分区是否过期。
 */
public class PartitionUpdateTimeExpireStrategy extends PartitionExpireStrategy {

    /**
     * 构造分区更新时间过期策略。
     *
     * @param options 核心配置选项
     * @param partitionType 分区类型
     */
    public PartitionUpdateTimeExpireStrategy(CoreOptions options, RowType partitionType) {
        super(partitionType, options.partitionDefaultName());
    }

    @Override
    public List<PartitionEntry> selectExpiredPartitions(
            FileStoreScan scan, LocalDateTime expirationTime) {
        long expirationMilli =
                expirationTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return scan.readPartitionEntries().stream()
                .filter(partitionEntry -> expirationMilli > partitionEntry.lastFileCreationTime())
                .collect(Collectors.toList());
    }
}
