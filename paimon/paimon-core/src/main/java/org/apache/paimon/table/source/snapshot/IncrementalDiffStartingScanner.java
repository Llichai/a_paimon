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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.tag.TagPeriodHandler;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 增量 Diff 起始扫描器
 *
 * <p>通过 {@link SnapshotReader#readIncrementalDiff} 获取两个快照之间的数据差异。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>比较两个快照的完整数据（baseManifestList）
 *   <li>返回增量分片（IncrementalSplit），包含 before/after 文件
 *   <li>支持跨桶数变更检查
 * </ul>
 *
 * <p><b>与 IncrementalDeltaStartingScanner 的区别：</b>
 * <table border="1">
 *   <tr>
 *     <th>对比项</th>
 *     <th>IncrementalDiffStartingScanner</th>
 *     <th>IncrementalDeltaStartingScanner</th>
 *   </tr>
 *   <tr>
 *     <td>读取内容</td>
 *     <td>完整快照比较（baseManifestList）</td>
 *     <td>增量文件聚合（deltaManifestList）</td>
 *   </tr>
 *   <tr>
 *     <td>结果格式</td>
 *     <td>IncrementalSplit（before/after）</td>
 *     <td>DataSplit（after only）</td>
 *   </tr>
 *   <tr>
 *     <td>使用场景</td>
 *     <td>标签间比较、完整差异查询</td>
 *     <td>流式增量读取</td>
 *   </tr>
 *   <tr>
 *     <td>性能</td>
 *     <td>较慢（读取全部文件）</td>
 *     <td>较快（只读取增量文件）</td>
 *   </tr>
 * </table>
 *
 * <p><b>创建方法：</b>
 * <ul>
 *   <li>{@link #betweenTags}：两个标签之间的差异
 *   <li>{@link #betweenSnapshotIds}：两个快照 ID 之间的差异
 *   <li>{@link #betweenTimestamps}：两个时间戳之间的差异
 *   <li>{@link #toEndAutoTag}：从前一个自动标签到指定标签的差异
 * </ul>
 *
 * <p><b>桶数一致性检查：</b>
 * <ul>
 *   <li>如果起始和结束快照使用了不同的 schema
 *   <li>会检查桶数（bucket）配置是否一致
 *   <li>桶数不同会抛出 {@link TimeTravelUtil.InconsistentTagBucketException}
 * </ul>
 *
 * @see SnapshotReader#readIncrementalDiff
 * @see IncrementalDeltaStartingScanner
 */
public class IncrementalDiffStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalDiffStartingScanner.class);

    private final Snapshot start;
    private final Snapshot end;

    public IncrementalDiffStartingScanner(
            SnapshotManager snapshotManager, Snapshot start, Snapshot end) {
        super(snapshotManager);
        this.start = start;
        this.end = end;
        this.startingSnapshotId = start.id();

        TimeTravelUtil.checkRescaleBucketForIncrementalDiffQuery(
                new SchemaManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch()),
                start,
                end);
    }

    @Override
    public Result scan(SnapshotReader reader) {
        return StartingScanner.fromPlan(reader.withSnapshot(end).readIncrementalDiff(start));
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader reader) {
        // ignore start, just use end to read partition entries
        return reader.withSnapshot(end).partitionEntries();
    }

    public static StartingScanner betweenTags(
            Tag startTag,
            Tag endTag,
            SnapshotManager snapshotManager,
            Pair<String, String> incrementalBetween) {
        Snapshot start = startTag.trimToSnapshot();
        Snapshot end = endTag.trimToSnapshot();

        LOG.info(
                "{} start and end are parsed to tag with snapshot id {} to {}.",
                INCREMENTAL_BETWEEN.key(),
                start.id(),
                end.id());

        checkArgument(
                end.id() >= start.id(),
                "Tag end %s with snapshot id %s should be >= tag start %s with snapshot id %s",
                incrementalBetween.getRight(),
                end.id(),
                incrementalBetween.getLeft(),
                start.id());

        if (start.id() == end.id()) {
            return new EmptyResultStartingScanner(snapshotManager);
        }

        return new IncrementalDiffStartingScanner(snapshotManager, start, end);
    }

    public static StartingScanner betweenSnapshotIds(
            long startId, long endId, SnapshotManager snapshotManager) {
        Snapshot start = snapshotManager.snapshot(startId);
        Snapshot end = snapshotManager.snapshot(endId);
        return new IncrementalDiffStartingScanner(snapshotManager, start, end);
    }

    public static IncrementalDiffStartingScanner betweenTimestamps(
            long startTimestamp, long endTimestamp, SnapshotManager snapshotManager) {
        Snapshot startSnapshot = snapshotManager.earlierOrEqualTimeMills(startTimestamp);
        if (startSnapshot == null) {
            startSnapshot = snapshotManager.earliestSnapshot();
        }

        Snapshot endSnapshot = snapshotManager.earlierOrEqualTimeMills(endTimestamp);
        if (endSnapshot == null) {
            endSnapshot = snapshotManager.latestSnapshot();
        }

        return new IncrementalDiffStartingScanner(snapshotManager, startSnapshot, endSnapshot);
    }

    public static AbstractStartingScanner toEndAutoTag(
            SnapshotManager snapshotManager, String endTagName, CoreOptions options) {
        TagPeriodHandler periodHandler = TagPeriodHandler.create(options);
        checkArgument(
                periodHandler.isAutoTag(endTagName),
                "Specified tag '%s' is not an auto-created tag.",
                endTagName);

        TagManager tagManager =
                new TagManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());

        Optional<Tag> endTag = tagManager.get(endTagName);
        if (!endTag.isPresent()) {
            LOG.info("Tag {} doesn't exist.", endTagName);
            return new EmptyResultStartingScanner(snapshotManager);
        }
        Snapshot end = endTag.get().trimToSnapshot();

        LocalDateTime endTagTime = periodHandler.tagToTime(endTagName);

        List<Pair<Tag, LocalDateTime>> previousTags =
                tagManager.tagObjects().stream()
                        .filter(p -> periodHandler.isAutoTag(p.getRight()))
                        .map(p -> Pair.of(p.getLeft(), periodHandler.tagToTime(p.getRight())))
                        .filter(p -> p.getRight().isBefore(endTagTime))
                        .sorted((tag1, tag2) -> tag2.getRight().compareTo(tag1.getRight()))
                        .collect(Collectors.toList());

        if (previousTags.isEmpty()) {
            LOG.info("Didn't found earlier tags for {}.", endTagName);
            return new EmptyResultStartingScanner(snapshotManager);
        }
        LOG.info("Found start tag {} .", periodHandler.timeToTag(previousTags.get(0).getRight()));
        Snapshot start = previousTags.get(0).getLeft().trimToSnapshot();

        return new IncrementalDiffStartingScanner(snapshotManager, start, end);
    }
}
