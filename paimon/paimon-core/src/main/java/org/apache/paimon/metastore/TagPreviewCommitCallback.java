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

package org.apache.paimon.metastore;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.tag.TagPreview;

import java.util.List;
import java.util.Optional;

/**
 * 用于标签预览的分区添加回调。
 *
 * <p><b>功能说明：</b>
 * 当提交成功时，根据快照的水位线（watermark）自动创建标签预览，
 * 并在元数据存储中添加对应的分区。
 *
 * <p><b>标签预览机制：</b>
 * <ul>
 *   <li>根据时间规则自动提取标签名称（如每小时/每天）
 *   <li>为符合条件的快照创建标签
 *   <li>在元数据存储中创建对应的分区
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>定时快照：按时间间隔自动创建快照标签
 *   <li>数据版本：为每个数据版本自动创建标签和分区
 *   <li>外部可见：使外部系统能够通过分区方式访问标签数据
 * </ul>
 *
 * <p><b>工作流程：</b>
 * <ol>
 *   <li>提交成功后，提取快照的水位线和提交时间
 *   <li>调用 {@link TagPreview#extractTag} 判断是否需要创建标签
 *   <li>如果需要，调用 {@link AddPartitionTagCallback} 创建分区
 * </ol>
 *
 * @see TagPreview
 * @see AddPartitionTagCallback
 */
public class TagPreviewCommitCallback implements CommitCallback {

    /** 标签回调，用于在元数据存储中创建分区。 */
    private final AddPartitionTagCallback tagCallback;

    /** 标签预览器，用于根据时间规则提取标签名称。 */
    private final TagPreview tagPreview;

    /**
     * 构造函数。
     *
     * @param tagCallback 标签回调
     * @param tagPreview 标签预览器
     */
    public TagPreviewCommitCallback(AddPartitionTagCallback tagCallback, TagPreview tagPreview) {
        this.tagCallback = tagCallback;
        this.tagPreview = tagPreview;
    }

    /**
     * 提交成功后的回调。
     *
     * <p>根据快照信息判断是否需要创建标签，并通知标签回调。
     *
     * @param baseFiles 基础文件列表（未使用）
     * @param deltaFiles 增量文件列表（未使用）
     * @param indexFiles 索引文件列表（未使用）
     * @param snapshot 快照信息，包含水位线
     */
    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {
        long currentMillis = System.currentTimeMillis();
        Optional<String> tagOptional = tagPreview.extractTag(currentMillis, snapshot.watermark());
        tagOptional.ifPresent(tagCallback::notifyCreation);
    }

    /**
     * 重试提交时的回调。
     *
     * <p>与 call 方法类似，但使用提交消息中的水位线。
     *
     * @param committable 提交消息
     */
    @Override
    public void retry(ManifestCommittable committable) {
        long currentMillis = System.currentTimeMillis();
        Optional<String> tagOptional =
                tagPreview.extractTag(currentMillis, committable.watermark());
        tagOptional.ifPresent(tagCallback::notifyCreation);
    }

    /**
     * 关闭回调，释放资源。
     *
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {
        tagCallback.close();
    }
}
