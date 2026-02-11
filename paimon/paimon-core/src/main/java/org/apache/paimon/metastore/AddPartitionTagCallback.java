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

import org.apache.paimon.table.PartitionHandler;
import org.apache.paimon.table.sink.TagCallback;

import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * 将新创建的分区添加到元数据存储的 {@link TagCallback} 实现。
 *
 * <p><b>功能说明：</b>
 * 当创建或删除标签（Tag）时，自动在元数据存储中添加或删除对应的分区。
 * 这种机制将 Paimon 的标签映射为元数据存储的分区，便于外部系统管理和查询。
 *
 * <p><b>标签到分区的映射：</b>
 * 每个标签名称对应一个分区值，例如：
 * <ul>
 *   <li>标签 "v1.0" -> 分区 {tag_field: "v1.0"}
 *   <li>标签 "snapshot-20230101" -> 分区 {tag_field: "snapshot-20230101"}
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>版本管理：将标签作为分区在 Hive 中展示
 *   <li>时间分区：为时间点标签创建对应的时间分区
 *   <li>外部访问：使外部系统能够通过分区方式访问标签数据
 * </ul>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>标签字段必须是表的分区字段
 *   <li>标签名称将作为该分区字段的值
 *   <li>删除标签时会同步删除对应的分区
 * </ul>
 *
 * @see TagCallback
 * @see PartitionHandler
 */
public class AddPartitionTagCallback implements TagCallback {

    /** 分区处理器，用于在元数据存储中创建/删除分区。 */
    private final PartitionHandler partitionHandler;

    /** 分区字段名称，标签名将作为该字段的分区值。 */
    private final String partitionField;

    /**
     * 构造函数。
     *
     * @param partitionHandler 分区处理器
     * @param partitionField 分区字段名称
     */
    public AddPartitionTagCallback(PartitionHandler partitionHandler, String partitionField) {
        this.partitionHandler = partitionHandler;
        this.partitionField = partitionField;
    }

    /**
     * 当标签创建时调用。
     *
     * <p>在元数据存储中创建一个新分区，分区值为标签名。
     *
     * @param tagName 标签名称
     */
    @Override
    public void notifyCreation(String tagName) {
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put(partitionField, tagName);
        try {
            partitionHandler.createPartitions(Collections.singletonList(partitionSpec));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 当标签删除时调用。
     *
     * <p>从元数据存储中删除对应的分区。
     *
     * @param tagName 标签名称
     */
    @Override
    public void notifyDeletion(String tagName) {
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put(partitionField, tagName);
        try {
            partitionHandler.dropPartitions(Collections.singletonList(partitionSpec));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭回调，释放资源。
     *
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {
        partitionHandler.close();
    }
}
