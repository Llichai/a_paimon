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

package org.apache.paimon.annotation;

import java.lang.annotation.Target;

/**
 * 配置选项组注解。
 *
 * <p>该注解用于指定一组配置选项的分组信息,是 {@link ConfigGroups} 注解的组成部分。
 * 配置选项会根据键前缀被分配到相应的配置组中,便于配置的分类管理和文档生成。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>分组定义</b>: 定义配置选项的逻辑分组
 *   <li><b>前缀匹配</b>: 通过键前缀自动分配配置选项
 *   <li><b>文档生成</b>: 为配置文档生成提供分组信息
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 在 ConfigGroups 注解中使用
 * @ConfigGroups(groups = {
 *     @ConfigGroup(name = "Compaction", keyPrefix = "compaction"),
 *     @ConfigGroup(name = "Snapshot", keyPrefix = "snapshot")
 * })
 * public class CoreOptions {
 *     // compaction.* 选项会被分配到 Compaction 组
 *     public static final ConfigOption<Integer> COMPACTION_THREADS = ...;
 *
 *     // snapshot.* 选项会被分配到 Snapshot 组
 *     public static final ConfigOption<Long> SNAPSHOT_NUM_RETAINED = ...;
 * }
 * }</pre>
 *
 * <h2>设计理念</h2>
 * <p>该注解采用基于前缀的自动分组机制:
 * <ul>
 *   <li>配置选项根据键前缀自动分配到对应的组
 *   <li>当一个键匹配多个前缀时,选择最长的匹配前缀
 *   <li>不匹配任何前缀的选项会被分配到默认组
 *   <li>每个选项只会被分配到一个组中
 * </ul>
 *
 * @see ConfigGroups
 * @since 1.0
 */
@Target({})
public @interface ConfigGroup {
    /**
     * 配置组的名称。
     *
     * <p>该名称用于文档生成和分组显示,应使用清晰的描述性名称。
     *
     * @return 配置组的名称
     */
    String name();

    /**
     * 配置键的前缀。
     *
     * <p>配置选项的键如果以该前缀开头,则会被自动分配到此配置组中。
     * 前缀匹配采用最长匹配原则。
     *
     * <p>示例:
     * <ul>
     *   <li>前缀 "compaction" 匹配 "compaction.min.file-num"
     *   <li>前缀 "snapshot" 匹配 "snapshot.num-retained"
     * </ul>
     *
     * @return 配置键的前缀字符串
     */
    String keyPrefix();
}
