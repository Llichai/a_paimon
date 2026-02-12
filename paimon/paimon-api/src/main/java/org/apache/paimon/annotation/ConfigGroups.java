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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 配置选项分组注解。
 *
 * <p>该注解用于包含配置选项的类上,支持根据键前缀将配置选项分离到不同的表中。
 * 通过定义多个配置组,可以实现配置的逻辑分类和结构化管理,便于生成结构清晰的配置文档。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>自动分组</b>: 根据配置键前缀自动将选项分配到对应的组
 *   <li><b>前缀匹配</b>: 支持最长前缀匹配,确保精确分组
 *   <li><b>默认分组</b>: 不匹配任何前缀的选项自动归入默认组
 *   <li><b>唯一分配</b>: 每个配置选项只会被分配到一个组
 * </ul>
 *
 * <h2>分组规则</h2>
 * <ol>
 *   <li>配置选项根据键与配置组的前缀进行匹配
 *   <li>如果键匹配多个前缀,选择最长的匹配前缀
 *   <li>配置选项永远不会被分配到多个组
 *   <li>不匹配任何组的选项会被隐式添加到默认组
 * </ol>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 为配置类定义多个分组
 * @ConfigGroups(groups = {
 *     @ConfigGroup(name = "Memory", keyPrefix = "memory"),
 *     @ConfigGroup(name = "Compaction", keyPrefix = "compaction"),
 *     @ConfigGroup(name = "Snapshot", keyPrefix = "snapshot")
 * })
 * public class CoreOptions {
 *     // Memory 组
 *     public static final ConfigOption<MemorySize> MEMORY_POOL_SIZE =
 *         ConfigOption.key("memory.pool.size")...;
 *
 *     // Compaction 组 (最长匹配)
 *     public static final ConfigOption<Integer> COMPACTION_MIN_FILE_NUM =
 *         ConfigOption.key("compaction.min.file-num")...;
 *
 *     // Snapshot 组
 *     public static final ConfigOption<Long> SNAPSHOT_NUM_RETAINED =
 *         ConfigOption.key("snapshot.num-retained")...;
 *
 *     // 默认组 (不匹配任何前缀)
 *     public static final ConfigOption<String> TABLE_TYPE =
 *         ConfigOption.key("table.type")...;
 * }
 * }</pre>
 *
 * <h2>最长前缀匹配示例</h2>
 * <pre>{@code
 * @ConfigGroups(groups = {
 *     @ConfigGroup(name = "Compaction", keyPrefix = "compaction"),
 *     @ConfigGroup(name = "Early Compaction", keyPrefix = "compaction.early")
 * })
 * public class CompactionOptions {
 *     // 匹配 "Compaction" 组
 *     ConfigOption key = "compaction.min.file-num";
 *
 *     // 匹配 "Early Compaction" 组 (更长的前缀)
 *     ConfigOption key = "compaction.early.max-file-num";
 * }
 * }</pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li><b>配置文档生成</b>: 生成结构化的配置文档,按组分类显示
 *   <li><b>配置管理</b>: 将相关的配置选项逻辑分组管理
 *   <li><b>用户体验</b>: 帮助用户更容易理解和查找配置选项
 * </ul>
 *
 * <h2>设计理念</h2>
 * <p>该注解采用声明式配置分组方式:
 * <ul>
 *   <li>通过注解声明式地定义配置组结构
 *   <li>运行时自动根据前缀匹配规则分配选项
 *   <li>支持嵌套前缀,实现层次化分组
 *   <li>简化配置文档的生成和维护工作
 * </ul>
 *
 * @see ConfigGroup
 * @since 1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigGroups {
    /**
     * 配置组数组。
     *
     * <p>定义该类中包含的所有配置组。配置选项会根据键前缀自动分配到相应的组中。
     * 如果不指定任何组,所有配置选项都会被分配到默认组。
     *
     * @return 配置组数组,默认为空数组
     */
    ConfigGroup[] groups() default {};
}
