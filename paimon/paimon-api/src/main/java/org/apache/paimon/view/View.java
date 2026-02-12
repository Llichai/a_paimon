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

package org.apache.paimon.view;

import org.apache.paimon.types.RowType;

import java.util.Map;
import java.util.Optional;

/**
 * 视图定义接口。
 *
 * <p>视图是基于查询语句的虚拟表,不存储实际数据,而是在查询时动态计算结果。
 * Paimon 的视图支持多方言查询、配置选项和元数据管理。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>查询定义</b>: 存储视图的 SQL 查询语句
 *   <li><b>多方言支持</b>: 支持针对不同 SQL 方言的查询定义
 *   <li><b>Schema 管理</b>: 定义视图的字段类型和结构
 *   <li><b>元数据</b>: 支持注释、选项等元数据信息
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建视图
 * View view = catalog.getView(identifier);
 *
 * // 获取视图信息
 * String name = view.name();
 * RowType rowType = view.rowType();
 * String query = view.query();
 *
 * // 获取特定方言的查询
 * String sparkQuery = view.query("spark");
 * String flinkQuery = view.query("flink");
 *
 * // 复制视图并添加动态选项
 * Map<String, String> dynamicOptions = new HashMap<>();
 * dynamicOptions.put("dynamic.option", "value");
 * View newView = view.copy(dynamicOptions);
 * }</pre>
 *
 * <h2>多方言支持</h2>
 * <p>视图可以为不同的 SQL 方言定义不同的查询语句:
 * <ul>
 *   <li>默认查询: 通用的 SQL 查询语句
 *   <li>方言查询: 针对特定引擎(Spark、Flink 等)优化的查询
 *   <li>自动回退: 如果没有特定方言的查询,使用默认查询
 * </ul>
 *
 * <h2>设计理念</h2>
 * <p>视图接口采用轻量级设计:
 * <ul>
 *   <li>不存储实际数据,仅存储查询定义
 *   <li>支持跨引擎的查询复用
 *   <li>提供灵活的元数据管理
 *   <li>支持视图的动态配置
 * </ul>
 *
 * @see ViewChange
 * @see ViewSchema
 * @since 1.0
 */
public interface View {

    /**
     * 视图名称。
     *
     * <p>用于标识此视图的短名称(不包含数据库名)。
     *
     * @return 视图名称
     */
    String name();

    /**
     * 视图全名。
     *
     * <p>包含数据库名的完整视图标识符,格式为 "database.view_name"。
     *
     * @return 视图全名
     */
    String fullName();

    /**
     * 视图的行类型。
     *
     * <p>定义视图输出的字段名称和类型,相当于视图的 Schema。
     *
     * @return 视图的行类型
     */
    RowType rowType();

    /**
     * 视图的查询语句。
     *
     * <p>返回视图的默认 SQL 查询语句,用于定义视图的逻辑。
     *
     * @return 视图的默认查询语句
     */
    String query();

    /**
     * 视图的方言查询映射。
     *
     * <p>返回不同 SQL 方言对应的查询语句映射。键为方言名称(如 "spark", "flink"),
     * 值为对应的查询语句。
     *
     * @return 方言到查询语句的映射
     */
    Map<String, String> dialects();

    /**
     * 获取指定方言的查询语句。
     *
     * <p>如果存在指定方言的查询,返回该查询;否则返回默认查询语句。
     *
     * <p>示例:
     * <pre>{@code
     * String sparkQuery = view.query("spark");
     * // 如果没有 spark 方言的查询,返回默认查询
     * }</pre>
     *
     * @param dialect 方言名称
     * @return 指定方言的查询语句,如果不存在则返回默认查询
     */
    default String query(String dialect) {
        return dialects().getOrDefault(dialect, query());
    }

    /**
     * 视图的注释。
     *
     * <p>返回视图的可选注释信息,用于描述视图的用途和含义。
     *
     * @return 视图注释的 Optional 包装
     */
    Optional<String> comment();

    /**
     * 视图的配置选项。
     *
     * <p>返回视图的配置选项映射,用于存储视图的元数据和配置信息。
     *
     * @return 视图配置选项映射
     */
    Map<String, String> options();

    /**
     * 复制视图并添加动态选项。
     *
     * <p>创建视图的副本,并合并指定的动态选项。动态选项会覆盖原有的相同键值。
     *
     * <p>使用场景:
     * <ul>
     *   <li>在查询时动态设置视图参数
     *   <li>为不同的使用场景定制视图行为
     *   <li>临时覆盖视图配置
     * </ul>
     *
     * @param dynamicOptions 要添加的动态选项
     * @return 包含动态选项的新视图对象
     */
    View copy(Map<String, String> dynamicOptions);
}
