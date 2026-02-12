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

package org.apache.paimon.factories;

/**
 * 工厂基础接口,用于从配置键值对创建对象实例。
 *
 * <p>该接口是 Paimon 工厂模式的核心抽象,所有类型的工厂都应实现此接口。
 * 工厂从 Paimon 的 catalog 中的键值对列表创建对象实例。
 *
 * <h2>唯一标识</h2>
 * <p>工厂通过 {@link Class} 和 {@link #identifier()} 唯一标识:
 * <ul>
 *   <li><b>Class</b>: 工厂接口类型(如 CatalogFactory、FileSystemFactory)
 *   <li><b>identifier</b>: 工厂的字符串标识符(如 "filesystem"、"hive")
 * </ul>
 *
 * <h2>SPI 发现机制</h2>
 * <p>可用的工厂通过 Java 的服务提供者接口(SPI)机制发现:
 * <ul>
 *   <li>实现此接口的类可添加到 JAR 文件中的 {@code META-INF/services/org.apache.paimon.factories.Factory}
 *   <li>系统会自动扫描和加载所有注册的工厂实现
 *   <li>通过标识符查找和实例化具体的工厂
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 实现自定义工厂
 * public class MyCustomFactory implements Factory {
 *     @Override
 *     public String identifier() {
 *         return "my-custom";  // 唯一标识符
 *     }
 * }
 *
 * // 在 META-INF/services/org.apache.paimon.factories.Factory 中注册:
 * // com.example.MyCustomFactory
 *
 * // 通过工厂工具类发现和使用
 * Factory factory = FactoryUtil.discoverFactory(
 *     classLoader, Factory.class, "my-custom");
 * }</pre>
 *
 * <h2>标识符命名规范</h2>
 * <ul>
 *   <li>使用小写单词(如 {@code kafka}、{@code filesystem})
 *   <li>多个版本时用 "-" 分隔版本号(如 {@code elasticsearch-7})
 *   <li>保持简洁且具有描述性
 * </ul>
 *
 * @see FactoryUtil
 * @see FactoryException
 * @since 1.0
 */
public interface Factory {
    /**
     * 返回工厂的唯一标识符。
     *
     * <p>该标识符在相同工厂接口的所有实现中必须唯一。
     *
     * <h3>命名约定</h3>
     * <p>为保持一致性,标识符应遵循以下规则:
     * <ul>
     *   <li>使用一个小写单词(例如 {@code kafka}、{@code hive})
     *   <li>如果存在多个版本,使用 "-" 附加版本号(例如 {@code elasticsearch-7})
     *   <li>避免使用特殊字符和空格
     * </ul>
     *
     * <h3>示例</h3>
     * <ul>
     *   <li>{@code filesystem} - 文件系统工厂
     *   <li>{@code hive} - Hive catalog 工厂
     *   <li>{@code jdbc-postgres} - PostgreSQL JDBC 工厂
     * </ul>
     *
     * @return 工厂的唯一标识符字符串
     */
    String identifier();
}
