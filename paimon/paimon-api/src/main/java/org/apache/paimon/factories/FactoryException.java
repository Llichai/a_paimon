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
 * 工厂异常类,表示工厂操作中发生的错误。
 *
 * <p>该异常用于指示工厂发现、创建或使用过程中的问题,例如:
 * <ul>
 *   <li>找不到指定的工厂实现
 *   <li>发现多个匹配的工厂实现(歧义)
 *   <li>工厂初始化失败
 *   <li>工厂配置错误
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>工厂未找到</b>: 指定的标识符没有对应的工厂实现
 *   <li><b>工厂歧义</b>: 多个工厂具有相同的标识符
 *   <li><b>加载失败</b>: SPI 加载工厂时出错
 *   <li><b>配置错误</b>: 工厂所需的配置参数缺失或无效
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * try {
 *     Factory factory = FactoryUtil.discoverFactory(
 *         classLoader, CatalogFactory.class, "unknown");
 * } catch (FactoryException e) {
 *     // 处理工厂异常
 *     log.error("工厂发现失败: {}", e.getMessage());
 * }
 * }</pre>
 *
 * @see Factory
 * @see FactoryUtil
 * @since 1.0
 */
public class FactoryException extends RuntimeException {
    /**
     * 构造一个新的工厂异常。
     *
     * @param message 异常消息,描述错误的详细信息
     */
    public FactoryException(String message) {
        super(message);
    }
}
