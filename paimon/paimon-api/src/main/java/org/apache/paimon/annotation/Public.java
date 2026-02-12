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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 公共稳定接口注解。
 *
 * <p>该注解用于标记公共的、稳定的 API 接口。被标记为公共接口的类表示其 API
 * 已经稳定,遵循向后兼容性原则,可以安全地在生产环境中使用。
 *
 * <h2>主要特点</h2>
 * <ul>
 *   <li><b>稳定性</b>: API 接口经过充分验证,不会轻易变更
 *   <li><b>向后兼容</b>: 遵循语义化版本规则,保证向后兼容
 *   <li><b>生产就绪</b>: 可以在生产环境中安全使用
 *   <li><b>长期支持</b>: 提供长期的支持和维护
 * </ul>
 *
 * <h2>兼容性保证</h2>
 * <p>标记为 @Public 的 API 承诺:
 * <ul>
 *   <li>在主版本(major version)内保持向后兼容
 *   <li>方法签名不会在次版本(minor version)中改变
 *   <li>弃用的 API 会在移除前至少保留一个主版本
 *   <li>任何破坏性变更都会在发行说明中明确说明
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 公共稳定的表接口
 * @Public
 * public interface Table {
 *     String name();
 *     Schema schema();
 *     // ... 其他稳定的公共方法
 * }
 *
 * // 公共稳定的 Catalog 接口
 * @Public
 * public interface Catalog {
 *     List<String> listDatabases();
 *     List<String> listTables(String databaseName);
 *     Table getTable(Identifier identifier);
 *     // ... 其他稳定的公共方法
 * }
 * }</pre>
 *
 * <h2>标记准则</h2>
 * <p>以下情况应该标记为 @Public:
 * <ul>
 *   <li>面向用户的主要 API 接口
 *   <li>经过充分测试和验证的功能
 *   <li>文档完善,使用场景明确
 *   <li>已经在多个版本中保持稳定
 * </ul>
 *
 * <h2>不应标记的情况</h2>
 * <ul>
 *   <li>内部实现类(应使用 package-private 或 @VisibleForTesting)
 *   <li>实验性功能(应使用 @Experimental)
 *   <li>仅用于框架内部的接口
 *   <li>可能发生变更的 API
 * </ul>
 *
 * <h2>版本演进</h2>
 * <p>公共 API 的演进路径:
 * <ol>
 *   <li>初始实现,标记为 @Experimental
 *   <li>经过验证和改进,API 稳定
 *   <li>标记为 @Public,提供兼容性保证
 *   <li>如需弃用,先添加 @Deprecated 注解
 *   <li>在后续主版本中移除弃用的 API
 * </ol>
 *
 * <h2>与其他注解的关系</h2>
 * <ul>
 *   <li>{@link Experimental}: 实验性 API,与 @Public 互斥
 *   <li>{@link VisibleForTesting}: 仅用于测试,不是公共 API
 *   <li>@Deprecated: 可以与 @Public 共存,表示即将移除的公共 API
 * </ul>
 *
 * <h2>设计理念</h2>
 * <p>该注解体现了 Paimon 的 API 设计理念:
 * <ul>
 *   <li>明确区分公共 API 和内部实现
 *   <li>为用户提供稳定性保证
 *   <li>通过注解进行 API 生命周期管理
 *   <li>支持 API 的渐进式演进
 * </ul>
 *
 * @see Experimental
 * @see VisibleForTesting
 * @since 1.0
 */
@Documented
@Target(ElementType.TYPE)
@Public
public @interface Public {}
