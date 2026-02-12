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
 * 仅供测试可见注解。
 *
 * <p>该注解声明一个函数、字段、构造函数或整个类型仅为了测试目的而可见。
 * 这些成员本应该是 {@code private} 的(因为不打算被外部调用),但由于某些测试
 * 需要访问它们,所以无法声明为私有的。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>测试访问</b>: 允许测试代码访问本应为私有的成员
 *   <li><b>内部实现</b>: 标记不应在生产代码中使用的内部实现
 *   <li><b>封装保护</b>: 在放宽访问控制的同时保持封装语义
 *   <li><b>文档说明</b>: 明确说明访问控制放宽的原因
 * </ul>
 *
 * <h2>典型示例</h2>
 * <pre>{@code
 * public class FileStoreImpl implements FileStore {
 *     // 本应该是 private,但测试需要访问
 *     @VisibleForTesting
 *     protected Map<String, FileEntry> fileCache;
 *
 *     // 本应该是 private,但测试需要验证内部状态
 *     @VisibleForTesting
 *     protected void clearInternalCache() {
 *         fileCache.clear();
 *     }
 *
 *     // 本应该是 private,但测试需要创建特定实例
 *     @VisibleForTesting
 *     protected FileStoreImpl(Map<String, FileEntry> initialCache) {
 *         this.fileCache = initialCache;
 *     }
 * }
 *
 * // 对应的测试代码
 * public class FileStoreImplTest {
 *     @Test
 *     public void testCacheBehavior() {
 *         FileStoreImpl store = new FileStoreImpl(createTestCache());
 *         // 访问被标记为 @VisibleForTesting 的字段
 *         assertEquals(10, store.fileCache.size());
 *
 *         // 调用被标记为 @VisibleForTesting 的方法
 *         store.clearInternalCache();
 *         assertTrue(store.fileCache.isEmpty());
 *     }
 * }
 * }</pre>
 *
 * <h2>最佳实践</h2>
 * <ul>
 *   <li><b>最小权限</b>: 使用最小的访问权限(protected 优于 public)
 *   <li><b>明确文档</b>: 在 JavaDoc 中说明为何需要放宽访问控制
 *   <li><b>避免滥用</b>: 只在确实需要测试访问时使用
 *   <li><b>替代方案</b>: 考虑使用包私有或重构设计
 * </ul>
 *
 * <h2>常见使用模式</h2>
 * <ol>
 *   <li><b>内部状态验证</b>: 测试需要验证对象的内部状态
 *   <pre>{@code
 *   @VisibleForTesting
 *   protected int getInternalCounter() {
 *       return counter;
 *   }
 *   }</pre>
 *
 *   <li><b>测试构造器</b>: 提供用于测试的特殊构造器
 *   <pre>{@code
 *   @VisibleForTesting
 *   protected Service(MockDependency mockDep) {
 *       this.dependency = mockDep;
 *   }
 *   }</pre>
 *
 *   <li><b>重置方法</b>: 提供用于测试的状态重置方法
 *   <pre>{@code
 *   @VisibleForTesting
 *   protected void resetState() {
 *       this.initialized = false;
 *       this.cache.clear();
 *   }
 *   }</pre>
 * </ol>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>不是公共 API</b>: 标记的成员不属于公共 API,可能随时变更
 *   <li><b>仅用于测试</b>: 生产代码不应调用这些成员
 *   <li><b>可能重构</b>: 随着设计改进,这些成员可能被移除或修改
 *   <li><b>代码审查</b>: 使用该注解时应在代码审查中说明原因
 * </ul>
 *
 * <h2>替代方案</h2>
 * <p>在某些情况下,可以考虑以下替代方案:
 * <ul>
 *   <li><b>包私有</b>: 将测试放在同一个包下,使用包私有访问
 *   <li><b>测试友好的设计</b>: 重构代码使其更容易测试
 *   <li><b>依赖注入</b>: 通过构造器或方法注入依赖,便于测试
 *   <li><b>公共 API</b>: 如果真的需要外部访问,考虑设计为公共 API
 * </ul>
 *
 * <h2>适用范围</h2>
 * <p>该注解可以应用于:
 * <ul>
 *   <li>类型(TYPE): 整个类仅用于测试
 *   <li>方法(METHOD): 方法仅用于测试访问
 *   <li>字段(FIELD): 字段仅用于测试访问
 *   <li>构造函数(CONSTRUCTOR): 构造器仅用于测试
 * </ul>
 *
 * @see Public
 * @see Experimental
 * @since 1.0
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
public @interface VisibleForTesting {}
