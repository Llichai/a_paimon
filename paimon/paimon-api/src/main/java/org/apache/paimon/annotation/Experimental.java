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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 实验性 API 注解。
 *
 * <p>该注解用于标记实验性的 API 类、方法或字段。被标记为实验性的 API 表示其接口
 * 尚未稳定,可能在未来的版本中发生变更或删除,不建议在生产环境中使用。
 *
 * <h2>主要特点</h2>
 * <ul>
 *   <li><b>不稳定性</b>: API 接口可能在未来版本中发生变更
 *   <li><b>无兼容性保证</b>: 修改时不保证向后兼容
 *   <li><b>可能移除</b>: 实验性功能可能在正式版本中被移除
 *   <li><b>反馈收集</b>: 用于收集用户反馈,完善功能设计
 * </ul>
 *
 * <h2>适用范围</h2>
 * <p>该注解可以应用于:
 * <ul>
 *   <li>类型(TYPE): 实验性的类或接口
 *   <li>字段(FIELD): 实验性的配置选项或字段
 *   <li>方法(METHOD): 实验性的方法
 *   <li>参数(PARAMETER): 实验性的方法参数
 *   <li>构造函数(CONSTRUCTOR): 实验性的构造方法
 *   <li>局部变量(LOCAL_VARIABLE): 实验性的局部变量
 *   <li>包(PACKAGE): 整个包都是实验性的
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 实验性类
 * @Experimental
 * public class NewFeatureTable implements Table {
 *     // ...
 * }
 *
 * // 实验性方法
 * public class TableService {
 *     @Experimental
 *     public void experimentalOperation() {
 *         // 新功能实现
 *     }
 * }
 *
 * // 实验性配置选项
 * @Experimental
 * public static final ConfigOption<Boolean> EXPERIMENTAL_FEATURE =
 *     ConfigOption.key("experimental.feature.enabled")
 *         .booleanType()
 *         .defaultValue(false);
 * }</pre>
 *
 * <h2>使用建议</h2>
 * <ul>
 *   <li><b>文档说明</b>: 在 JavaDoc 中明确说明实验性的原因和可能的变更
 *   <li><b>版本规划</b>: 计划何时将实验性 API 稳定化或移除
 *   <li><b>用户提醒</b>: 在运行时日志中提示用户使用了实验性功能
 *   <li><b>谨慎使用</b>: 避免在关键生产环境中依赖实验性 API
 * </ul>
 *
 * <h2>生命周期</h2>
 * <p>实验性 API 的典型生命周期:
 * <ol>
 *   <li>标记为 @Experimental,开始接收用户反馈
 *   <li>根据反馈进行迭代和改进
 *   <li>API 稳定后,移除 @Experimental 注解,标记为 @Public
 *   <li>或者,如果功能不成熟,可能在后续版本中移除
 * </ol>
 *
 * <h2>与其他注解的关系</h2>
 * <ul>
 *   <li>{@link Public}: 稳定的公共 API,与 @Experimental 互斥
 *   <li>{@link VisibleForTesting}: 仅用于测试,与 @Experimental 语义不同
 * </ul>
 *
 * @see Public
 * @see VisibleForTesting
 * @since 1.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({
    ElementType.TYPE,
    ElementType.FIELD,
    ElementType.METHOD,
    ElementType.PARAMETER,
    ElementType.CONSTRUCTOR,
    ElementType.LOCAL_VARIABLE,
    ElementType.PACKAGE
})
public @interface Experimental {}
