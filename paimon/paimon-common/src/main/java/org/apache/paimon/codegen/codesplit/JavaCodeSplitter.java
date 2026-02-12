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

package org.apache.paimon.codegen.codesplit;

import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Java 代码分割器。
 *
 * <p>重写生成的 Java 代码,使每个方法的长度变小并可以被编译。该类是代码生成框架中的核心组件,
 * 用于解决 Java 编译器的方法长度限制问题(Java 方法的字节码不能超过 64KB)。
 *
 * <p>代码分割的主要原因:
 * <ul>
 *   <li><b>方法大小限制</b> - JVM 规范限制单个方法的字节码不能超过 64KB</li>
 *   <li><b>编译性能</b> - 过大的方法会导致编译时间显著增加</li>
 *   <li><b>JIT 优化</b> - 过大的方法可能无法被 JIT 编译器优化</li>
 *   <li><b>类常量池限制</b> - 类的常量池条目数量有上限</li>
 * </ul>
 *
 * <p>分割策略采用多阶段重写器链:
 * <ol>
 *   <li>{@link ReturnValueRewriter} - 将带返回值的函数转换为 void 函数,返回值通过成员变量传递</li>
 *   <li>{@link DeclarationRewriter} - 将局部变量声明提升为成员字段</li>
 *   <li>{@link BlockStatementRewriter} - 提取代码块语句到单独的方法</li>
 *   <li>{@link FunctionSplitter} - 将长函数拆分为多个较小的函数</li>
 *   <li>{@link MemberFieldRewriter} - 重组成员字段以避免类成员数量限制</li>
 * </ol>
 *
 * <p>使用示例:
 * <pre>{@code
 * String splitCode = JavaCodeSplitter.split(
 *     generatedCode,
 *     4000,  // 最大方法长度(字符数)
 *     10000  // 最大类成员数量
 * );
 * }</pre>
 */
public class JavaCodeSplitter {

    /**
     * 分割 Java 代码。
     *
     * <p>该方法是代码分割的入口点,内部捕获所有异常并重新包装为 RuntimeException。
     *
     * @param code 要分割的 Java 源代码
     * @param maxMethodLength 单个方法的最大字符长度
     * @param maxClassMemberCount 类的最大成员数量
     * @return 分割后的 Java 代码
     * @throws RuntimeException 如果代码分割失败
     */
    public static String split(String code, int maxMethodLength, int maxClassMemberCount) {
        try {
            return splitImpl(code, maxMethodLength, maxClassMemberCount);
        } catch (Throwable t) {
            throw new RuntimeException(
                    "JavaCodeSplitter failed. This is a bug. Please file an issue.", t);
        }
    }

    /**
     * 代码分割的具体实现。
     *
     * <p>执行多阶段的代码重写流程:
     * <ol>
     *   <li>检查代码长度,如果小于限制则直接返回</li>
     *   <li>应用返回值重写器,处理带返回值的函数</li>
     *   <li>应用声明重写器,将局部变量提升为成员字段</li>
     *   <li>应用代码块重写器,提取代码块到独立方法</li>
     *   <li>应用函数分割器,拆分长函数</li>
     *   <li>应用成员字段重写器,重组类成员</li>
     * </ol>
     *
     * <p>如果任何重写步骤失败,则回退到原始代码。
     *
     * @param code 要分割的代码
     * @param maxMethodLength 最大方法长度
     * @param maxClassMemberCount 最大类成员数量
     * @return 分割后的代码
     */
    private static String splitImpl(String code, int maxMethodLength, int maxClassMemberCount) {
        checkArgument(code != null && !code.isEmpty(), "code cannot be empty");
        checkArgument(maxMethodLength > 0, "maxMethodLength must be greater than 0");
        checkArgument(maxClassMemberCount > 0, "maxClassMemberCount must be greater than 0");

        if (code.length() <= maxMethodLength) {
            return code;
        }

        String returnValueRewrittenCode = new ReturnValueRewriter(code, maxMethodLength).rewrite();
        return Optional.ofNullable(
                        new DeclarationRewriter(returnValueRewrittenCode, maxMethodLength)
                                .rewrite())
                .map(text -> new BlockStatementRewriter(text, maxMethodLength).rewrite())
                .map(text -> new FunctionSplitter(text, maxMethodLength).rewrite())
                .map(text -> new MemberFieldRewriter(text, maxClassMemberCount).rewrite())
                .orElse(code);
    }
}
