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

/**
 * Java 代码重写器接口。
 *
 * <p>用于代码生成过程中对生成的 Java 代码进行重写和优化的统一接口。
 * 代码重写器主要用于解决 Java 编译器对代码的限制,例如:
 * <ul>
 *   <li>方法长度限制 - Java 方法的字节码不能超过 64KB</li>
 *   <li>类成员数量限制 - 类的常量池大小有限制</li>
 *   <li>局部变量数量限制 - 方法的局部变量数量有限制</li>
 * </ul>
 *
 * <p>通过实现此接口,不同的重写器可以处理不同类型的代码优化问题:
 * <ul>
 *   <li>{@link ReturnValueRewriter} - 重写带返回值的函数</li>
 *   <li>{@link DeclarationRewriter} - 重写局部变量声明</li>
 *   <li>{@link BlockStatementRewriter} - 重写代码块语句</li>
 *   <li>{@link FunctionSplitter} - 拆分长函数</li>
 *   <li>{@link MemberFieldRewriter} - 重写成员字段</li>
 * </ul>
 */
public interface CodeRewriter {

    /**
     * 重写代码。
     *
     * <p>实现类应该在此方法中执行具体的代码转换逻辑,返回重写后的代码字符串。
     *
     * @return 重写后的 Java 代码
     */
    String rewrite();
}
