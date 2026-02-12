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

package org.apache.paimon.function;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataField;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 函数接口,定义 Paimon 中用户定义函数(UDF)的基本契约。
 *
 * <p>该接口表示一个可以在 Paimon 系统中注册和使用的函数。
 * 函数可以有多个定义(definitions),每个定义对应不同的实现方式。
 *
 * <h2>函数属性</h2>
 * <ul>
 *   <li>名称和标识: 函数的简称、全名和标识符
 *   <li>参数信息: 输入参数和返回参数的类型定义
 *   <li>确定性: 函数是否为确定性函数
 *   <li>定义集合: 函数的具体实现定义
 *   <li>元数据: 注释和配置选项
 * </ul>
 *
 * <h2>函数类型</h2>
 * 函数可以有三种定义类型:
 * <ul>
 *   <li>File: 基于文件的函数(JAR、Python 脚本等)
 *   <li>SQL: SQL 表达式函数
 *   <li>Lambda: Lambda 表达式函数
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 获取函数信息
 * Function func = catalog.getFunction(identifier);
 *
 * // 访问函数属性
 * String name = func.name();  // 简称
 * String fullName = func.fullName();  // 全限定名
 * boolean deterministic = func.isDeterministic();  // 是否确定性
 *
 * // 获取函数定义
 * Map<String, FunctionDefinition> defs = func.definitions();
 * FunctionDefinition sqlDef = func.definition("sql");
 *
 * // 获取参数信息
 * Optional<List<DataField>> inputs = func.inputParams();
 * Optional<List<DataField>> outputs = func.returnParams();
 * }</pre>
 *
 * @see FunctionDefinition
 * @see FunctionImpl
 */
public interface Function {

    /**
     * 获取函数的简称(不包含数据库名)。
     *
     * @return 函数名称
     */
    String name();

    /**
     * 获取函数的全限定名(包含数据库名)。
     *
     * @return 完整的函数名称,格式为 "database.function"
     */
    String fullName();

    /**
     * 获取函数的标识符。
     *
     * @return 函数标识符,包含数据库和函数名
     */
    Identifier identifier();

    /**
     * 获取函数的输入参数定义。
     *
     * @return 输入参数列表的 Optional,如果未定义则为空
     */
    Optional<List<DataField>> inputParams();

    /**
     * 获取函数的返回参数定义。
     *
     * @return 返回参数列表的 Optional,如果未定义则为空
     */
    Optional<List<DataField>> returnParams();

    /**
     * 判断函数是否为确定性函数。
     *
     * <p>确定性函数对相同的输入总是返回相同的输出。
     *
     * @return 如果是确定性函数返回 true,否则返回 false
     */
    boolean isDeterministic();

    /**
     * 获取函数的所有定义。
     *
     * <p>一个函数可以有多个定义,每个定义对应不同的实现方式(如 SQL、Lambda、文件等)。
     *
     * @return 函数定义的 Map,键为定义名称,值为定义对象
     */
    Map<String, FunctionDefinition> definitions();

    /**
     * 根据名称获取指定的函数定义。
     *
     * @param name 定义名称
     * @return 对应的函数定义,如果不存在返回 null
     */
    FunctionDefinition definition(String name);

    /**
     * 获取函数的注释说明。
     *
     * @return 函数注释
     */
    String comment();

    /**
     * 获取函数的配置选项。
     *
     * @return 配置选项 Map
     */
    Map<String, String> options();
}
