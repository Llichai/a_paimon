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

package org.apache.paimon.rest;

import java.util.regex.Pattern;

/**
 * 函数名称验证器。
 *
 * <p>用于验证 REST Catalog 中函数名称的合法性,确保函数名符合命名规范。
 *
 * <h2>命名规则</h2>
 * <ul>
 *   <li><b>必须包含至少一个字母</b>: 不能是纯数字、纯符号
 *   <li><b>允许的字符</b>: 字母(A-Z, a-z)、数字(0-9)、点(.)、下划线(_)、连字符(-)
 *   <li><b>不能为空</b>: null 或空字符串都不合法
 * </ul>
 *
 * <h2>有效的函数名示例</h2>
 * <pre>
 * my_function          ✓ 包含字母和下划线
 * MyFunction123        ✓ 包含字母和数字
 * my.function          ✓ 包含字母和点
 * my-function          ✓ 包含字母和连字符
 * func1                ✓ 包含字母和数字
 * a                    ✓ 单个字母
 * _func_               ✓ 字母加下划线
 * my.db.func           ✓ 分层命名
 * </pre>
 *
 * <h2>无效的函数名示例</h2>
 * <pre>
 * ""                   ✗ 空字符串
 * null                 ✗ null 值
 * 123                  ✗ 纯数字,没有字母
 * ___                  ✗ 纯下划线,没有字母
 * my function          ✗ 包含空格
 * my@function          ✗ 包含非法字符 @
 * my/function          ✗ 包含非法字符 /
 * function()           ✗ 包含括号
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 验证函数名是否合法
 * String funcName = "my_custom_function";
 * if (RESTFunctionValidator.isValidFunctionName(funcName)) {
 *     System.out.println("函数名合法");
 * } else {
 *     System.out.println("函数名不合法");
 * }
 *
 * // 检查并抛出异常
 * try {
 *     RESTFunctionValidator.checkFunctionName("123");
 * } catch (IllegalArgumentException e) {
 *     // 处理无效函数名
 *     System.err.println("Invalid function name: " + e.getMessage());
 * }
 * }</pre>
 *
 * <h2>设计理由</h2>
 * <ul>
 *   <li><b>至少一个字母</b>: 避免与数字常量混淆
 *   <li><b>限制字符集</b>: 确保跨平台兼容性
 *   <li><b>允许点号</b>: 支持命名空间(如 my.namespace.func)
 *   <li><b>允许下划线和连字符</b>: 提供灵活的命名风格
 * </ul>
 *
 * @see org.apache.paimon.rest.requests.CreateFunctionRequest
 * @see org.apache.paimon.rest.requests.AlterFunctionRequest
 */
public class RESTFunctionValidator {

    /**
     * 函数名称匹配模式。
     *
     * <p>正则表达式: {@code ^(?=.*[A-Za-z])[A-Za-z0-9._-]+$}
     *
     * <p>解释:
     * <ul>
     *   <li>{@code ^} - 字符串开始
     *   <li>{@code (?=.*[A-Za-z])} - 正向预查,确保至少包含一个字母
     *   <li>{@code [A-Za-z0-9._-]+} - 一个或多个字母、数字、点、下划线或连字符
     *   <li>{@code $} - 字符串结束
     * </ul>
     */
    private static final Pattern FUNCTION_NAME_PATTERN =
            Pattern.compile("^(?=.*[A-Za-z])[A-Za-z0-9._-]+$");

    /**
     * 检查函数名称是否合法。
     *
     * @param name 函数名称
     * @return 如果合法返回 true,否则返回 false
     */
    public static boolean isValidFunctionName(String name) {
        return org.apache.paimon.utils.StringUtils.isNotEmpty(name)
                && FUNCTION_NAME_PATTERN.matcher(name).matches();
    }

    /**
     * 检查函数名称是否合法,如果不合法则抛出异常。
     *
     * @param name 函数名称
     * @throws IllegalArgumentException 如果函数名称不合法
     */
    public static void checkFunctionName(String name) {
        boolean isValid = isValidFunctionName(name);
        if (!isValid) {
            throw new IllegalArgumentException("Invalid function name: " + name);
        }
    }
}
