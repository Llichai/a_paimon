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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 代码重写器工具类。
 *
 * <p>为代码重写器提供通用的工具方法,主要用于:
 * <ul>
 *   <li>生成唯一的名称 - 为拆分后的方法和变量生成唯一标识符</li>
 *   <li>提取上下文文本 - 从 ANTLR 解析树节点中提取源代码文本</li>
 *   <li>计算文本长度 - 用于判断是否需要进行代码分割</li>
 * </ul>
 *
 * <p>该类使用全局计数器确保生成的名称在整个代码生成过程中是唯一的。
 */
public class CodeSplitUtil {

    /** 全局计数器,用于生成唯一的名称后缀 */
    private static final AtomicLong COUNTER = new AtomicLong(0L);

    /**
     * 获取全局计数器。
     *
     * @return 全局计数器实例
     */
    public static AtomicLong getCounter() {
        return COUNTER;
    }

    /**
     * 生成新的唯一名称。
     *
     * <p>通过在原始名称后添加 "$" 和递增的计数器值来生成唯一名称。
     * 例如: "myMethod" 可能变成 "myMethod$0", "myMethod$1" 等。
     *
     * @param name 原始名称
     * @return 带有唯一后缀的新名称
     */
    public static String newName(String name) {
        return name + "$" + COUNTER.getAndIncrement();
    }

    /**
     * 从解析器上下文中提取源代码文本。
     *
     * <p>该方法从 ANTLR 解析树节点中提取对应的原始源代码文本。
     *
     * @param ctx ANTLR 解析器规则上下文
     * @return 对应的源代码文本,如果上下文为 null 则返回空字符串
     */
    public static String getContextString(ParserRuleContext ctx) {
        if (ctx == null) {
            return "";
        }

        CharStream cs = ctx.start.getInputStream();
        return cs.getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    /**
     * 获取上下文文本的长度。
     *
     * <p>用于判断代码块是否超过最大长度限制,从而决定是否需要进行代码分割。
     *
     * @param ctx ANTLR 解析器规则上下文
     * @return 对应源代码文本的字符长度
     */
    public static int getContextTextLength(ParserRuleContext ctx) {
        return getContextString(ctx).length();
    }
}
