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

import org.apache.paimon.codegen.codesplit.JavaParser.StatementContext;

/**
 * return、continue 和 break 语句计数器。
 *
 * <p>简单的解析器访问者,用于统计代码块中 'return'、'continue' 和 'break' 关键字的总数。
 * 这个计数器主要用于代码分割过程中判断代码块是否包含控制流跳转语句。
 *
 * <p>用途:
 * <ul>
 *   <li>检测代码块是否可以安全提取 - 包含跳转语句的代码块需要特殊处理</li>
 *   <li>决定是否需要添加额外的控制流标志</li>
 *   <li>辅助其他代码重写器做出正确的分割决策</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * ReturnAndJumpCounter counter = new ReturnAndJumpCounter();
 * counter.visit(parserRuleContext);
 * int count = counter.getCounter();  // 获取跳转语句总数
 * }</pre>
 */
public class ReturnAndJumpCounter extends JavaParserBaseVisitor<Void> {

    /** 跳转语句计数器 */
    private int counter = 0;

    /**
     * 访问语句节点。
     *
     * <p>检查语句是否包含 return、break 或 continue 关键字,如果包含则增加计数。
     *
     * @param ctx 语句上下文
     * @return null(访问者模式标准返回)
     */
    @Override
    public Void visitStatement(StatementContext ctx) {
        if (ctx.RETURN() != null || ctx.BREAK() != null || ctx.CONTINUE() != null) {
            counter++;
        }
        return visitChildren(ctx);
    }

    /**
     * 获取跳转语句的总数。
     *
     * @return return、break 和 continue 语句的总数
     */
    public int getCounter() {
        return counter;
    }
}
