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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.LinkedHashSet;
import java.util.Stack;

/**
 * 返回值重写器。
 *
 * <p>将带返回值的函数重写为两个函数,并将返回值存储到成员变量中。这是代码分割的第一步,
 * 目的是将所有函数统一转换为 void 类型,便于后续的函数拆分操作。
 *
 * <p><b>为什么需要返回值重写?</b>
 * <ul>
 *   <li>带返回值的函数在拆分时需要特殊处理,因为拆分后的每个子函数都可能包含 return 语句</li>
 *   <li>将返回值存储到成员变量中,可以让所有拆分后的子函数都使用 void 返回类型</li>
 *   <li>简化了后续的函数拆分逻辑,统一了函数签名</li>
 * </ul>
 *
 * <p><b>重写前</b>
 * <pre><code>
 * public class Example {
 *     public int myFun(int a) {
 *         a += 1;
 *         return a;
 *     }
 * }
 * </code></pre>
 *
 * <p><b>重写后</b>
 * <pre><code>
 * public class Example {
 *     int myFunReturnValue$0;
 *
 *     public int myFun(int a) {
 *         myFunImpl(a);
 *         return myFunReturnValue$0;
 *     }
 *
 *     void myFunImpl(int a) {
 *         a += 1;
 *         {
 *             myFunReturnValue$0 = a;
 *             return;
 *         }
 *     }
 * }
 * </code></pre>
 *
 * <p><b>实现机制:</b>
 * <ol>
 *   <li>为每个返回值创建一个成员变量(如 myFunReturnValue$0)</li>
 *   <li>创建一个新的 void 实现方法(原方法名 + "Impl")</li>
 *   <li>将原方法体移到实现方法中</li>
 *   <li>将所有 return 语句替换为赋值语句 + return</li>
 *   <li>原方法只调用实现方法并返回成员变量值</li>
 * </ol>
 */
public class ReturnValueRewriter implements CodeRewriter {

    /** 最大方法长度限制 */
    private final int maxMethodLength;

    /** Token 流 */
    private final CommonTokenStream tokenStream;

    /** Token 流重写器,用于修改代码 */
    private final TokenStreamRewriter rewriter;

    /**
     * 创建返回值重写器。
     *
     * @param code 要重写的 Java 代码
     * @param maxMethodLength 最大方法长度
     */
    public ReturnValueRewriter(String code, int maxMethodLength) {
        this.maxMethodLength = maxMethodLength;

        this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
        this.rewriter = new TokenStreamRewriter(tokenStream);
    }

    /**
     * 执行返回值重写。
     *
     * @return 重写后的代码
     */
    public String rewrite() {
        OuterVisitor visitor = new OuterVisitor();
        JavaParser javaParser = new JavaParser(tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        visitor.visit(javaParser.compilationUnit());
        return rewriter.getText();
    }

    /**
     * 外部访问器,用于处理类和方法级别的重写。
     */
    private class OuterVisitor extends JavaParserBaseVisitor<Void> {

        /** 新增字段的栈,用于在嵌套类中正确添加成员变量 */
        private final Stack<StringBuilder> newFields;

        private OuterVisitor() {
            this.newFields = new Stack<>();
        }

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            newFields.push(new StringBuilder());
            Void ret = visitChildren(ctx);
            // 在类体开始位置插入所有新增的成员变量
            rewriter.insertAfter(ctx.start, "\n" + newFields.pop().toString());
            return ret;
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {
            // 只处理非 void 方法
            if ("void".equals(ctx.typeTypeOrVoid().getText())) {
                return null;
            }

            // 只处理长度超过限制的方法
            long methodBodyLength = CodeSplitUtil.getContextTextLength(ctx.methodBody().block());
            if (methodBodyLength < maxMethodLength) {
                return null;
            }

            // 只处理有多个语句的方法
            if (ctx.methodBody().block().blockStatement() == null
                    || ctx.methodBody().block().blockStatement().size() <= 1) {
                return null;
            }

            // 提取函数的真实参数名称
            LinkedHashSet<String> declarations = new LinkedHashSet<>();
            new JavaParserBaseVisitor<Void>() {
                @Override
                public Void visitFormalParameter(JavaParser.FormalParameterContext ctx) {
                    declarations.add(ctx.variableDeclaratorId().getText());
                    return null;
                }
            }.visit(ctx);

            // 提取函数定义的各个部分
            String type = CodeSplitUtil.getContextString(ctx.typeTypeOrVoid());
            String functionName = ctx.IDENTIFIER().getText();
            String parameters = CodeSplitUtil.getContextString(ctx.formalParameters());
            String methodQualifier = "";
            if (ctx.THROWS() != null) {
                methodQualifier =
                        " throws " + CodeSplitUtil.getContextString(ctx.qualifiedNameList());
            }

            // 创建新的返回值变量
            String returnVarName = CodeSplitUtil.newName(functionName + "ReturnValue");
            newFields.peek().append(String.format("%s %s;\n", type, returnVarName));

            // 替换所有 return 语句
            InnerVisitor visitor = new InnerVisitor(returnVarName);
            visitor.visitMethodDeclaration(ctx);

            // 创建新的方法调用
            // 原方法: 调用实现方法并返回成员变量
            String newMethodBody =
                    String.format(
                            "{ %sImpl(%s); return %s; }",
                            functionName, String.join(", ", declarations), returnVarName);
            // 实现方法: void 返回类型 + Impl 后缀
            String implMethodDeclaration =
                    String.format("void %sImpl%s%s ", functionName, parameters, methodQualifier);
            rewriter.insertBefore(
                    ctx.methodBody().start, newMethodBody + "\n\n" + implMethodDeclaration);

            return null;
        }
    }

    /**
     * 内部访问器,用于替换方法体内的 return 语句。
     */
    private class InnerVisitor extends JavaParserBaseVisitor<Void> {

        /** 返回值变量名 */
        private final String returnVarName;

        private InnerVisitor(String returnVarName) {
            this.returnVarName = returnVarName;
        }

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            // 跳过匿名内部类,避免错误地重写内部类的 return 语句
            return null;
        }

        @Override
        public Void visitLambdaBody(JavaParser.LambdaBodyContext ctx) {
            // 跳过 lambda 表达式,避免错误地重写 lambda 的 return 语句
            return null;
        }

        @Override
        public Void visitStatement(JavaParser.StatementContext ctx) {
            if (ctx.RETURN() != null) {
                // 将 "return value;" 替换为 "{ returnVar = value; return; }"
                String newReturnStatement =
                        String.format(
                                // 添加一对 {} 包围这些语句,避免原始 return 语句没有 {} 的情况
                                // 例如: if (...) return;
                                "{ %s = %s; return; }",
                                returnVarName,
                                CodeSplitUtil.getContextString(ctx.expression().get(0)));
                rewriter.replace(ctx.start, ctx.stop, newReturnStatement);
            }
            return visitChildren(ctx);
        }
    }
}
