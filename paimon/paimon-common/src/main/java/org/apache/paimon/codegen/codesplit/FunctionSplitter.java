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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * 函数分割器。
 *
 * <p>将长函数拆分为多个较小的函数。这是代码分割流程中的核心步骤之一,用于解决单个方法过长导致的编译问题。
 *
 * <p><b>处理范围和前置条件:</b>
 * <ul>
 *   <li>只处理无返回值的函数(void 类型)</li>
 *   <li>带返回值的函数应该已经由 {@link ReturnValueRewriter} 转换</li>
 *   <li>对于包含 return 语句的函数,会通过 {@link AddBoolBeforeReturnRewriter} 添加提前返回检查</li>
 * </ul>
 *
 * <p><b>拆分策略:</b>
 * <ol>
 *   <li>将方法体按语句块(block statement)分组</li>
 *   <li>每组的总长度不超过 maxMethodLength</li>
 *   <li>为每组生成一个新的辅助方法(方法名 + "_split" + 计数器)</li>
 *   <li>原方法中替换为对这些辅助方法的调用</li>
 *   <li>如果方法包含 return 语句,在调用后添加提前返回检查</li>
 * </ol>
 *
 * <p><b>重写前</b>
 * <pre><code>
 * public class Example {
 *     public void myFun(int a, int b) {
 *         a += b;
 *         b += a;
 *         if (a > 0) {
 *             return;
 *         }
 *         a *= 2;
 *         b *= 2;
 *         System.out.println(a);
 *         System.out.println(b);
 *     }
 * }
 * </code></pre>
 *
 * <p><b>重写后</b>
 * <pre><code>
 * public class Example {
 *     boolean myFunHasReturned$0;
 *
 *     public void myFun(int a, int b) {
 *         myFunHasReturned$0 = false;
 *         myFun_split1(a, b);
 *
 *         myFun_split2(a, b);
 *         if (myFunHasReturned$0) {
 *             return;
 *         }
 *
 *         myFun_split3(a, b);
 *     }
 *
 *     void myFun_split1(int a, int b) {
 *         a += b;
 *         b += a;
 *     }
 *
 *     void myFun_split2(int a, int b) {
 *         if (a > 0) {
 *             {
 *                 myFunHasReturned$0 = true;
 *                 return;
 *             }
 *         }
 *     }
 *
 *     void myFun_split3(int a, int b) {
 *         a *= 2;
 *         b *= 2;
 *         System.out.println(a);
 *         System.out.println(b);
 *     }
 * }
 * </code></pre>
 *
 * <p><b>提前返回处理机制:</b>
 * <p>为了正确处理包含 return 语句的方法,分割器:
 * <ol>
 *   <li>首先通过 AddBoolBeforeReturnRewriter 为方法添加一个布尔标志位</li>
 *   <li>在每个可能提前返回的拆分方法调用后检查该标志位</li>
 *   <li>如果标志位为 true,立即从原方法返回</li>
 * </ol>
 */
public class FunctionSplitter implements CodeRewriter {

    /** 待拆分的代码 */
    private String code;

    /** 单个方法的最大长度限制 */
    private final int maxMethodLength;

    /**
     * 创建函数分割器。
     *
     * @param code 要拆分的代码
     * @param maxMethodLength 最大方法长度
     */
    public FunctionSplitter(String code, int maxMethodLength) {
        this.code = code;
        this.maxMethodLength = maxMethodLength;
    }

    /**
     * 执行函数拆分。
     *
     * <p>拆分流程:
     * <ol>
     *   <li>首先应用 AddBoolBeforeReturnRewriter 处理 return 语句</li>
     *   <li>然后使用 FunctionSplitVisitor 访问并拆分长方法</li>
     * </ol>
     *
     * @return 拆分后的代码
     */
    public String rewrite() {
        AddBoolBeforeReturnRewriter boolRewriter =
                new AddBoolBeforeReturnRewriter(this.code, maxMethodLength);
        code = boolRewriter.rewrite();
        FunctionSplitVisitor visitor = new FunctionSplitVisitor(boolRewriter.getBoolVarNames());
        JavaParser javaParser = new JavaParser(visitor.tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        visitor.visit(javaParser.compilationUnit());
        return visitor.rewriter.getText();
    }

    /**
     * 函数拆分访问器。
     *
     * <p>遍历 AST 并拆分长方法的内部访问器类。
     */
    private class FunctionSplitVisitor extends JavaParserBaseVisitor<Void> {

        /** Token 流 */
        private final CommonTokenStream tokenStream;

        /** Token 流重写器 */
        private final TokenStreamRewriter rewriter;

        /** 布尔变量名映射表(用于提前返回检查) */
        private final List<Map<String, String>> boolVarNames;

        /** 当前处理的类计数 */
        private int classCount;

        /**
         * 创建函数拆分访问器。
         *
         * @param boolVarNames 布尔变量名列表,用于提前返回检查
         */
        private FunctionSplitVisitor(List<Map<String, String>> boolVarNames) {
            this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
            this.rewriter = new TokenStreamRewriter(tokenStream);

            this.boolVarNames = boolVarNames;

            this.classCount = -1;
        }

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            classCount++;
            return visitChildren(ctx);
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {

            // 只处理 void 类型方法
            if (!"void".equals(ctx.typeTypeOrVoid().getText())) {
                return null;
            }

            long methodBodyLength = CodeSplitUtil.getContextTextLength(ctx.methodBody().block());

            // 方法长度未超过限制,不需要拆分
            if (methodBodyLength < maxMethodLength) {
                return null;
            }

            // 方法体为空或只有一个语句,无法拆分
            if (ctx.methodBody().block().blockStatement() == null
                    || ctx.methodBody().block().blockStatement().size() <= 1) {
                return null;
            }

            List<String> splitFuncBodies = new ArrayList<>();
            List<JavaParser.BlockStatementContext> blockStatementContexts = new ArrayList<>();

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

            // 收集所有代码块语句
            for (JavaParser.BlockStatementContext blockStatementContext :
                    ctx.methodBody().block().blockStatement()) {
                blockStatementContexts.add(blockStatementContext);
                splitFuncBodies.add(CodeSplitUtil.getContextString(blockStatementContext));
            }

            // 将代码块合并为不超过长度限制的组
            List<String> mergedCodeBlocks = getMergedCodeBlocks(splitFuncBodies);
            List<String> newSplitMethods = new ArrayList<>();
            List<String> newSplitMethodCalls = new ArrayList<>();

            // 处理异常声明
            String methodQualifier = "";
            if (ctx.THROWS() != null) {
                methodQualifier =
                        " throws " + CodeSplitUtil.getContextString(ctx.qualifiedNameList());
            }

            // 获取提前返回检查的变量名
            String hasReturnedVarName = boolVarNames.get(classCount).get(functionName + parameters);
            if (hasReturnedVarName != null) {
                rewriter.insertAfter(
                        ctx.methodBody().block().start,
                        String.format("\n%s = false;", hasReturnedVarName));
            }

            // 为每个合并后的代码块生成新的拆分方法
            for (String methodBody : mergedCodeBlocks) {
                long counter = CodeSplitUtil.getCounter().getAndIncrement();

                // 生成拆分方法的定义: void f_splitXX(int x, String y)
                String splitMethodDef =
                        type
                                + " "
                                + functionName
                                + "_split"
                                + counter
                                + parameters
                                + methodQualifier;

                String newSplitMethod = splitMethodDef + " {\n" + methodBody + "\n}\n";

                // 生成对拆分方法的调用: f_splitXX(x, y);
                String newSplitMethodCall =
                        functionName
                                + "_split"
                                + counter
                                + "("
                                + String.join(", ", declarations)
                                + ");\n";

                // 如果该拆分方法包含提前返回标志,添加返回检查
                if (hasReturnedVarName != null && newSplitMethod.contains(hasReturnedVarName)) {
                    newSplitMethodCall +=
                            String.format("if (%s) { return; }\n", hasReturnedVarName);
                }

                newSplitMethods.add(newSplitMethod);
                newSplitMethodCalls.add(newSplitMethodCall);
            }

            // 替换原方法体中的语句为拆分方法调用
            for (int i = 0; i < blockStatementContexts.size(); i++) {
                if (i < newSplitMethods.size()) {
                    rewriter.replace(
                            blockStatementContexts.get(i).start,
                            blockStatementContexts.get(i).stop,
                            newSplitMethodCalls.get(i));
                    rewriter.insertAfter(ctx.getParent().stop, "\n" + newSplitMethods.get(i));
                } else {
                    rewriter.delete(
                            blockStatementContexts.get(i).start,
                            blockStatementContexts.get(i).stop);
                }
            }
            return null;
        }

        /**
         * 合并代码块。
         *
         * <p>将多个代码块合并为不超过 maxMethodLength 的组。
         * 这样可以在满足长度限制的前提下,最小化生成的方法数量。
         *
         * @param codeBlock 代码块列表
         * @return 合并后的代码块列表
         */
        private List<String> getMergedCodeBlocks(List<String> codeBlock) {
            List<String> mergedCodeBlocks = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            codeBlock.forEach(
                    code -> {
                        if (sb.length() + code.length() + 1 <= maxMethodLength) {
                            sb.append("\n").append(code);
                        } else {
                            if (sb.length() > 0) {
                                mergedCodeBlocks.add(sb.toString());
                                sb.delete(0, sb.length());
                            }
                            sb.append(code);
                        }
                    });
            if (sb.length() > 0) {
                mergedCodeBlocks.add(sb.toString());
            }
            return mergedCodeBlocks;
        }
    }
}
