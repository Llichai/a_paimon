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

package org.apache.paimon.options;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 结构化选项分割器辅助类,用于根据给定分隔符和引号逻辑分割字符串。
 *
 * <p>该类支持使用单引号(')或双引号(")引用字符串的一部分。引号可以通过加倍来转义。
 *
 * <h2>分割规则</h2>
 * <ul>
 *   <li>支持单引号和双引号
 *   <li>引号可以通过加倍来转义(如 '' 或 "")
 *   <li>引号内的分隔符不会触发分割
 *   <li>空格会被自动去除(除非在引号内)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 示例1: 使用单引号
 * List<String> result = StructuredOptionsSplitter.splitEscaped("'A;B';C", ';');
 * // 结果: ["A;B", "C"]
 *
 * // 示例2: 使用双引号
 * List<String> result = StructuredOptionsSplitter.splitEscaped("\"AB'D\";B;C", ';');
 * // 结果: ["AB'D", "B", "C"]
 *
 * // 示例3: 转义引号
 * List<String> result = StructuredOptionsSplitter.splitEscaped("\"AB'\"\"D;B\";C", ';');
 * // 结果: ["AB'\"D;B", "C"]
 *
 * // 示例4: 转义字符串
 * String escaped = StructuredOptionsSplitter.escapeWithSingleQuote("A;BCD", ";");
 * // 结果: "'A;BCD'"
 * }</pre>
 */
class StructuredOptionsSplitter {

    /**
     * 在给定分隔符上分割给定字符串。
     * 支持使用单引号(')或双引号(")引用字符串的一部分。
     * 引号可以通过加倍来转义。
     *
     * <p>示例:
     *
     * <ul>
     *   <li>'A;B';C => [A;B], [C]
     *   <li>"AB'D";B;C => [AB'D], [B], [C]
     *   <li>"AB'""D;B";C => [AB'"D;B], [C]
     * </ul>
     *
     * <p>更多示例请查看测试。
     *
     * @param string 要分割的字符串
     * @param delimiter 分割所用的分隔符
     * @return 分割结果列表
     */
    static List<String> splitEscaped(String string, char delimiter) {
        List<Token> tokens = tokenize(checkNotNull(string), delimiter);
        return processTokens(tokens);
    }

    /**
     * 使用单引号转义给定字符串,如果输入字符串包含双引号或任何给定的 {@code charsToEscape}。
     * 输入字符串中的任何单引号都将通过加倍来转义。
     *
     * <p>假设 escapeChar 是 (;)
     *
     * <p>示例:
     *
     * <ul>
     *   <li>A,B,C,D => A,B,C,D
     *   <li>A'B'C'D => 'A''B''C''D'
     *   <li>A;BCD => 'A;BCD'
     *   <li>AB"C"D => 'AB"C"D'
     *   <li>AB'"D:B => 'AB''"D:B'
     * </ul>
     *
     * @param string 需要转义的字符串
     * @param charsToEscape 转义条件的转义字符
     * @return 使用单引号转义后的字符串
     */
    static String escapeWithSingleQuote(String string, String... charsToEscape) {
        boolean escape =
                Arrays.stream(charsToEscape).anyMatch(string::contains)
                        || string.contains("\"")
                        || string.contains("'");

        if (escape) {
            return "'" + string.replaceAll("'", "''") + "'";
        }

        return string;
    }

    /**
     * 处理令牌列表,将其转换为字符串列表。
     *
     * @param tokens 令牌列表
     * @return 分割后的字符串列表
     */
    private static List<String> processTokens(List<Token> tokens) {
        final List<String> splits = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            switch (token.getTokenType()) {
                case DOUBLE_QUOTED:
                case SINGLE_QUOTED:
                    if (i + 1 < tokens.size()
                            && tokens.get(i + 1).getTokenType() != TokenType.DELIMITER) {
                        int illegalPosition = tokens.get(i + 1).getPosition() - 1;
                        throw new IllegalArgumentException(
                                "Could not split string. Illegal quoting at position: "
                                        + illegalPosition);
                    }
                    splits.add(token.getString());
                    break;
                case UNQUOTED:
                    splits.add(token.getString());
                    break;
                case DELIMITER:
                    if (i + 1 < tokens.size()
                            && tokens.get(i + 1).getTokenType() == TokenType.DELIMITER) {
                        splits.add("");
                    }
                    break;
            }
        }

        return splits;
    }

    /**
     * 将字符串标记化为令牌列表。
     *
     * @param string 要标记化的字符串
     * @param delimiter 分隔符
     * @return 令牌列表
     */
    private static List<Token> tokenize(String string, char delimiter) {
        final List<Token> tokens = new ArrayList<>();
        final StringBuilder builder = new StringBuilder();
        for (int cursor = 0; cursor < string.length(); ) {
            final char c = string.charAt(cursor);

            int nextChar = cursor + 1;
            if (c == '\'') {
                nextChar = consumeInQuotes(string, '\'', cursor, builder);
                tokens.add(new Token(TokenType.SINGLE_QUOTED, builder.toString(), cursor));
            } else if (c == '"') {
                nextChar = consumeInQuotes(string, '"', cursor, builder);
                tokens.add(new Token(TokenType.DOUBLE_QUOTED, builder.toString(), cursor));
            } else if (c == delimiter) {
                tokens.add(new Token(TokenType.DELIMITER, String.valueOf(c), cursor));
            } else if (!Character.isWhitespace(c)) {
                nextChar = consumeUnquoted(string, delimiter, cursor, builder);
                tokens.add(new Token(TokenType.UNQUOTED, builder.toString().trim(), cursor));
            }
            builder.setLength(0);
            cursor = nextChar;
        }

        return tokens;
    }

    /**
     * 消费引号内的字符。
     *
     * @param string 字符串
     * @param quote 引号字符
     * @param cursor 当前游标位置
     * @param builder 字符串构建器
     * @return 下一个游标位置
     */
    private static int consumeInQuotes(
            String string, char quote, int cursor, StringBuilder builder) {
        for (int i = cursor + 1; i < string.length(); i++) {
            char c = string.charAt(i);
            if (c == quote) {
                if (i + 1 < string.length() && string.charAt(i + 1) == quote) {
                    builder.append(c);
                    i += 1;
                } else {
                    return i + 1;
                }
            } else {
                builder.append(c);
            }
        }

        throw new IllegalArgumentException(
                "Could not split string. Quoting was not closed properly.");
    }

    /**
     * 消费未引用的字符。
     *
     * @param string 字符串
     * @param delimiter 分隔符
     * @param cursor 当前游标位置
     * @param builder 字符串构建器
     * @return 下一个游标位置
     */
    private static int consumeUnquoted(
            String string, char delimiter, int cursor, StringBuilder builder) {
        int i;
        for (i = cursor; i < string.length(); i++) {
            char c = string.charAt(i);
            if (c == delimiter) {
                return i;
            }

            builder.append(c);
        }

        return i;
    }

    /**
     * 令牌类型枚举。
     */
    private enum TokenType {
        /** 双引号引用的令牌 */
        DOUBLE_QUOTED,
        /** 单引号引用的令牌 */
        SINGLE_QUOTED,
        /** 未引用的令牌 */
        UNQUOTED,
        /** 分隔符令牌 */
        DELIMITER
    }

    /**
     * 令牌类,表示分割过程中的一个令牌。
     */
    private static class Token {
        /** 令牌类型 */
        private final TokenType tokenType;
        /** 令牌字符串值 */
        private final String string;
        /** 令牌在原字符串中的位置 */
        private final int position;

        /**
         * 构造令牌。
         *
         * @param tokenType 令牌类型
         * @param string 令牌字符串值
         * @param position 令牌位置
         */
        private Token(TokenType tokenType, String string, int position) {
            this.tokenType = tokenType;
            this.string = string;
            this.position = position;
        }

        /**
         * 获取令牌类型。
         *
         * @return 令牌类型
         */
        public TokenType getTokenType() {
            return tokenType;
        }

        /**
         * 获取令牌字符串值。
         *
         * @return 字符串值
         */
        public String getString() {
            return string;
        }

        /**
         * 获取令牌位置。
         *
         * @return 位置
         */
        public int getPosition() {
            return position;
        }
    }

    /** 私有构造函数,防止实例化。 */
    private StructuredOptionsSplitter() {}
}
