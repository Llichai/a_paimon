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

package org.apache.paimon.options.description;

import org.apache.paimon.utils.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/** 表示 {@link Description} 中的文本块。 */
public class TextElement implements BlockElement, InlineElement {
    private final String format;
    private final List<InlineElement> elements;
    private final EnumSet<TextStyle> textStyles = EnumSet.noneOf(TextStyle.class);

    /**
     * 创建一个文本块,其中包含将被替换为给定 {@link InlineElement} 的正确字符串表示的占位符("%s")。
     * 例如:
     *
     * <p>{@code text("这是包含链接的文本 %s", link("https://somepage", "到这里"))}
     *
     * @param format 包含元素占位符的文本
     * @param elements 要放在文本中的元素
     * @return 文本块
     */
    public static TextElement text(String format, InlineElement... elements) {
        return new TextElement(format, Arrays.asList(elements));
    }

    /**
     * 创建简单的文本块。
     *
     * @param text 简单的文本块
     * @return 文本块
     */
    public static TextElement text(String text) {
        return new TextElement(text, Collections.emptyList());
    }

    /** 将 {@link InlineElement} 列表包装成单个 {@link TextElement}。 */
    public static InlineElement wrap(InlineElement... elements) {
        return text(StringUtils.repeat("%s", elements.length), elements);
    }

    /**
     * 创建格式化为代码的文本块。
     *
     * @param text 将格式化为代码的文本块
     * @return 格式化为代码的文本块
     */
    public static TextElement code(String text) {
        TextElement element = text(text);
        element.textStyles.add(TextStyle.CODE);
        return element;
    }

    public String getFormat() {
        return format;
    }

    public List<InlineElement> getElements() {
        return elements;
    }

    public EnumSet<TextStyle> getStyles() {
        return textStyles;
    }

    private TextElement(String format, List<InlineElement> elements) {
        this.format = format;
        this.elements = elements;
    }

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }

    /** Styles that can be applied to {@link TextElement} e.g. code, bold etc. */
    public enum TextStyle {
        CODE
    }
}
