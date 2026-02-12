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

import org.apache.paimon.annotation.Public;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link org.apache.paimon.options.ConfigOption} 的描述类。允许使用多种丰富的格式。
 *
 * <p>该类用于为配置选项提供详细的描述信息,包括纯文本、列表、链接、换行符等格式。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个包含列表和链接的描述
 * Description description = Description.builder()
 *     .text("这是一个配置选项的描述: ")
 *     .list(
 *         text("这是列表的第一个元素"),
 *         text("这是第二个元素,包含一个链接 %s", link("https://example.com")))
 *     .build();
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public class Description {

    private final List<BlockElement> blocks;

    public static DescriptionBuilder builder() {
        return new DescriptionBuilder();
    }

    public List<BlockElement> getBlocks() {
        return blocks;
    }

    /**
     * Builder for {@link Description}. Allows adding a rich formatting like lists, links,
     * linebreaks etc. For example:
     *
     * <pre>{@code
     * Description description = Description.builder()
     * 	.text("This is some list: ")
     * 	.list(
     * 		text("this is first element of list"),
     * 		text("this is second element of list with a %s", link("https://link")))
     * 	.build();
     * }</pre>
     */
    public static class DescriptionBuilder {

        private final List<BlockElement> blocks = new ArrayList<>();

        /**
         * Adds a block of text with placeholders ("%s") that will be replaced with proper string
         * representation of given {@link InlineElement}. For example:
         *
         * <p>{@code text("This is a text with a link %s", link("https://somepage", "to here"))}
         *
         * @param format text with placeholders for elements
         * @param elements elements to be put in the text
         * @return description with added block of text
         */
        public DescriptionBuilder text(String format, InlineElement... elements) {
            blocks.add(TextElement.text(format, elements));
            return this;
        }

        /**
         * Creates a simple block of text.
         *
         * @param text a simple block of text
         * @return block of text
         */
        public DescriptionBuilder text(String text) {
            blocks.add(TextElement.text(text));
            return this;
        }

        /**
         * Block of description add.
         *
         * @param block block of description to add
         * @return block of description
         */
        public DescriptionBuilder add(BlockElement block) {
            blocks.add(block);
            return this;
        }

        /** Creates a line break in the description. */
        public DescriptionBuilder linebreak() {
            blocks.add(LineBreakElement.linebreak());
            return this;
        }

        /** Adds a bulleted list to the description. */
        public DescriptionBuilder list(InlineElement... elements) {
            blocks.add(ListElement.list(elements));
            return this;
        }

        /** Creates description representation. */
        public Description build() {
            return new Description(blocks);
        }
    }

    private Description(List<BlockElement> blocks) {
        this.blocks = blocks;
    }
}
