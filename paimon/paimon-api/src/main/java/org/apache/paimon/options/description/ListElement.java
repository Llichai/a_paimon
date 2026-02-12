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

import java.util.Arrays;
import java.util.List;

/** 表示 {@link Description} 中的列表。 */
public class ListElement implements BlockElement {

    private final List<InlineElement> entries;

    /**
     * 创建包含文本块的列表。例如:
     *
     * <pre>{@code
     * .list(
     * 	text("这是列表的第一个元素"),
     * 	text("这是第二个元素,包含一个链接 %s", link("https://link"))
     * )
     * }</pre>
     *
     * @param elements 此列表的条目列表
     * @return 列表表示
     */
    public static ListElement list(InlineElement... elements) {
        return new ListElement(Arrays.asList(elements));
    }

    public List<InlineElement> getEntries() {
        return entries;
    }

    private ListElement(List<InlineElement> entries) {
        this.entries = entries;
    }

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }
}
