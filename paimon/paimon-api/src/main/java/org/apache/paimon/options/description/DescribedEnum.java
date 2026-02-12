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

import org.apache.paimon.options.ConfigOption;

/**
 * 用于描述 {@link ConfigOption} 中使用的枚举常量。
 *
 * <p>对于用作配置选项的枚举,可以实现此接口为每个枚举常量提供 {@link Description}。
 * 这将在生成配置选项的文档时使用,以在文档中包含可用值的列表及其各自的描述。
 *
 * <p>更准确地说,只有 {@link InlineElement} 可以作为块元素不能嵌套到列表中返回。
 */
public interface DescribedEnum {

    /**
     * 返回枚举常量的描述。
     *
     * <p>如果要包含链接或代码块,请使用 {@link TextElement#wrap(InlineElement...)}
     * 将多个内联元素包装为单个元素。
     */
    InlineElement getDescription();
}
