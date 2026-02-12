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

/** 表示 {@link Description} 中的链接的元素。 */
public class LinkElement implements InlineElement {
    private final String link;
    private final String text;

    /**
     * 创建具有给定 URL 和描述的链接。
     *
     * @param link 此链接应指向的地址
     * @param text 应在文本中使用的链接描述
     * @return 链接表示
     */
    public static LinkElement link(String link, String text) {
        return new LinkElement(link, text);
    }

    /**
     * 创建具有给定 URL 的链接。此 URL 将用作该链接的描述。
     *
     * @param link 此链接应指向的地址
     * @return 链接表示
     */
    public static LinkElement link(String link) {
        return new LinkElement(link, link);
    }

    public String getLink() {
        return link;
    }

    public String getText() {
        return text;
    }

    private LinkElement(String link, String text) {
        this.link = link;
        this.text = text;
    }

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }
}
