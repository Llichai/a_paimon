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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.KeyValue;

/**
 * 带位置的键值对
 *
 * <p>用于 Deletion Vector 的 {@link KeyValue} 及其文件位置。
 *
 * <p>包含信息：
 * <ul>
 *   <li>keyValue：键值对
 *   <li>fileName：文件名
 *   <li>rowPosition：行在文件中的位置
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：跟踪每个键值对的位置
 *   <li>Deletion Vector：标记删除的行位置
 *   <li>行级别更新：定位并更新特定行
 * </ul>
 */
public class PositionedKeyValue {

    /** 键值对 */
    private final KeyValue keyValue;
    /** 文件名 */
    private final String fileName;
    /** 行位置 */
    private final long rowPosition;

    /**
     * 构造带位置的键值对
     *
     * @param keyValue 键值对
     * @param fileName 文件名
     * @param rowPosition 行位置
     */
    public PositionedKeyValue(KeyValue keyValue, String fileName, long rowPosition) {
        this.keyValue = keyValue;
        this.fileName = fileName;
        this.rowPosition = rowPosition;
    }

    /**
     * 获取文件名
     *
     * @return 文件名
     */
    public String fileName() {
        return fileName;
    }

    /**
     * 获取行位置
     *
     * @return 行位置
     */
    public long rowPosition() {
        return rowPosition;
    }

    /**
     * 获取键值对
     *
     * @return 键值对
     */
    public KeyValue keyValue() {
        return keyValue;
    }
}
