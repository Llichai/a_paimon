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

package org.apache.paimon.fileindex;

/**
 * 文件索引过滤结果接口。
 *
 * <p>用于决定是否需要扫描某个文件。索引过滤的结果有两种:
 * <ul>
 *   <li>{@link #REMAIN}: 需要保留该文件,继续扫描</li>
 *   <li>{@link #SKIP}: 可以跳过该文件,不需要扫描</li>
 * </ul>
 *
 * <p>该接口支持逻辑运算:
 * <ul>
 *   <li>{@link #and(FileIndexResult)}: AND 运算,两个条件都满足才保留</li>
 *   <li>{@link #or(FileIndexResult)}: OR 运算,任一条件满足就保留</li>
 * </ul>
 *
 * <p>逻辑运算规则:
 * <pre>
 * AND:
 *   REMAIN AND REMAIN = REMAIN
 *   REMAIN AND SKIP   = SKIP
 *   SKIP   AND REMAIN = SKIP
 *   SKIP   AND SKIP   = SKIP
 *
 * OR:
 *   REMAIN OR REMAIN = REMAIN
 *   REMAIN OR SKIP   = REMAIN
 *   SKIP   OR REMAIN = REMAIN
 *   SKIP   OR SKIP   = SKIP
 * </pre>
 */
public interface FileIndexResult {

    /**
     * 保留结果常量。
     *
     * <p>表示文件可能包含满足条件的数据,需要继续扫描。
     */
    FileIndexResult REMAIN =
            new FileIndexResult() {
                @Override
                public boolean remain() {
                    return true;
                }

                @Override
                public FileIndexResult and(FileIndexResult fileIndexResult) {
                    // REMAIN AND x = x
                    return fileIndexResult;
                }

                @Override
                public FileIndexResult or(FileIndexResult fileIndexResult) {
                    // REMAIN OR x = REMAIN
                    return this;
                }
            };

    /**
     * 跳过结果常量。
     *
     * <p>表示文件确定不包含满足条件的数据,可以跳过扫描。
     */
    FileIndexResult SKIP =
            new FileIndexResult() {
                @Override
                public boolean remain() {
                    return false;
                }

                @Override
                public FileIndexResult and(FileIndexResult fileIndexResult) {
                    // SKIP AND x = SKIP
                    return this;
                }

                @Override
                public FileIndexResult or(FileIndexResult fileIndexResult) {
                    // SKIP OR x = x
                    return fileIndexResult;
                }
            };

    /**
     * 是否需要保留该文件。
     *
     * @return true 表示需要扫描,false 表示可以跳过
     */
    boolean remain();

    /**
     * AND 逻辑运算。
     *
     * <p>默认实现:
     * <ul>
     *   <li>如果另一个结果是 REMAIN,返回当前结果</li>
     *   <li>如果另一个结果是 SKIP,返回 SKIP</li>
     * </ul>
     *
     * @param fileIndexResult 另一个过滤结果
     * @return AND 运算后的结果
     */
    default FileIndexResult and(FileIndexResult fileIndexResult) {
        if (fileIndexResult.remain()) {
            return this;
        } else {
            return SKIP;
        }
    }

    /**
     * OR 逻辑运算。
     *
     * <p>默认实现:
     * <ul>
     *   <li>如果另一个结果是 REMAIN,返回 REMAIN</li>
     *   <li>如果另一个结果是 SKIP,返回当前结果</li>
     * </ul>
     *
     * @param fileIndexResult 另一个过滤结果
     * @return OR 运算后的结果
     */
    default FileIndexResult or(FileIndexResult fileIndexResult) {
        if (fileIndexResult.remain()) {
            return REMAIN;
        } else {
            return this;
        }
    }
}
