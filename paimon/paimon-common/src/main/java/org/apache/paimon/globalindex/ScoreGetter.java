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

package org.apache.paimon.globalindex;

/**
 * 评分获取器,用于在全局索引 Top-K 场景中为行 ID 生成评分。
 *
 * <p>该接口用于向量搜索等需要相关性评分的索引类型,
 * 为每个匹配的行 ID 提供一个浮点数评分值。
 */
public interface ScoreGetter {

    /**
     * 获取指定行 ID 的评分。
     *
     * @param rowId 行 ID
     * @return 该行的评分值
     */
    float score(long rowId);
}
