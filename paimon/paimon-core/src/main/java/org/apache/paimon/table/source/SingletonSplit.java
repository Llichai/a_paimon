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

package org.apache.paimon.table.source;

/**
 * 单例分片，用于系统表，扫描总是只产生一个分片。
 *
 * <p>SingletonSplit 是一个抽象基类，用于那些不需要分片的表（如系统表、元数据表）。
 * 这些表的扫描总是返回单个 Split。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>系统表</b>: 如快照表、文件表等，数据量很小，不需要分片</li>
 *   <li><b>元数据表</b>: 如分区表、标签表等</li>
 *   <li><b>聚合结果表</b>: 全局聚合的结果，只有一行数据</li>
 * </ul>
 *
 * <h3>特点</h3>
 * <ul>
 *   <li>rowCount() 固定返回 1（表示单个分片）</li>
 *   <li>不支持并行读取（只有一个分片）</li>
 *   <li>简化了系统表的实现</li>
 * </ul>
 *
 * @see Split 分片接口
 */
public abstract class SingletonSplit implements Split {

    @Override
    public long rowCount() {
        return 1;
    }
}
