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

import javax.annotation.Nullable;

/**
 * 写入结果元数据。
 *
 * <p>该类封装了索引写入操作的结果信息,包括生成的文件名、行数和自定义元数据。
 * 用于在索引写入完成后传递写入统计信息。
 */
public class ResultEntry {

    /** 生成的索引文件名 */
    private final String fileName;

    /** 索引的行数 */
    private final long rowCount;

    /** 自定义元数据(可选) */
    @Nullable private final byte[] meta;

    /**
     * 构造结果条目。
     *
     * @param fileName 索引文件名
     * @param rowCount 索引行数
     * @param meta 自定义元数据,可为 null
     */
    public ResultEntry(String fileName, long rowCount, @Nullable byte[] meta) {
        this.fileName = fileName;
        this.rowCount = rowCount;
        this.meta = meta;
    }

    /** 返回索引文件名。 */
    public String fileName() {
        return fileName;
    }

    /** 返回索引行数。 */
    public long rowCount() {
        return rowCount;
    }

    /** 返回自定义元数据。 */
    public byte[] meta() {
        return meta;
    }
}
