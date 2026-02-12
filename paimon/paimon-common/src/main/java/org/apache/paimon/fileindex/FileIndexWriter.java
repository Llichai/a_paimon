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
 * 文件索引写入器抽象基类。
 *
 * <p>用于构建文件索引。写入流程:
 * <ol>
 *   <li>创建写入器实例</li>
 *   <li>通过 {@link #writeRecord(Object)} 写入每个键值</li>
 *   <li>调用 {@link #serializedBytes()} 获取序列化后的索引数据</li>
 * </ol>
 *
 * <p>特点:
 * <ul>
 *   <li>自动跟踪是否为空索引(没有写入任何记录)</li>
 *   <li>子类需要实现具体的索引构建和序列化逻辑</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * FileIndexWriter writer = indexer.createWriter();
 * for (Object key : keys) {
 *     writer.writeRecord(key);
 * }
 * byte[] indexBytes = writer.serializedBytes();
 * }</pre>
 */
public abstract class FileIndexWriter {

    /** 标记索引是否为空(未写入任何记录) */
    private boolean empty = true;

    /**
     * 写入一条记录。
     *
     * <p>该方法会自动将 empty 标记设置为 false,然后调用 {@link #write(Object)} 进行实际写入。
     *
     * @param key 要索引的键值
     */
    public void writeRecord(Object key) {
        empty = false;
        write(key);
    }

    /**
     * 写入一个键值到索引中。
     *
     * <p>注意:键对象可能会被重用,如果需要在内存中保存,请务必手动复制。
     *
     * @param key 要索引的键值(可能被重用)
     */
    public abstract void write(Object key);

    /**
     * 获取序列化后的索引字节数据。
     *
     * @return 序列化的索引字节数组
     */
    public abstract byte[] serializedBytes();

    /**
     * 检查索引是否为空。
     *
     * @return true 表示未写入任何记录,false 表示已写入记录
     */
    public boolean empty() {
        return empty;
    }
}
