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

package org.apache.paimon.io;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 键值对数据文件记录读取器。
 *
 * <p>将内部行格式的数据文件记录转换为 {@link KeyValue} 对象。
 * 这是键值表存储层的核心读取器,负责:
 * <ul>
 *   <li>将行数据反序列化为键值对</li>
 *   <li>设置记录的层级信息(用于LSM树)</li>
 *   <li>过滤空记录</li>
 * </ul>
 *
 * @see KeyValue
 * @see KeyValueSerializer
 */
public class KeyValueDataFileRecordReader implements FileRecordReader<KeyValue> {

    /** 底层的内部行读取器 */
    private final FileRecordReader<InternalRow> reader;
    /** 键值对序列化器,用于将行转换为键值对 */
    private final KeyValueSerializer serializer;
    /** 文件的层级(LSM树的层级) */
    private final int level;

    /**
     * 构造键值对数据文件记录读取器。
     *
     * @param reader 内部行读取器
     * @param keyType 键类型
     * @param valueType 值类型
     * @param level 文件的层级
     */
    public KeyValueDataFileRecordReader(
            FileRecordReader<InternalRow> reader, RowType keyType, RowType valueType, int level) {
        this.reader = reader;
        this.serializer = new KeyValueSerializer(keyType, valueType);
        this.level = level;
    }

    /**
     * 读取一批记录。
     *
     * <p>从底层读取器读取一批内部行,然后转换为键值对。
     * 转换过程中会:
     * <ol>
     *   <li>过滤空行</li>
     *   <li>使用序列化器将行反序列化为键值对</li>
     *   <li>设置键值对的层级</li>
     * </ol>
     *
     * @return 键值对迭代器,如果没有更多数据则返回 null
     * @throws IOException 如果读取过程中发生I/O错误
     */
    @Nullable
    @Override
    public FileRecordIterator<KeyValue> readBatch() throws IOException {
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        // 转换内部行为键值对,并设置层级
        return iterator.transform(
                internalRow ->
                        internalRow == null
                                ? null
                                : serializer.fromRow(internalRow).setLevel(level));
    }

    /**
     * 关闭读取器,释放资源。
     *
     * @throws IOException 如果关闭过程中发生I/O错误
     */
    @Override
    public void close() throws IOException {
        reader.close();
    }
}
