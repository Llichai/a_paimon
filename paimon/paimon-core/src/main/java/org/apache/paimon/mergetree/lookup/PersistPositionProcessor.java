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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_LONG_SIZE;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeLong;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;

/**
 * 位置持久化处理器
 *
 * <p>返回 {@link FilePosition} 的 {@link PersistProcessor}。
 *
 * <p>特点：
 * <ul>
 *   <li>持久化：仅存储行位置（变长编码）
 *   <li>读取：返回 FilePosition（文件名 + 行位置）
 *   <li>需要行位置信息
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Deletion Vector：标记需要删除的行位置
 *   <li>LOOKUP changelog 模式：定位原始数据行
 *   <li>最小化存储：只存储位置信息，不存储值
 * </ul>
 *
 * <p>存储格式：
 * <pre>
 * [变长编码的行位置]
 * </pre>
 *
 * <p>示例：
 * <pre>
 * // 写入：key -> [rowPosition: 1234]（变长编码）
 * // 查询：key -> FilePosition("file001.parquet", 1234)
 * </pre>
 */
public class PersistPositionProcessor implements PersistProcessor<FilePosition> {

    /**
     * 需要行位置
     *
     * @return true
     */
    @Override
    public boolean withPosition() {
        return true;
    }

    /**
     * 不支持不带行位置的持久化
     *
     * @param kv 键值对
     * @return 不支持
     */
    @Override
    public byte[] persistToDisk(KeyValue kv) {
        throw new UnsupportedOperationException();
    }

    /**
     * 持久化到磁盘（仅存储行位置）
     *
     * <p>使用变长编码节省空间
     *
     * @param kv 键值对
     * @param rowPosition 行位置
     * @return 变长编码的行位置字节数组
     */
    @Override
    public byte[] persistToDisk(KeyValue kv, long rowPosition) {
        byte[] bytes = new byte[MAX_VAR_LONG_SIZE]; // 最大变长编码大小
        int len = encodeLong(bytes, rowPosition); // 变长编码
        return Arrays.copyOf(bytes, len); // 截取实际长度
    }

    /**
     * 从磁盘读取
     *
     * @param key 键
     * @param level 层级
     * @param bytes 字节数组（变长编码的行位置）
     * @param fileName 文件名
     * @return FilePosition
     */
    @Override
    public FilePosition readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        long rowPosition = decodeLong(bytes, 0); // 变长解码
        return new FilePosition(fileName, rowPosition);
    }

    /**
     * 创建工厂
     *
     * @return 工厂实例
     */
    public static Factory<FilePosition> factory() {
        return new Factory<FilePosition>() {
            @Override
            public String identifier() {
                return "position";
            }

            @Override
            public PersistProcessor<FilePosition> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistPositionProcessor();
            }
        };
    }
}
