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

package org.apache.paimon.mergetree;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.FileIOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * 数据文件读取器
 *
 * <p>用于 Lookup 场景的 {@link DataFileMeta} 读取器。
 *
 * <p>核心功能：
 * <ul>
 *   <li>get：根据键查询值（Lookup）
 *   <li>fileKibiBytes：获取本地文件大小（KiB）
 *   <li>close：关闭读取器并删除本地临时文件
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：在本地缓存远程文件，提供快速查询
 *   <li>文件下载后创建读取器，用于读取和查询
 *   <li>关闭时自动清理本地临时文件
 * </ul>
 */
public class DataFileReader implements Closeable {

    /** 本地临时文件 */
    private final File localFile;
    /** 远程文件元数据 */
    private final DataFileMeta remoteFile;
    /** Lookup 存储读取器 */
    private final LookupStoreReader reader;

    /**
     * 构造数据文件读取器
     *
     * @param localFile 本地临时文件
     * @param remoteFile 远程文件元数据
     * @param reader Lookup 存储读取器
     */
    public DataFileReader(File localFile, DataFileMeta remoteFile, LookupStoreReader reader) {
        this.localFile = localFile;
        this.remoteFile = remoteFile;
        this.reader = reader;
    }

    /**
     * 根据键查询值
     *
     * @param key 键（字节数组）
     * @return 值（字节数组），如果不存在则返回 null
     * @throws IOException IO 异常
     */
    @Nullable
    public byte[] get(byte[] key) throws IOException {
        return reader.lookup(key);
    }

    /**
     * 获取本地文件大小（KiB）
     *
     * @return 文件大小（KiB）
     */
    public int fileKibiBytes() {
        long kibiBytes = localFile.length() >> 10; // 右移10位除以1024
        if (kibiBytes > Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Lookup file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
        }
        return (int) kibiBytes;
    }

    /**
     * 获取远程文件元数据
     *
     * @return 远程文件元数据
     */
    public DataFileMeta remoteFile() {
        return remoteFile;
    }

    /**
     * 关闭读取器
     *
     * <p>关闭底层读取器并删除本地临时文件
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        reader.close();
        FileIOUtils.deleteFileOrDirectory(localFile); // 删除本地临时文件
    }
}
