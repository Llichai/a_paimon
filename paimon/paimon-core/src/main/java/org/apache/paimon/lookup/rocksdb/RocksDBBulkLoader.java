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

package org.apache.paimon.lookup.rocksdb;

import org.apache.paimon.lookup.ListBulkLoader;
import org.apache.paimon.lookup.ValueBulkLoader;
import org.apache.paimon.utils.ListDelimitedSerializer;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.rocksdb.TtlDB;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * RocksDB 批量加载器实现,通过直接生成 SST 文件来高效导入数据.
 *
 * <p>RocksDBBulkLoader 使用 RocksDB 的 {@link SstFileWriter} API 直接生成 SST 文件,
 * 然后通过 {@code ingestExternalFile} API 将文件导入到数据库,避免了常规写入的写放大。
 *
 * <h2>批量加载流程:</h2>
 * <ol>
 *   <li>创建 SstFileWriter,打开临时 SST 文件
 *   <li>按键的升序逐条写入键值对
 *   <li>当 SST 文件达到目标大小时,关闭当前文件并创建新文件
 *   <li>调用 finish() 时,将所有 SST 文件导入到 RocksDB
 * </ol>
 *
 * <h2>性能优势:</h2>
 * <ul>
 *   <li><b>避免写放大</b>: 直接生成 LSM-tree 的底层文件,不需要多次合并
 *   <li><b>高吞吐</b>: 批量写入 SST 文件比逐条插入快 10-100 倍
 *   <li><b>低内存开销</b>: 不需要维护 MemTable
 * </ul>
 *
 * <h2>TTL 支持:</h2>
 * <p>如果数据库启用了 TTL,批量加载器会自动在每个值后面附加时间戳。
 *
 * <h2>文件管理:</h2>
 * <ul>
 *   <li>SST 文件命名: sst-{uuid}-{index}
 *   <li>文件大小: 由 options.targetFileSizeBase() 控制
 *   <li>自动分片: 每 1000 条记录检查一次文件大小,超过目标大小则创建新文件
 * </ul>
 *
 * <h2>使用约束:</h2>
 * <ul>
 *   <li>键必须按升序写入
 *   <li>键不能重复
 *   <li>finish() 必须被调用以完成导入
 * </ul>
 *
 * @see SstFileWriter RocksDB SST 文件写入器
 * @see ValueBulkLoader 单值批量加载接口
 * @see ListBulkLoader 列表批量加载接口
 */
public class RocksDBBulkLoader implements ValueBulkLoader, ListBulkLoader {

    /** 唯一标识符,用于生成 SST 文件名. */
    private final String uuid = UUID.randomUUID().toString();

    /** 列表序列化器,用于序列化值列表. */
    private final ListDelimitedSerializer listSerializer = new ListDelimitedSerializer();

    /** 列族句柄. */
    private final ColumnFamilyHandle columnFamily;

    /** RocksDB 数据存储路径. */
    private final String path;

    /** RocksDB 实例. */
    private final RocksDB db;

    /** 是否启用 TTL. */
    private final boolean isTtlEnabled;

    /** RocksDB 选项. */
    private final Options options;

    /** 已生成的 SST 文件列表. */
    private final List<String> files = new ArrayList<>();

    /** 当前时间戳(秒),用于 TTL. */
    private final int currentTimeSeconds;

    /** 当前的 SST 文件写入器. */
    private SstFileWriter writer = null;

    /** SST 文件索引. */
    private int sstIndex = 0;

    /** 当前 SST 文件的记录数. */
    private long recordNum = 0;

    public RocksDBBulkLoader(
            RocksDB db, Options options, ColumnFamilyHandle columnFamily, String path) {
        this.db = db;
        this.isTtlEnabled = db instanceof TtlDB;
        this.options = options;
        this.columnFamily = columnFamily;
        this.path = path;
        this.currentTimeSeconds = (int) (System.currentTimeMillis() / 1000);
    }

    @Override
    public void write(byte[] key, byte[] value) throws WriteException {
        try {
            if (writer == null) {
                writer = new SstFileWriter(new EnvOptions(), options);
                String path = new File(this.path, "sst-" + uuid + "-" + (sstIndex++)).getPath();
                writer.open(path);
                files.add(path);
            }

            if (isTtlEnabled) {
                value = appendTimestamp(value);
            }

            try {
                writer.put(key, value);
            } catch (RocksDBException e) {
                throw new WriteException(e);
            }

            recordNum++;
            if (recordNum % 1000 == 0 && writer.fileSize() >= options.targetFileSizeBase()) {
                writer.finish();
                writer.close();
                writer = null;
                recordNum = 0;
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(byte[] key, List<byte[]> value) throws WriteException {
        byte[] bytes;
        try {
            bytes = listSerializer.serializeList(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        write(key, bytes);
    }

    private byte[] appendTimestamp(byte[] value) {
        byte[] newValue = new byte[value.length + 4];
        System.arraycopy(value, 0, newValue, 0, value.length);
        newValue[value.length] = (byte) (currentTimeSeconds & 0xff);
        newValue[value.length + 1] = (byte) ((currentTimeSeconds >> 8) & 0xff);
        newValue[value.length + 2] = (byte) ((currentTimeSeconds >> 16) & 0xff);
        newValue[value.length + 3] = (byte) ((currentTimeSeconds >> 24) & 0xff);
        return newValue;
    }

    @Override
    public void finish() {
        try {
            if (writer != null) {
                writer.finish();
                writer.close();
            }

            if (files.size() > 0) {
                IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions();
                db.ingestExternalFile(columnFamily, files, ingestOptions);
                ingestOptions.close();
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
