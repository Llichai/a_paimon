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

package org.apache.paimon.index;

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.IntHashSet;
import org.apache.paimon.utils.IntIterator;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

/**
 * 动态bucket索引维护器。
 *
 * <p>维护动态bucket中的键哈希码索引。
 * 通过记录每个bucket中已存在的键哈希值,可以快速判断新数据应该分配到哪个bucket,
 * 避免重复扫描数据文件,提高写入性能。
 */
public class DynamicBucketIndexMaintainer {

    private final HashIndexFile indexFile;
    private final IntHashSet hashcode;

    private boolean modified;

    private DynamicBucketIndexMaintainer(
            HashIndexFile indexFile, @Nullable IndexFileMeta restoredFile) {
        this.indexFile = indexFile;
        IntHashSet hashcode = new IntHashSet();
        if (restoredFile != null) {
            hashcode = new IntHashSet((int) restoredFile.rowCount());
            restore(indexFile, hashcode, restoredFile);
        }
        this.hashcode = hashcode;
        this.modified = false;
    }

    private void restore(HashIndexFile indexFile, IntHashSet hashcode, IndexFileMeta file) {
        try (IntIterator iterator = indexFile.read(file)) {
            while (true) {
                try {
                    hashcode.add(iterator.next());
                } catch (EOFException ignored) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 通知有新记录写入。
     *
     * <p>将新记录的键哈希值添加到索引中。
     *
     * @param record 键值记录
     */
    public void notifyNewRecord(KeyValue record) {
        InternalRow key = record.key();
        if (!(key instanceof BinaryRow)) {
            throw new IllegalArgumentException("Unsupported key type: " + key.getClass());
        }
        boolean changed = hashcode.add(key.hashCode());
        if (changed) {
            modified = true;
        }
    }

    /**
     * 准备提交索引变更。
     *
     * <p>将索引变更写入文件。
     *
     * @return 索引文件元数据列表
     */
    public List<IndexFileMeta> prepareCommit() {
        if (modified) {
            IndexFileMeta entry;
            try {
                entry = indexFile.write(hashcode.toIntIterator());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            modified = false;
            return Collections.singletonList(entry);
        }
        return Collections.emptyList();
    }

    @VisibleForTesting
    public boolean isEmpty() {
        return hashcode.size() == 0;
    }

    /**
     * 创建 {@link DynamicBucketIndexMaintainer} 的工厂类。
     *
     * <p>负责恢复和创建索引维护器实例。
     */
    public static class Factory {

        private final IndexFileHandler handler;

        public Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        public IndexFileHandler indexFileHandler() {
            return handler;
        }

        public DynamicBucketIndexMaintainer create(
                BinaryRow partition, int bucket, @Nullable IndexFileMeta restoredFile) {
            return new DynamicBucketIndexMaintainer(
                    handler.hashIndex(partition, bucket), restoredFile);
        }
    }
}
