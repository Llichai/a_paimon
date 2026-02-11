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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.function.Function;

/**
 * 键值数据文件写入器的标准实现类。
 *
 * <p>用于写入包含完整键值对数据的文件,区别于精简模式(Thin Mode):
 * <ul>
 *   <li>标准模式: 存储 [键字段, _SEQUENCE_NUMBER_, _ROW_KIND_, 值字段]
 *   <li>精简模式: 存储 [_SEQUENCE_NUMBER_, _ROW_KIND_, 值字段] (键字段从值字段推导)
 * </ul>
 *
 * <p>主要功能:
 * <ul>
 *   <li>将 KeyValue 记录转换为内部行格式并写入文件
 *   <li>分别收集键字段和值字段的统计信息
 *   <li>支持多种文件格式(Parquet、ORC、Avro)
 *   <li>为每个文件生成完整的元数据(DataFileMeta)
 * </ul>
 *
 * <p>统计信息提取:
 * <ul>
 *   <li>rowStats 布局: [键字段统计, _SEQUENCE_NUMBER_统计, _ROW_KIND_统计, 值字段统计]
 *   <li>键统计提取: rowStats[0 : numKeyFields]
 *   <li>值统计提取: rowStats[numKeyFields + 2 : end] (跳过2个系统字段)
 * </ul>
 *
 * @see KeyValueThinDataFileWriterImpl 精简模式写入器
 * @see KeyValueDataFileWriter 键值写入器基类
 */
public class KeyValueDataFileWriterImpl extends KeyValueDataFileWriter {

    public KeyValueDataFileWriterImpl(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            long schemaId,
            int level,
            CoreOptions options,
            FileSource fileSource,
            FileIndexOptions fileIndexOptions,
            boolean isExternalPath) {
        super(
                fileIO,
                context,
                path,
                converter,
                keyType,
                valueType,
                KeyValue.schema(keyType, valueType),
                schemaId,
                level,
                options,
                fileSource,
                fileIndexOptions,
                isExternalPath);
    }

    /**
     * 从行统计信息中提取键和值的统计信息。
     *
     * <p>标准模式下的行统计布局:
     * <pre>
     * [键字段1统计, 键字段2统计, ..., _SEQUENCE_NUMBER_统计, _ROW_KIND_统计, 值字段1统计, 值字段2统计, ...]
     * </pre>
     *
     * @param rowStats 完整的行统计信息数组
     * @return 键值统计信息对 (键统计[], 值统计[])
     */
    @Override
    Pair<SimpleColStats[], SimpleColStats[]> fetchKeyValueStats(SimpleColStats[] rowStats) {
        int numKeyFields = keyType.getFieldCount();
        // 提取键统计: 从索引0开始,取numKeyFields个元素
        // 提取值统计: 从索引numKeyFields+2开始(跳过_SEQUENCE_NUMBER_和_ROW_KIND_),取剩余元素
        return Pair.of(
                Arrays.copyOfRange(rowStats, 0, numKeyFields),
                Arrays.copyOfRange(rowStats, numKeyFields + 2, rowStats.length));
    }
}
