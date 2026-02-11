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
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * 键值数据文件写入器的精简模式实现类。
 *
 * <p>精简模式(Thin Mode)特点:
 * <ul>
 *   <li><b>存储格式</b>: 只存储 [_SEQUENCE_NUMBER_, _ROW_KIND_, 值字段]
 *   <li><b>键字段省略</b>: 不写入键字段,从值字段推导
 *   <li><b>适用条件</b>:
 *       <ul>
 *         <li>所有键字段必须是特殊字段(_key_xxx)
 *         <li>键字段对应的原始字段必须在值字段中存在
 *       </ul>
 *   <li><b>优势</b>: 减少存储空间,避免重复存储相同数据
 * </ul>
 *
 * <p>键统计信息映射:
 * <ul>
 *   <li>问题: 精简模式不写入键字段,但元数据需要键统计信息
 *   <li>解决: 通过keyStatMapping从值统计映射到键统计
 *   <li>映射关系:
 *       <pre>
 *       键字段ID = 原始字段ID + KEY_FIELD_ID_START
 *       keyStatMapping[i] = 值字段索引(通过ID反查)
 *       </pre>
 * </ul>
 *
 * <p>示例:
 * <pre>{@code
 * // 表定义: PRIMARY KEY(id, name)
 * // 键字段: [_key_id, _key_name]  (id=100, name=101)
 * // 值字段: [id, name, age, city]  (id=0, name=1, age=2, city=3)
 *
 * // 精简模式写入: [_SEQUENCE_NUMBER_, _ROW_KIND_, id, name, age, city]
 * // 标准模式写入: [_key_id, _key_name, _SEQUENCE_NUMBER_, _ROW_KIND_, id, name, age, city]
 *
 * // 键统计映射:
 * // keyStatMapping[0] = 0  // _key_id的统计来自值字段id(索引0)
 * // keyStatMapping[1] = 1  // _key_name的统计来自值字段name(索引1)
 * }</pre>
 *
 * <p>对比标准模式:
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>标准模式</th>
 *     <th>精简模式</th>
 *   </tr>
 *   <tr>
 *     <td>存储字段</td>
 *     <td>键+系统+值</td>
 *     <td>系统+值</td>
 *   </tr>
 *   <tr>
 *     <td>文件大小</td>
 *     <td>较大</td>
 *     <td>较小</td>
 *   </tr>
 *   <tr>
 *     <td>统计提取</td>
 *     <td>直接提取</td>
 *     <td>需要映射</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>所有情况</td>
 *     <td>键在值中存在</td>
 *   </tr>
 * </table>
 *
 * @see KeyValueDataFileWriterImpl 标准模式实现
 * @see KeyValueDataFileWriter 键值写入器基类
 */
public class KeyValueThinDataFileWriterImpl extends KeyValueDataFileWriter {

    /** 键统计信息到值统计信息的映射索引。 */
    private final int[] keyStatMapping;

    public KeyValueThinDataFileWriterImpl(
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
                KeyValue.schema(RowType.of(), valueType),
                schemaId,
                level,
                options,
                fileSource,
                fileIndexOptions,
                isExternalPath);
        Map<Integer, Integer> idToIndex = new HashMap<>(valueType.getFieldCount());
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            idToIndex.put(valueType.getFields().get(i).id(), i);
        }
        this.keyStatMapping = new int[keyType.getFieldCount()];
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            keyStatMapping[i] =
                    idToIndex.get(
                            keyType.getFields().get(i).id() - SpecialFields.KEY_FIELD_ID_START);
        }
    }

    /**
     * Fetches the key and value statistics.
     *
     * @param rowStats The row statistics.
     * @return A pair of key statistics and value statistics.
     */
    @Override
    Pair<SimpleColStats[], SimpleColStats[]> fetchKeyValueStats(SimpleColStats[] rowStats) {
        int numKeyFields = keyType.getFieldCount();
        // In thin mode, there is no key stats in rowStats, so we only jump
        // _SEQUENCE_NUMBER_ and _ROW_KIND_ stats. Therefore, the 'from' value is 2.
        SimpleColStats[] valFieldStats = Arrays.copyOfRange(rowStats, 2, rowStats.length);
        // Thin mode on, so need to map value stats to key stats.
        SimpleColStats[] keyStats = new SimpleColStats[numKeyFields];
        for (int i = 0; i < keyStatMapping.length; i++) {
            keyStats[i] = valFieldStats[keyStatMapping[i]];
        }

        return Pair.of(keyStats, valFieldStats);
    }
}
