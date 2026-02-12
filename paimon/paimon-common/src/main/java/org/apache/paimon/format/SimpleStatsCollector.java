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

package org.apache.paimon.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.util.Arrays;

import static org.apache.paimon.statistics.SimpleColStatsCollector.createFullStatsFactories;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 统计信息收集器，用于从一系列记录中提取每列的统计信息。
 *
 * <p>该收集器可以收集每列的最小值、最大值和空值数量等统计信息，
 * 这些信息可用于查询优化、数据跳过和元数据管理。
 */
public class SimpleStatsCollector {

    /** 行数据到对象数组的转换器 */
    private final RowDataToObjectArrayConverter converter;
    /** 每列的统计信息收集器 */
    private final SimpleColStatsCollector[] statsCollectors;
    /** 每个字段的序列化器 */
    private final Serializer<Object>[] fieldSerializers;
    /** 是否禁用统计信息收集 */
    private final boolean isDisabled;

    /**
     * 构造函数，为所有列创建完整的统计信息收集器。
     *
     * @param rowType 行类型
     */
    public SimpleStatsCollector(RowType rowType) {
        this(rowType, createFullStatsFactories(rowType.getFieldCount()));
    }

    /**
     * 构造函数，使用自定义的统计信息收集器工厂。
     *
     * @param rowType 行类型
     * @param collectorFactory 统计信息收集器工厂数组
     */
    public SimpleStatsCollector(
            RowType rowType, SimpleColStatsCollector.Factory[] collectorFactory) {
        int numFields = rowType.getFieldCount();
        checkArgument(
                numFields == collectorFactory.length,
                "numFields %s should equal to stats length %s.",
                numFields,
                collectorFactory.length);
        this.statsCollectors = SimpleColStatsCollector.create(collectorFactory);
        this.converter = new RowDataToObjectArrayConverter(rowType);
        this.fieldSerializers = new Serializer[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldSerializers[i] = InternalSerializers.create(rowType.getTypeAt(i));
        }
        this.isDisabled =
                Arrays.stream(statsCollectors)
                        .allMatch(p -> p instanceof NoneSimpleColStatsCollector);
    }

    /**
     * 判断统计信息收集是否被禁用。
     *
     * @return 如果所有列都不收集统计信息返回 true，否则返回 false
     */
    public boolean isDisabled() {
        return isDisabled;
    }

    /**
     * 使用新的行数据更新统计信息。
     *
     * <p><b>重要</b>：此行的字段不应被重用，因为它们会直接存储在收集器中。
     *
     * @param row 数据行
     */
    public void collect(InternalRow row) {
        Object[] objects = converter.convert(row);
        for (int i = 0; i < row.getFieldCount(); i++) {
            SimpleColStatsCollector collector = statsCollectors[i];
            Object obj = objects[i];
            collector.collect(obj, fieldSerializers[i]);
        }
    }

    /**
     * 提取收集到的统计信息。
     *
     * @return 每列的统计信息数组
     */
    public SimpleColStats[] extract() {
        SimpleColStats[] stats = new SimpleColStats[this.statsCollectors.length];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = this.statsCollectors[i].result();
        }
        return stats;
    }
}
