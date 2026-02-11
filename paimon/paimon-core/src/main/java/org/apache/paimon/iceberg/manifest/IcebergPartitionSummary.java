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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Iceberg Manifest 文件中存储的分区汇总信息类。
 *
 * <p>在 Iceberg 的 Manifest List 文件中,为每个分区存储汇总统计信息,
 * 用于查询优化和数据跳过(data skipping)。
 *
 * <p>包含的统计信息:
 * <ul>
 *   <li>containsNull: 分区是否包含 NULL 值
 *   <li>containsNan: 分区是否包含 NaN 值(用于浮点数)
 *   <li>lowerBound: 分区值的下界(二进制格式)
 *   <li>upperBound: 分区值的上界(二进制格式)
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>查询优化:根据分区边界值跳过不相关的分区
 *   <li>统计信息:快速了解分区的数据分布情况
 * </ul>
 *
 * <p>参考: <a href="https://iceberg.apache.org/spec/#manifest-lists">Iceberg 规范</a>
 *
 * @see IcebergPartitionSummarySerializer 序列化器
 */
public class IcebergPartitionSummary {

    /** 是否包含 NULL 值 */
    private final boolean containsNull;

    /** 是否包含 NaN 值 */
    private final boolean containsNan;

    /** 分区值下界(二进制序列化) */
    private final byte[] lowerBound;

    /** 分区值上界(二进制序列化) */
    private final byte[] upperBound;

    /**
     * 构造分区汇总信息。
     *
     * @param containsNull 是否包含 NULL
     * @param containsNan 是否包含 NaN
     * @param lowerBound 下界值
     * @param upperBound 上界值
     */
    public IcebergPartitionSummary(
            boolean containsNull, boolean containsNan, byte[] lowerBound, byte[] upperBound) {
        this.containsNull = containsNull;
        this.containsNan = containsNan;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public boolean containsNull() {
        return containsNull;
    }

    public boolean containsNan() {
        return containsNan;
    }

    public byte[] lowerBound() {
        return lowerBound;
    }

    public byte[] upperBound() {
        return upperBound;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(509, "contains_null", DataTypes.BOOLEAN().notNull()));
        fields.add(new DataField(518, "contains_nan", DataTypes.BOOLEAN()));
        fields.add(new DataField(510, "lower_bound", DataTypes.BYTES()));
        fields.add(new DataField(511, "upper_bound", DataTypes.BYTES()));
        return new RowType(false, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergPartitionSummary that = (IcebergPartitionSummary) o;
        return containsNull == that.containsNull
                && containsNan == that.containsNan
                && Arrays.equals(lowerBound, that.lowerBound)
                && Arrays.equals(upperBound, that.upperBound);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(containsNull, containsNan);
        result = 31 * result + Arrays.hashCode(lowerBound);
        result = 31 * result + Arrays.hashCode(upperBound);
        return result;
    }
}
