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

package org.apache.paimon.stats;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 列的统计信息,支持以下统计指标。
 *
 * <ul>
 *   <li>minValues: 列的最小值
 *   <li>maxValues: 列的最大值
 *   <li>nullCounts: 列的空值数量
 * </ul>
 *
 * <p>所有统计信息都以二进制形式存储，可显著减少内存消耗，但代价是获取时需要知道列类型。
 *
 * @since 0.9.0
 */
@Public
public class SimpleStats {

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_MIN_VALUES", newBytesType(false)),
                            new DataField(1, "_MAX_VALUES", newBytesType(false)),
                            new DataField(2, "_NULL_COUNTS", new ArrayType(new BigIntType(true)))));

    /** 列数为 0 时的空统计信息。 */
    public static final SimpleStats EMPTY_STATS =
            new SimpleStats(EMPTY_ROW, EMPTY_ROW, BinaryArray.fromLongArray(new Long[0]));

    private final BinaryRow minValues;
    private final BinaryRow maxValues;
    private final BinaryArray nullCounts;

    public SimpleStats(BinaryRow minValues, BinaryRow maxValues, BinaryArray nullCounts) {
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.nullCounts = nullCounts;
    }

    public BinaryRow minValues() {
        return minValues;
    }

    public BinaryRow maxValues() {
        return maxValues;
    }

    public BinaryArray nullCounts() {
        return nullCounts;
    }

    public InternalRow toRow() {
        return GenericRow.of(
                serializeBinaryRow(minValues), serializeBinaryRow(maxValues), nullCounts);
    }

    public static SimpleStats fromRow(InternalRow row) {
        BinaryRow minValues = deserializeBinaryRow(row.getBinary(0));
        BinaryRow maxValues = deserializeBinaryRow(row.getBinary(1));
        InternalArray nullCounts = row.getArray(2);
        if (minValues.getFieldCount() == 0
                && maxValues.getFieldCount() == 0
                && nullCounts.size() == 0) {
            return EMPTY_STATS;
        }
        return new SimpleStats(minValues, maxValues, BinaryArray.fromLongArray(nullCounts));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleStats that = (SimpleStats) o;
        return Objects.equals(minValues, that.minValues)
                && Objects.equals(maxValues, that.maxValues)
                && Objects.equals(nullCounts, that.nullCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minValues, maxValues, nullCounts);
    }
}
