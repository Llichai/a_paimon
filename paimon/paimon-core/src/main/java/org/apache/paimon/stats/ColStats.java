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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.OptionalUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 列统计信息类，支持以下统计指标。
 *
 * <ul>
 *   <li>distinctCount: 不同值的数量
 *   <li>min: 列的最小值
 *   <li>max: 列的最大值
 *   <li>nullCount: 空值数量
 *   <li>avgLen: 平均列长度
 *   <li>maxLen: 最大列长度
 * </ul>
 *
 * @param <T> 列的内部数据类型
 */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColStats<T> {

    private static final String FIELD_COL_ID = "colId";
    private static final String FIELD_DISTINCT_COUNT = "distinctCount";
    private static final String FIELD_MIN = "min";
    private static final String FIELD_MAX = "max";
    private static final String FIELD_NULL_COUNT = "nullCount";
    private static final String FIELD_AVG_LEN = "avgLen";
    private static final String FIELD_MAX_LEN = "maxLen";

    @JsonProperty(FIELD_COL_ID)
    private final int colId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_DISTINCT_COUNT)
    private final @Nullable Long distinctCount;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MIN)
    private @Nullable String serializedMin;

    private @Nullable Comparable<T> min;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MAX)
    private @Nullable String serializedMax;

    private @Nullable Comparable<T> max;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NULL_COUNT)
    private final @Nullable Long nullCount;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_AVG_LEN)
    private final @Nullable Long avgLen;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MAX_LEN)
    private final @Nullable Long maxLen;

    /** 该构造函数仅供 Jackson 使用。 */
    @JsonCreator
    public ColStats(
            @JsonProperty(FIELD_COL_ID) int colId,
            @JsonProperty(FIELD_DISTINCT_COUNT) @Nullable Long distinctCount,
            @JsonProperty(FIELD_MIN) @Nullable String serializedMin,
            @JsonProperty(FIELD_MAX) @Nullable String serializedMax,
            @JsonProperty(FIELD_NULL_COUNT) @Nullable Long nullCount,
            @JsonProperty(FIELD_AVG_LEN) @Nullable Long avgLen,
            @JsonProperty(FIELD_MAX_LEN) @Nullable Long maxLen) {
        this.colId = colId;
        this.distinctCount = distinctCount;
        this.serializedMin = serializedMin;
        this.serializedMax = serializedMax;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    private ColStats(
            int colId,
            @Nullable Long distinctCount,
            @Nullable Comparable<T> min,
            @Nullable Comparable<T> max,
            @Nullable Long nullCount,
            @Nullable Long avgLen,
            @Nullable Long maxLen) {
        this.colId = colId;
        this.distinctCount = distinctCount;
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    /** 创建列统计信息实例。 */
    public static <T> ColStats<T> newColStats(
            int colId,
            @Nullable Long distinctCount,
            @Nullable Comparable<T> min,
            @Nullable Comparable<T> max,
            @Nullable Long nullCount,
            @Nullable Long avgLen,
            @Nullable Long maxLen) {
        return new ColStats<>(colId, distinctCount, min, max, nullCount, avgLen, maxLen);
    }

    /** 返回列 ID。 */
    public int colId() {
        return colId;
    }

    /** 返回不同值的数量。 */
    public OptionalLong distinctCount() {
        return OptionalUtils.ofNullable(distinctCount);
    }

    /** 返回最小值。 */
    public Optional<Comparable<T>> min() {
        return Optional.ofNullable(min);
    }

    /** 返回最大值。 */
    public Optional<Comparable<T>> max() {
        return Optional.ofNullable(max);
    }

    /** 返回空值数量。 */
    public OptionalLong nullCount() {
        return OptionalUtils.ofNullable(nullCount);
    }

    /** 返回平均长度。 */
    public OptionalLong avgLen() {
        return OptionalUtils.ofNullable(avgLen);
    }

    /** 返回最大长度。 */
    public OptionalLong maxLen() {
        return OptionalUtils.ofNullable(maxLen);
    }

    /** 将最小值和最大值序列化为字符串。 */
    @SuppressWarnings("unchecked")
    public void serializeFieldsToString(DataType dataType) {
        if ((min != null && serializedMin == null) || (max != null && serializedMax == null)) {
            Serializer<T> serializer = InternalSerializers.create(dataType);
            if (min != null && serializedMin == null) {
                serializedMin = serializer.serializeToString((T) min);
            }
            if (max != null && serializedMax == null) {
                serializedMax = serializer.serializeToString((T) max);
            }
        }
    }

    /** 从字符串反序列化最小值和最大值。 */
    @SuppressWarnings("unchecked")
    public void deserializeFieldsFromString(DataType dataType) {
        if ((serializedMin != null && min == null) || (serializedMax != null && max == null)) {
            Serializer<T> serializer = InternalSerializers.create(dataType);
            if (serializedMin != null && min == null) {
                min = (Comparable<T>) serializer.deserializeFromString(serializedMin);
            }
            if (serializedMax != null && max == null) {
                max = (Comparable<T>) serializer.deserializeFromString(serializedMax);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColStats<?> colStats = (ColStats<?>) o;
        return colId == colStats.colId
                && Objects.equals(distinctCount, colStats.distinctCount)
                && Objects.equals(min, colStats.min)
                && Objects.equals(max, colStats.max)
                && Objects.equals(nullCount, colStats.nullCount)
                && Objects.equals(avgLen, colStats.avgLen)
                && Objects.equals(maxLen, colStats.maxLen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(colId, distinctCount, min, max, nullCount, avgLen, maxLen);
    }

    @Override
    public String toString() {
        return JsonSerdeUtil.toJson(this);
    }
}
