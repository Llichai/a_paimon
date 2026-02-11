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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Iceberg 快照摘要类。
 *
 * <p>存储快照的摘要信息,以键值对形式存储元数据。
 *
 * <p>常用字段:
 * <ul>
 *   <li>operation: 操作类型(append、overwrite 等)
 *   <li>其他自定义元数据
 * </ul>
 *
 * <p>预定义的摘要常量:
 * <ul>
 *   <li>APPEND: 追加操作摘要
 *   <li>OVERWRITE: 覆盖操作摘要
 * </ul>
 *
 * <p>参考: <a href="https://iceberg.apache.org/spec/#snapshots">Iceberg 规范</a>
 *
 * @see IcebergSnapshot 快照类
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergSnapshotSummary {

    private static final String FIELD_OPERATION = "operation";

    public static final IcebergSnapshotSummary APPEND = new IcebergSnapshotSummary("append");
    public static final IcebergSnapshotSummary OVERWRITE = new IcebergSnapshotSummary("overwrite");

    private final Map<String, String> summary;

    @JsonCreator
    public IcebergSnapshotSummary(Map<String, String> summary) {
        this.summary = summary != null ? new HashMap<>(summary) : new HashMap<>();
    }

    public IcebergSnapshotSummary(String operation) {
        this.summary = new HashMap<>();
        this.summary.put(FIELD_OPERATION, operation);
    }

    @JsonValue
    public Map<String, String> getSummary() {
        return new HashMap<>(summary);
    }

    public String operation() {
        return summary.get(FIELD_OPERATION);
    }

    public String get(String key) {
        return summary.get(key);
    }

    public void put(String key, String value) {
        summary.put(key, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summary);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergSnapshotSummary)) {
            return false;
        }

        IcebergSnapshotSummary that = (IcebergSnapshotSummary) o;
        return Objects.equals(summary, that.summary);
    }
}
