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

package org.apache.paimon.table;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * 表回滚时间点。
 *
 * <p>定义了表回滚操作的目标时间点，支持两种类型:
 * <ul>
 *   <li>快照时间点 (SnapshotInstant): 回滚到指定的快照 ID
 *   <li>标签时间点 (TagInstant): 回滚到指定的标签名称
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 回滚到快照 ID 100
 * Instant instant1 = Instant.snapshot(100L);
 *
 * // 回滚到标签 "v1.0"
 * Instant instant2 = Instant.tag("v1.0");
 * }</pre>
 */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = Instant.Types.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(value = Instant.SnapshotInstant.class, name = Instant.Types.SNAPSHOT),
    @JsonSubTypes.Type(value = Instant.TagInstant.class, name = Instant.Types.TAG)
})
public interface Instant extends Serializable {

    /**
     * 创建快照时间点。
     *
     * @param snapshotId 快照 ID
     * @return 快照时间点实例
     */
    static Instant snapshot(Long snapshotId) {
        return new SnapshotInstant(snapshotId);
    }

    /**
     * 创建标签时间点。
     *
     * @param tagName 标签名称
     * @return 标签时间点实例
     */
    static Instant tag(String tagName) {
        return new TagInstant(tagName);
    }

    /**
     * 快照时间点，用于表回滚到指定快照。
     *
     * <p>通过快照 ID 来标识回滚目标，适用于精确控制回滚版本的场景。
     */
    final class SnapshotInstant implements Instant {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_SNAPSHOT_ID = "snapshotId";

        /** 目标快照 ID。 */
        @JsonProperty(FIELD_SNAPSHOT_ID)
        private final long snapshotId;

        @JsonCreator
        public SnapshotInstant(@JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId) {
            this.snapshotId = snapshotId;
        }

        /**
         * 获取快照 ID。
         *
         * @return 快照 ID
         */
        @JsonGetter(FIELD_SNAPSHOT_ID)
        public long getSnapshotId() {
            return snapshotId;
        }
    }

    /**
     * 标签时间点，用于表回滚到指定标签。
     *
     * <p>通过标签名称来标识回滚目标，适用于回滚到预定义的重要版本的场景。
     */
    final class TagInstant implements Instant {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_TAG_NAME = "tagName";

        /** 目标标签名称。 */
        @JsonProperty(FIELD_TAG_NAME)
        private final String tagName;

        @JsonCreator
        public TagInstant(@JsonProperty(FIELD_TAG_NAME) String tagName) {
            this.tagName = tagName;
        }

        /**
         * 获取标签名称。
         *
         * @return 标签名称
         */
        @JsonGetter(FIELD_TAG_NAME)
        public String getTagName() {
            return tagName;
        }
    }

    /**
     * 表回滚类型标识。
     *
     * <p>用于 JSON 序列化/反序列化时区分不同的时间点类型。
     */
    class Types {
        /** 类型字段名。 */
        public static final String FIELD_TYPE = "type";

        /** 快照类型标识。 */
        public static final String SNAPSHOT = "snapshot";

        /** 标签类型标识。 */
        public static final String TAG = "tag";

        private Types() {}
    }
}
