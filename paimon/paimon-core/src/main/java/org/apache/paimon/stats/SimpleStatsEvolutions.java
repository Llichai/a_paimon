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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.schema.SchemaEvolutionUtil.createIndexCastMapping;
import static org.apache.paimon.schema.SchemaEvolutionUtil.devolveFilters;

/** 用于创建列统计数组序列化器的转换器。 */
public class SimpleStatsEvolutions {

    private final Function<Long, List<DataField>> schemaFields;
    private final long tableSchemaId;
    private final List<DataField> tableDataFields;
    private final AtomicReference<List<DataField>> tableFields;
    private final ConcurrentMap<Long, SimpleStatsEvolution> evolutions;

    public SimpleStatsEvolutions(Function<Long, List<DataField>> schemaFields, long tableSchemaId) {
        this.schemaFields = schemaFields;
        this.tableSchemaId = tableSchemaId;
        this.tableDataFields = schemaFields.apply(tableSchemaId);
        this.tableFields = new AtomicReference<>();
        this.evolutions = new ConcurrentHashMap<>();
    }

    public SimpleStatsEvolution getOrCreate(long dataSchemaId) {
        return evolutions.computeIfAbsent(
                dataSchemaId,
                id -> {
                    if (tableSchemaId == id) {
                        return new SimpleStatsEvolution(
                                new RowType(schemaFields.apply(id)), null, null);
                    }

                    // Get atomic schema fields.
                    List<DataField> schemaTableFields =
                            tableFields.updateAndGet(v -> v == null ? tableDataFields : v);
                    List<DataField> dataFields = schemaFields.apply(id);
                    IndexCastMapping indexCastMapping =
                            createIndexCastMapping(schemaTableFields, schemaFields.apply(id));
                    @Nullable int[] indexMapping = indexCastMapping.getIndexMapping();
                    // Create col stats array serializer with schema evolution
                    return new SimpleStatsEvolution(
                            new RowType(dataFields),
                            indexMapping,
                            indexCastMapping.getCastMapping());
                });
    }

    /**
     * 如果文件的模式 ID 不等于当前表模式 ID,则将过滤器转换为演化安全的过滤器,如果无法转换则返回 null。
     */
    @Nullable
    public Predicate tryDevolveFilter(long dataSchemaId, @Nullable Predicate filter) {
        if (filter == null || dataSchemaId == tableSchemaId) {
            return filter;
        }

        // 过滤器 p1 && p2,如果只有 p1 安全,我们只返回 p1 以尽最大努力进行过滤,并让计算引擎执行 p2。
        List<Predicate> filters = PredicateBuilder.splitAnd(filter);
        List<Predicate> devolved =
                devolveFilters(tableDataFields, schemaFields.apply(dataSchemaId), filters, false);

        return devolved.isEmpty() ? null : PredicateBuilder.and(devolved);
    }

    /**
     * 过滤不安全的过滤器,例如过滤器为 'a > 9',旧类型为 String,新类型为 Int,
     * 如果记录为 9、10 和 11,则演化的过滤器不安全。
     */
    @Nullable
    public Predicate filterUnsafeFilter(
            long dataSchemaId, @Nullable Predicate filter, boolean keepNewFieldFilter) {
        if (filter == null || dataSchemaId == tableSchemaId) {
            return filter;
        }

        List<Predicate> filters = PredicateBuilder.splitAnd(filter);
        List<DataField> oldSchema = schemaFields.apply(dataSchemaId);
        List<Predicate> result = new ArrayList<>();
        for (Predicate predicate : filters) {
            if (!devolveFilters(
                            tableDataFields,
                            oldSchema,
                            singletonList(predicate),
                            keepNewFieldFilter)
                    .isEmpty()) {
                result.add(predicate);
            }
        }
        return result.isEmpty() ? null : PredicateBuilder.and(result);
    }

    public List<DataField> tableDataFields() {
        return tableDataFields;
    }
}
