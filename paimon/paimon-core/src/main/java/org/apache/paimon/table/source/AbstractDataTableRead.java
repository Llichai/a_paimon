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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ListUtils;
import org.apache.paimon.utils.ProjectedRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;

/**
 * 抽象数据表读取类，为数据表提供通用的读取功能。
 *
 * <p>AbstractDataTableRead 是所有数据表读取实现的抽象基类，封装了通用的读取逻辑，
 * 包括：过滤执行、列裁剪、查询授权等。
 *
 * <h3>架构层次</h3>
 * <pre>
 * InnerTableRead (接口)
 *     ↓
 * AbstractDataTableRead (抽象基类，通用逻辑)
 *     ↓
 * ├── KeyValueTableRead (主键表读取)
 * └── AppendTableRead (追加表读取)
 * </pre>
 *
 * <h3>核心功能</h3>
 * <ul>
 *   <li><b>过滤执行</b>: 在读取阶段执行过滤条件（精确过滤）</li>
 *   <li><b>列裁剪</b>: 只读取需要的列，减少 IO 量</li>
 *   <li><b>查询授权</b>: 支持细粒度的数据访问控制</li>
 *   <li><b>投影下推</b>: 将列投影下推到底层读取器</li>
 * </ul>
 *
 * <h3>过滤执行流程</h3>
 * <ol>
 *   <li>调用 {@link #withFilter(Predicate)} 设置过滤条件</li>
 *   <li>调用 {@link #executeFilter()} 启用过滤执行</li>
 *   <li>读取数据时，过滤器会在返回前过滤每一行</li>
 * </ol>
 *
 * <h3>列裁剪流程</h3>
 * <ol>
 *   <li>调用 {@link #withReadType(RowType)} 或 {@link #withProjection(int[])} 设置要读取的列</li>
 *   <li>调用 {@link #applyReadType(RowType)} 将列裁剪应用到底层读取器</li>
 *   <li>只读取指定的列，减少 IO 量</li>
 * </ol>
 *
 * <h3>查询授权</h3>
 * <p>如果 Split 是 {@link QueryAuthSplit}，读取器会应用授权规则：
 * <ul>
 *   <li>限制可访问的列（列级别权限）</li>
 *   <li>添加额外的过滤条件（行级别权限）</li>
 * </ul>
 *
 * <h3>子类实现</h3>
 * <p>子类需要实现以下方法：
 * <ul>
 *   <li>{@link #applyReadType(RowType)}: 应用列裁剪到底层读取器</li>
 *   <li>{@link #reader(Split)}: 创建底层数据读取器</li>
 *   <li>{@link #innerWithFilter(Predicate)}: 应用过滤条件到底层读取器</li>
 * </ul>
 *
 * @see InnerTableRead 表读取接口
 * @see KeyValueTableRead 主键表读取实现
 * @see AppendTableRead 追加表读取实现
 */
public abstract class AbstractDataTableRead implements InnerTableRead {

    private RowType readType;
    private boolean executeFilter = false;
    private Predicate predicate;
    private final TableSchema schema;

    public AbstractDataTableRead(TableSchema schema) {
        this.schema = schema;
    }

    public abstract void applyReadType(RowType readType);

    public abstract RecordReader<InternalRow> reader(Split split) throws IOException;

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        return this;
    }

    @Override
    public final InnerTableRead withFilter(Predicate predicate) {
        this.predicate = predicate;
        return innerWithFilter(predicate);
    }

    protected abstract InnerTableRead innerWithFilter(Predicate predicate);

    @Override
    public TableRead executeFilter() {
        this.executeFilter = true;
        return this;
    }

    @Override
    public final InnerTableRead withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        return withReadType(schema.logicalRowType().project(projection));
    }

    @Override
    public final InnerTableRead withReadType(RowType readType) {
        this.readType = readType;
        applyReadType(readType);
        return this;
    }

    @Override
    public final RecordReader<InternalRow> createReader(Split split) throws IOException {
        TableQueryAuthResult authResult = null;
        if (split instanceof QueryAuthSplit) {
            QueryAuthSplit authSplit = (QueryAuthSplit) split;
            split = authSplit.split();
            authResult = authSplit.authResult();
        }
        RecordReader<InternalRow> reader;
        if (authResult == null) {
            reader = reader(split);
        } else {
            reader = authedReader(split, authResult);
        }
        if (executeFilter) {
            reader = executeFilter(reader);
        }

        return reader;
    }

    private RecordReader<InternalRow> authedReader(Split split, TableQueryAuthResult authResult)
            throws IOException {
        RecordReader<InternalRow> reader;
        RowType tableType = schema.logicalRowType();
        RowType readType = this.readType == null ? tableType : this.readType;
        Predicate authPredicate = authResult.extractPredicate();
        ProjectedRow backRow = null;
        if (authPredicate != null) {
            Set<String> authFields = collectFieldNames(authPredicate);
            List<String> readFields = readType.getFieldNames();
            List<String> authAddNames = new ArrayList<>();
            Set<String> readFieldSet = new HashSet<>(readFields);
            for (String field : tableType.getFieldNames()) {
                if (authFields.contains(field) && !readFieldSet.contains(field)) {
                    authAddNames.add(field);
                }
            }
            if (!authAddNames.isEmpty()) {
                readType = tableType.project(ListUtils.union(readFields, authAddNames));
                withReadType(readType);
                backRow = ProjectedRow.from(readType.projectIndexes(readFields));
            }
        }
        reader = authResult.doAuth(reader(split), readType);
        if (backRow != null) {
            reader = reader.transform(backRow::replaceRow);
        }
        return reader;
    }

    private RecordReader<InternalRow> executeFilter(RecordReader<InternalRow> reader) {
        if (predicate == null) {
            return reader;
        }

        Predicate predicate = this.predicate;
        if (readType != null) {
            int[] projection = schema.logicalRowType().getFieldIndices(readType.getFieldNames());
            Optional<Predicate> optional =
                    predicate.visit(new PredicateProjectionConverter(projection));
            if (!optional.isPresent()) {
                return reader;
            }
            predicate = optional.get();
        }

        Predicate finalFilter = predicate;
        return reader.filter(finalFilter::test);
    }
}
