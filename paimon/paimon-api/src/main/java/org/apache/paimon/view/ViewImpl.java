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

package org.apache.paimon.view;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link View} 接口的默认实现。
 *
 * <p>该类是视图的标准实现,封装了视图的所有元数据信息,包括标识符、字段、
 * 查询语句、方言查询、注释和配置选项。
 *
 * <h2>主要特点</h2>
 * <ul>
 *   <li><b>不可变性</b>: 视图对象一旦创建就不可修改
 *   <li><b>完整元数据</b>: 包含视图的所有元数据信息
 *   <li><b>序列化支持</b>: 支持 JSON 序列化和反序列化
 *   <li><b>动态配置</b>: 支持通过 copy 方法添加动态选项
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建视图
 * Identifier identifier = Identifier.create("db", "my_view");
 * List<DataField> fields = Arrays.asList(
 *     new DataField(0, "id", DataTypes.BIGINT()),
 *     new DataField(1, "name", DataTypes.STRING())
 * );
 * String query = "SELECT id, name FROM source_table";
 * Map<String, String> dialects = new HashMap<>();
 * dialects.put("spark", "SELECT id, name FROM source_table");
 * String comment = "用户视图";
 * Map<String, String> options = new HashMap<>();
 *
 * View view = new ViewImpl(
 *     identifier, fields, query, dialects, comment, options
 * );
 *
 * // 使用视图
 * String name = view.name();              // "my_view"
 * String fullName = view.fullName();      // "db.my_view"
 * RowType rowType = view.rowType();       // 行类型
 * String sparkQuery = view.query("spark"); // Spark 方言查询
 *
 * // 创建带动态选项的副本
 * Map<String, String> dynamicOpts = new HashMap<>();
 * dynamicOpts.put("refresh.interval", "5min");
 * View newView = view.copy(dynamicOpts);
 * }</pre>
 *
 * <h2>内部结构</h2>
 * <p>ViewImpl 通过组合 {@link ViewSchema} 来管理视图的 schema 信息:
 * <ul>
 *   <li>identifier: 视图的唯一标识符
 *   <li>viewSchema: 包含字段、查询、方言、注释和选项
 * </ul>
 *
 * @see View
 * @see ViewSchema
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewImpl implements View {

    private final Identifier identifier;
    private final ViewSchema viewSchema;

    public ViewImpl(
            Identifier identifier,
            List<DataField> fields,
            String query,
            Map<String, String> dialects,
            @Nullable String comment,
            Map<String, String> options) {
        this.identifier = identifier;
        this.viewSchema = new ViewSchema(fields, query, dialects, comment, options);
    }

    @Override
    public String name() {
        return identifier.getObjectName();
    }

    @Override
    public String fullName() {
        return identifier.getFullName();
    }

    @Override
    public RowType rowType() {
        return new RowType(false, this.viewSchema.fields());
    }

    @Override
    public String query() {
        return this.viewSchema.query();
    }

    @Override
    public Map<String, String> dialects() {
        return this.viewSchema.dialects();
    }

    @Override
    public Optional<String> comment() {
        return Optional.ofNullable(this.viewSchema.comment());
    }

    @Override
    public Map<String, String> options() {
        return this.viewSchema.options();
    }

    @Override
    public View copy(Map<String, String> dynamicOptions) {
        Map<String, String> newOptions = new HashMap<>(options());
        newOptions.putAll(dynamicOptions);
        return new ViewImpl(
                identifier,
                viewSchema.fields(),
                query(),
                dialects(),
                viewSchema.comment(),
                newOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ViewImpl view = (ViewImpl) o;
        return Objects.equals(identifier, view.identifier)
                && Objects.equals(viewSchema, view.viewSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, viewSchema);
    }
}
