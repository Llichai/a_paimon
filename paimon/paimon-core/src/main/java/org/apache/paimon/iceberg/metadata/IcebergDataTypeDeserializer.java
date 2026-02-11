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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * Iceberg 数据类型的 JSON 反序列化器。
 *
 * <p>从 JSON 节点反序列化 Iceberg 类型定义，支持基本类型和复杂类型。
 *
 * <h3>支持的类型</h3>
 * <ul>
 *   <li><b>文本节点</b>：基本类型字符串（如 "int", "string", "decimal(10,2)"）
 *   <li><b>对象节点</b>：复杂类型，根据 "type" 字段区分：
 *     <ul>
 *       <li>"map" -> {@link IcebergMapType}
 *       <li>"list" -> {@link IcebergListType}
 *       <li>"struct" -> {@link IcebergStructType}
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h3>反序列化流程</h3>
 * <ol>
 *   <li>判断 JSON 节点类型（文本/对象）
 *   <li>文本节点：直接返回字符串
 *   <li>对象节点：解析 "type" 字段并转换为相应的类型对象
 * </ol>
 *
 * <h3>异常处理</h3>
 * <ul>
 *   <li>缺少 "type" 字段：抛出 IOException
 *   <li>未知类型：抛出 IOException
 *   <li>非预期节点类型：抛出 IOException
 * </ul>
 *
 * @see IcebergDataField
 * @see IcebergMapType
 * @see IcebergListType
 * @see IcebergStructType
 */
public class IcebergDataTypeDeserializer extends JsonDeserializer<Object> {
    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec mapper = p.getCodec();
        JsonNode node = mapper.readTree(p);

        if (node.isTextual()) {
            return node.asText();
        } else if (node.isObject()) {
            if (!node.has("type")) {
                throw new IOException(
                        "Exception occurs when deserialize iceberg data field. Missing 'type' field: "
                                + node);
            }

            String type = node.get("type").asText();
            switch (type) {
                case "map":
                    return mapper.treeToValue(node, IcebergMapType.class);
                case "list":
                    return mapper.treeToValue(node, IcebergListType.class);
                case "struct":
                    return mapper.treeToValue(node, IcebergStructType.class);
                default:
                    throw new IOException(
                            "Exception occurs when deserialize iceberg data field. Unknown iceberg field type: "
                                    + type);
            }
        } else {
            throw new IOException("Unexpected json node. node type: " + node.getNodeType());
        }
    }
}
