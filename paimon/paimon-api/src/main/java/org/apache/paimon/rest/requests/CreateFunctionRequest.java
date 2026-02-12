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

package org.apache.paimon.rest.requests;

import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionDefinition;
import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.types.DataField;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * 创建函数请求。
 *
 * <p>用于向 REST 服务器发送创建用户自定义函数(UDF)的请求,包含函数的完整定义信息。
 *
 * <p>函数可以有多个方言定义(如 SQL、Python 等),支持输入输出参数定义。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "name": "my_function",
 *   "inputParams": [
 *     {"id": 0, "name": "x", "type": "INT"}
 *   ],
 *   "returnParams": [
 *     {"id": 0, "name": "result", "type": "INT"}
 *   ],
 *   "deterministic": true,
 *   "definitions": {
 *     "SQL": {
 *       "content": "SELECT x + 1",
 *       "language": "SQL"
 *     }
 *   },
 *   "comment": "Increment function",
 *   "options": {}
 * }
 * }</pre>
 *
 * <p>示例: 创建简单的 SQL 函数
 *
 * <pre>{@code
 * List<DataField> inputs = Arrays.asList(
 *     new DataField(0, "x", DataTypes.INT())
 * );
 * List<DataField> outputs = Arrays.asList(
 *     new DataField(0, "result", DataTypes.INT())
 * );
 * Map<String, FunctionDefinition> defs = Map.of(
 *     "SQL", new FunctionDefinition("SELECT x + 1", "SQL")
 * );
 * CreateFunctionRequest request = new CreateFunctionRequest(
 *     "increment", inputs, outputs, true, defs, "Add 1 to input", Map.of()
 * );
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateFunctionRequest implements RESTRequest {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_INPUT_PARAMETERS = "inputParams";
    private static final String FIELD_RETURN_PARAMETERS = "returnParams";
    private static final String FIELD_DEFINITIONS = "definitions";
    private static final String FIELD_DETERMINISTIC = "deterministic";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_OPTIONS = "options";

    /** 函数名称。 */
    @JsonProperty(FIELD_NAME)
    private final String functionName;

    /** 输入参数列表。 */
    @JsonProperty(FIELD_INPUT_PARAMETERS)
    private final List<DataField> inputParams;

    /** 返回值参数列表。 */
    @JsonProperty(FIELD_RETURN_PARAMETERS)
    private final List<DataField> returnParams;

    /** 是否为确定性函数(相同输入总是返回相同输出)。 */
    @JsonProperty(FIELD_DETERMINISTIC)
    private final boolean deterministic;

    /** 函数定义映射,键为方言名称(如 SQL、Python),值为函数定义。 */
    @JsonProperty(FIELD_DEFINITIONS)
    private final Map<String, FunctionDefinition> definitions;

    /** 函数注释。 */
    @JsonProperty(FIELD_COMMENT)
    private final String comment;

    /** 函数配置选项。 */
    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    /**
     * 构造函数。
     *
     * @param functionName 函数名称
     * @param inputParams 输入参数列表
     * @param returnParams 返回值参数列表
     * @param deterministic 是否为确定性函数
     * @param definitions 函数定义映射
     * @param comment 函数注释
     * @param options 函数配置选项
     */
    @JsonCreator
    public CreateFunctionRequest(
            @JsonProperty(FIELD_NAME) String functionName,
            @JsonProperty(FIELD_INPUT_PARAMETERS) List<DataField> inputParams,
            @JsonProperty(FIELD_RETURN_PARAMETERS) List<DataField> returnParams,
            @JsonProperty(FIELD_DETERMINISTIC) boolean deterministic,
            @JsonProperty(FIELD_DEFINITIONS) Map<String, FunctionDefinition> definitions,
            @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options) {
        this.functionName = functionName;
        this.inputParams = inputParams;
        this.returnParams = returnParams;
        this.deterministic = deterministic;
        this.definitions = definitions;
        this.comment = comment;
        this.options = options;
    }

    /**
     * 从 Function 对象构造请求。
     *
     * @param function 函数对象
     */
    public CreateFunctionRequest(Function function) {
        this.functionName = function.name();
        this.inputParams = function.inputParams().orElse(null);
        this.returnParams = function.returnParams().orElse(null);
        this.deterministic = function.isDeterministic();
        this.definitions = function.definitions();
        this.comment = function.comment();
        this.options = function.options();
    }

    /**
     * 获取函数名称。
     *
     * @return 函数名称
     */
    @JsonGetter(FIELD_NAME)
    public String name() {
        return functionName;
    }

    /**
     * 获取输入参数列表。
     *
     * @return 输入参数列表
     */
    @JsonGetter(FIELD_INPUT_PARAMETERS)
    public List<DataField> inputParams() {
        return inputParams;
    }

    /**
     * 获取返回值参数列表。
     *
     * @return 返回值参数列表
     */
    @JsonGetter(FIELD_RETURN_PARAMETERS)
    public List<DataField> returnParams() {
        return returnParams;
    }

    /**
     * 判断是否为确定性函数。
     *
     * @return true 如果是确定性函数
     */
    @JsonGetter(FIELD_DETERMINISTIC)
    public boolean isDeterministic() {
        return deterministic;
    }

    /**
     * 获取函数定义映射。
     *
     * @return 函数定义映射
     */
    @JsonGetter(FIELD_DEFINITIONS)
    public Map<String, FunctionDefinition> definitions() {
        return definitions;
    }

    /**
     * 获取指定方言的函数定义。
     *
     * @param dialect 方言名称(如 SQL、Python)
     * @return 函数定义
     */
    public FunctionDefinition definition(String dialect) {
        return definitions.get(dialect);
    }

    /**
     * 获取函数注释。
     *
     * @return 函数注释
     */
    @JsonGetter(FIELD_COMMENT)
    public String comment() {
        return comment;
    }

    /**
     * 获取函数配置选项。
     *
     * @return 配置选项映射
     */
    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }
}
