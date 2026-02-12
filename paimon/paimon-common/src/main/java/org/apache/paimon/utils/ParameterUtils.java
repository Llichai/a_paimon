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

package org.apache.paimon.utils;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeJsonParser;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 参数工具类,用于字符串参数的格式转换。
 *
 * <p>该类提供了一系列静态方法,用于将字符串格式的参数转换为结构化的数据格式,
 * 主要用于处理命令行参数、配置文件参数等。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>分区解析</b> - 解析分区字符串为键值对映射列表
 *   <li><b>键值对解析</b> - 解析逗号分隔的键值对字符串
 *   <li><b>键值列表解析</b> - 解析键到值列表的映射
 *   <li><b>数据字段解析</b> - 从 JSON 字符串解析数据字段数组
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>分区参数</b> - 解析 "dt=2024-01-01,region=us" 格式的分区字符串
 *   <li><b>配置解析</b> - 解析命令行或配置文件中的键值对
 *   <li><b>字段定义</b> - 从 JSON 字符串解析表字段定义
 *   <li><b>多值参数</b> - 处理一个键对应多个值的情况
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 解析分区字符串
 * List<Map<String, String>> partitions = ParameterUtils.getPartitions(
 *     "dt=2024-01-01,region=us",
 *     "dt=2024-01-02,region=eu"
 * );
 * // 结果: [{"dt":"2024-01-01","region":"us"}, {"dt":"2024-01-02","region":"eu"}]
 *
 * // 解析键值对
 * Map<String, String> kvs = ParameterUtils.parseCommaSeparatedKeyValues(
 *     "key1=value1,key2=value2"
 * );
 * // 结果: {"key1":"value1", "key2":"value2"}
 *
 * // 解析数据字段数组
 * String json = "[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]";
 * List<DataField> fields = ParameterUtils.parseDataFieldArray(json);
 * }</pre>
 */
public class ParameterUtils {

    /**
     * 解析多个分区字符串。
     *
     * <p>将多个分区字符串转换为分区映射列表。每个分区字符串格式为逗号分隔的键值对。
     *
     * @param partitionStrings 分区字符串数组,每个字符串格式为 "key1=value1,key2=value2"
     * @return 分区映射列表,每个映射表示一个分区
     */
    public static List<Map<String, String>> getPartitions(String... partitionStrings) {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String partition : partitionStrings) {
            partitions.add(parseCommaSeparatedKeyValues(partition));
        }
        return partitions;
    }

    /**
     * 解析逗号分隔的键值对字符串。
     *
     * <p>将格式为 "key1=value1,key2=value2" 的字符串解析为键值对映射。
     * 键和值都会被trim处理,移除前后空格。
     *
     * @param keyValues 键值对字符串,格式为 "key1=value1,key2=value2"
     * @return 键值对映射
     * @throws IllegalArgumentException 如果键值对格式不正确
     */
    public static Map<String, String> parseCommaSeparatedKeyValues(String keyValues) {
        Map<String, String> kvs = new HashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(keyValues)) {
            for (String kvString : keyValues.split(",")) {
                parseKeyValueString(kvs, kvString);
            }
        }
        return kvs;
    }

    /**
     * 解析单个键值对字符串并添加到映射中。
     *
     * <p>解析格式为 "key=value" 的字符串,并将键值对添加到给定的映射中。
     * 键和值都会被trim处理。
     *
     * @param map 目标映射,用于存放解析的键值对
     * @param kvString 键值对字符串,格式为 "key=value"
     * @throws IllegalArgumentException 如果字符串格式不正确(不包含'='或包含多个'=')
     */
    public static void parseKeyValueString(Map<String, String> map, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        map.put(kv[0].trim(), kv[1].trim());
    }

    /**
     * 解析键到值列表的映射字符串。
     *
     * <p>解析格式为 "key=value1,value2,value3" 的字符串,将值按逗号分割为列表。
     * 适用于一个键对应多个值的场景。
     *
     * @param mapList 目标映射,用于存放解析的键到值列表的映射
     * @param kvString 键值列表字符串,格式为 "key=value1,value2,value3"
     * @throws IllegalArgumentException 如果字符串格式不正确
     */
    public static void parseKeyValueList(Map<String, List<String>> mapList, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        String[] valueArr = kv[1].trim().split(",");
        List<String> valueList = new ArrayList<>();
        for (String value : valueArr) {
            valueList.add(value);
        }
        mapList.put(kv[0].trim(), valueList);
    }

    /**
     * 从 JSON 字符串解析数据字段数组。
     *
     * <p>将 JSON 格式的字符串解析为 {@link DataField} 列表。
     * JSON 字符串应该是一个数组,每个元素包含字段的定义信息。
     *
     * @param data JSON 字符串,格式为 JSON 数组
     * @return 数据字段列表,如果输入为 null 或不是数组则返回空列表
     */
    public static List<DataField> parseDataFieldArray(String data) {
        List<DataField> list = new ArrayList<>();
        if (data != null) {
            JsonNode jsonArray = JsonSerdeUtil.fromJson(data, JsonNode.class);
            if (jsonArray.isArray()) {
                for (JsonNode objNode : jsonArray) {
                    DataField dataField = DataTypeJsonParser.parseDataField(objNode);
                    list.add(dataField);
                }
            }
        }
        return list;
    }
}
