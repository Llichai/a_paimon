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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 回调加载工具类。
 *
 * <p>负责从配置中加载和初始化回调实例，包括：
 * <ul>
 *   <li>{@link TagCallback}：标签操作回调
 *   <li>{@link CommitCallback}：提交操作回调
 * </ul>
 *
 * <p>主要功能：
 * <ul>
 *   <li><b>动态加载</b>：通过反射加载配置的回调类
 *   <li><b>参数注入</b>：支持带参数和无参数的构造函数
 *   <li><b>类型验证</b>：确保加载的类实现了正确的接口
 *   <li><b>批量处理</b>：支持加载多个回调实例
 * </ul>
 *
 * <p>配置格式：
 * <pre>
 * CREATE TABLE t (...) WITH (
 *   -- 标签回调配置
 *   'tag.callbacks' = 'com.example.TagCallback1:param1,com.example.TagCallback2',
 *
 *   -- 提交回调配置
 *   'commit.callbacks' = 'com.example.CommitCallback1:param1,com.example.CommitCallback2:param2'
 * );
 * </pre>
 *
 * <p>回调类要求：
 * <ul>
 *   <li>必须实现 {@link TagCallback} 或 {@link CommitCallback} 接口
 *   <li>支持两种构造函数：
 *       <ul>
 *         <li>无参构造函数：{@code public MyCallback() {...}}
 *         <li>单字符串参数构造函数：{@code public MyCallback(String param) {...}}
 *       </ul>
 * </ul>
 *
 * <p>使用示例：
 * <pre>
 * // 加载标签回调
 * List<TagCallback> tagCallbacks =
 *     CallbackUtils.loadTagCallbacks(coreOptions);
 *
 * // 加载提交回调
 * List<CommitCallback> commitCallbacks =
 *     CallbackUtils.loadCommitCallbacks(coreOptions, table);
 * </pre>
 *
 * @see TagCallback 标签回调接口
 * @see CommitCallback 提交回调接口
 * @see CoreOptions 配置选项
 */
public class CallbackUtils {

    /**
     * 加载标签回调列表。
     *
     * <p>从配置中加载所有标签回调实例。
     *
     * @param coreOptions 核心配置选项
     * @return 标签回调列表，如果未配置则返回空列表
     */
    public static List<TagCallback> loadTagCallbacks(CoreOptions coreOptions) {
        return loadCallbacks(coreOptions.tagCallbacks(), TagCallback.class);
    }

    /**
     * 加载提交回调列表。
     *
     * <p>从配置中加载所有提交回调实例，并为每个回调设置表对象。
     *
     * @param coreOptions 核心配置选项
     * @param table 文件存储表对象，会传递给每个回调的 setTable() 方法
     * @return 提交回调列表，如果未配置则返回空列表
     */
    public static List<CommitCallback> loadCommitCallbacks(
            CoreOptions coreOptions, FileStoreTable table) {
        // 加载回调
        List<CommitCallback> commitCallbacks =
                loadCallbacks(coreOptions.commitCallbacks(), CommitCallback.class);

        // 为每个回调设置表对象
        commitCallbacks.forEach(callback -> callback.setTable(table));

        return commitCallbacks;
    }

    /**
     * 加载回调实例列表（通用方法）。
     *
     * <p>通过反射动态加载和实例化回调类。
     *
     * <p>处理流程：
     * <ol>
     *   <li>遍历配置的类名和参数映射
     *   <li>加载类并验证类型
     *   <li>根据是否有参数选择构造函数
     *   <li>实例化并添加到结果列表
     * </ol>
     *
     * @param clazzParamMaps 类名到参数的映射，格式：{类名 -> 参数字符串}
     * @param expectClass 期望的接口类型
     * @param <T> 回调类型
     * @return 回调实例列表
     * @throws RuntimeException 如果类加载失败
     * @throws IllegalArgumentException 如果类型不匹配
     */
    @SuppressWarnings("unchecked")
    private static <T> List<T> loadCallbacks(
            Map<String, String> clazzParamMaps, Class<T> expectClass) {
        List<T> result = new ArrayList<>();

        // 遍历所有配置的回调
        for (Map.Entry<String, String> classParamEntry : clazzParamMaps.entrySet()) {
            String className = classParamEntry.getKey();
            String param = classParamEntry.getValue();

            // 加载类
            Class<?> clazz;
            try {
                clazz = Class.forName(className, true, CallbackUtils.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            // 验证类型：确保实现了期望的接口
            Preconditions.checkArgument(
                    expectClass.isAssignableFrom(clazz),
                    "Class " + clazz + " must implement " + expectClass);

            // 实例化
            try {
                if (param == null) {
                    // 无参构造函数
                    result.add((T) clazz.newInstance());
                } else {
                    // 带字符串参数的构造函数
                    result.add((T) clazz.getConstructor(String.class).newInstance(param));
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to initialize commit callback "
                                + className
                                + (param == null ? "" : " with param " + param),
                        e);
            }
        }

        return result;
    }
}
