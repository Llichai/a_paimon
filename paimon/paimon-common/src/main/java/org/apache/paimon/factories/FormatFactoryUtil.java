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

package org.apache.paimon.factories;

import org.apache.paimon.format.FileFormatFactory;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.factories.FactoryUtil.discoverFactories;

/**
 * 文件格式工厂工具类。
 *
 * <p>该工具类提供了发现和获取文件格式工厂的功能，使用服务发现机制（SPI）来查找可用的格式实现。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>工厂发现</b>：自动发现 classpath 中的文件格式工厂</li>
 *   <li><b>结果缓存</b>：使用 Caffeine 缓存已发现的工厂，提高性能</li>
 *   <li><b>标识符匹配</b>：根据格式标识符（如 "orc", "parquet"）查找对应工厂</li>
 *   <li><b>错误提示</b>：当找不到工厂时，列出所有可用的格式标识符</li>
 * </ul>
 *
 * <h2>支持的格式</h2>
 * <p>常见的文件格式包括：
 * <ul>
 *   <li>ORC - 列式存储格式</li>
 *   <li>Parquet - Apache Parquet 格式</li>
 *   <li>Avro - Apache Avro 格式</li>
 *   <li>其他通过插件加载的自定义格式</li>
 * </ul>
 *
 * <h2>缓存策略</h2>
 * <ul>
 *   <li>最大缓存 100 个 ClassLoader</li>
 *   <li>使用软引用，允许在内存不足时回收</li>
 *   <li>同步执行缓存操作，避免异步线程池开销</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 获取 ORC 格式工厂
 * FileFormatFactory orcFactory = FormatFactoryUtil.discoverFactory(
 *     Thread.currentThread().getContextClassLoader(),
 *     "orc"
 * );
 *
 * // 获取 Parquet 格式工厂
 * FileFormatFactory parquetFactory = FormatFactoryUtil.discoverFactory(
 *     classLoader,
 *     "parquet"
 * );
 *
 * // 处理未找到工厂的情况
 * try {
 *     FileFormatFactory factory = FormatFactoryUtil.discoverFactory(
 *         classLoader, "unknown-format");
 * } catch (FactoryException e) {
 *     // 异常消息中包含所有可用的格式标识符
 *     System.err.println(e.getMessage());
 * }
 * }</pre>
 *
 * @see FileFormatFactory
 * @see FactoryUtil
 */
public class FormatFactoryUtil {

    private static final Cache<ClassLoader, List<FileFormatFactory>> FACTORIES =
            Caffeine.newBuilder().softValues().maximumSize(100).executor(Runnable::run).build();

    /**
     * 发现指定标识符的文件格式工厂。
     *
     * <p>该方法使用服务发现机制（SPI）查找并返回与给定标识符匹配的文件格式工厂。
     *
     * @param classLoader 用于加载工厂的类加载器
     * @param identifier 格式标识符（如 "orc", "parquet", "avro"）
     * @param <T> 工厂类型
     * @return 匹配的文件格式工厂
     * @throws FactoryException 如果找不到匹配的工厂，异常消息中会列出所有可用的格式标识符
     */
    @SuppressWarnings("unchecked")
    public static <T extends FileFormatFactory> T discoverFactory(
            ClassLoader classLoader, String identifier) {
        final List<FileFormatFactory> foundFactories = getFactories(classLoader);

        final List<FileFormatFactory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.identifier().equals(identifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements FileFormatFactory in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            identifier,
                            foundFactories.stream()
                                    .map(FileFormatFactory::identifier)
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) matchingFactories.get(0);
    }

    private static List<FileFormatFactory> getFactories(ClassLoader classLoader) {
        return FACTORIES.get(
                classLoader, s -> discoverFactories(classLoader, FileFormatFactory.class));
    }
}
