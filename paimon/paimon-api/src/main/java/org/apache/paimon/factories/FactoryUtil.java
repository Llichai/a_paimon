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

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * 工厂工具类,提供使用 {@link Factory} 的便捷方法。
 *
 * <p>该工具类提供了工厂发现、查找和实例化的核心功能,基于 Java 的服务提供者接口(SPI)机制。
 * 它使用缓存优化性能,避免重复扫描类路径。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>工厂发现</b>: 根据标识符和类型发现工厂实现
 *   <li><b>标识符查询</b>: 列出特定类型的所有可用工厂标识符
 *   <li><b>单例工厂</b>: 查找和验证唯一的工厂实现
 *   <li><b>缓存机制</b>: 缓存已发现的工厂,提高性能
 * </ul>
 *
 * <h2>缓存策略</h2>
 * <p>工厂列表按类加载器缓存:
 * <ul>
 *   <li>使用软引用,允许 GC 在内存紧张时回收
 *   <li>最大缓存 100 个类加载器的工厂列表
 *   <li>同步执行,避免线程安全问题
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 发现特定类型和标识符的工厂
 * CatalogFactory catalogFactory = FactoryUtil.discoverFactory(
 *     classLoader,
 *     CatalogFactory.class,
 *     "filesystem"
 * );
 *
 * // 列出所有 CatalogFactory 的标识符
 * List<String> identifiers = FactoryUtil.discoverIdentifiers(
 *     classLoader,
 *     CatalogFactory.class
 * );
 * // 输出: ["filesystem", "hive", "jdbc", ...]
 *
 * // 查找单例工厂(确保只有一个实现)
 * Optional<MyFactory> factory = FactoryUtil.discoverSingletonFactory(
 *     classLoader,
 *     MyFactory.class
 * );
 * }</pre>
 *
 * <h2>异常处理</h2>
 * <p>工厂发现可能抛出 {@link FactoryException}:
 * <ul>
 *   <li><b>工厂未找到</b>: 没有实现指定接口的工厂
 *   <li><b>标识符不匹配</b>: 没有工厂匹配指定的标识符
 *   <li><b>多个匹配</b>: 发现多个具有相同标识符的工厂(歧义)
 * </ul>
 *
 * @see Factory
 * @see FactoryException
 * @since 1.0
 */
public class FactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    /** 工厂缓存,按类加载器存储已发现的工厂列表。使用软引用和有界缓存避免内存泄漏。 */
    private static final Cache<ClassLoader, List<Factory>> FACTORIES =
            Caffeine.newBuilder().softValues().maximumSize(100).executor(Runnable::run).build();

    /**
     * 发现并返回指定类型和标识符的工厂实现。
     *
     * <p>该方法执行以下步骤:
     * <ol>
     *   <li>从缓存或 SPI 获取所有工厂
     *   <li>过滤出实现指定工厂类的所有工厂
     *   <li>查找标识符匹配的工厂
     *   <li>验证只有一个匹配的工厂
     * </ol>
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * // 发现文件系统 catalog 工厂
     * CatalogFactory factory = FactoryUtil.discoverFactory(
     *     Thread.currentThread().getContextClassLoader(),
     *     CatalogFactory.class,
     *     "filesystem"
     * );
     * }</pre>
     *
     * @param classLoader 用于加载工厂的类加载器
     * @param factoryClass 工厂基类或接口
     * @param identifier 工厂的唯一标识符
     * @param <T> 工厂类型
     * @return 匹配的工厂实例
     * @throws FactoryException 如果未找到工厂、未找到匹配的标识符或发现多个匹配的工厂
     */
    @SuppressWarnings("unchecked")
    public static <T extends Factory> T discoverFactory(
            ClassLoader classLoader, Class<T> factoryClass, String identifier) {
        final List<Factory> factories = getFactories(classLoader);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        if (foundFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.identifier().equals(identifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            identifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(Factory::identifier)
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new FactoryException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            identifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) matchingFactories.get(0);
    }

    /**
     * 发现并返回指定类型的所有工厂标识符。
     *
     * <p>该方法查找类路径中所有实现指定工厂接口的工厂,并返回它们的标识符列表。
     * 用于列举可用的工厂选项。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * // 列出所有可用的 catalog 工厂
     * List<String> catalogs = FactoryUtil.discoverIdentifiers(
     *     classLoader,
     *     CatalogFactory.class
     * );
     * // 可能返回: ["filesystem", "hive", "jdbc", "rest"]
     *
     * // 列出所有文件系统工厂
     * List<String> fileSystems = FactoryUtil.discoverIdentifiers(
     *     classLoader,
     *     FileSystemFactory.class
     * );
     * // 可能返回: ["hdfs", "s3", "oss", "local"]
     * }</pre>
     *
     * @param classLoader 用于加载工厂的类加载器
     * @param factoryClass 工厂基类或接口
     * @param <T> 工厂类型
     * @return 标识符列表,如果没有找到返回空列表
     */
    public static <T extends Factory> List<String> discoverIdentifiers(
            ClassLoader classLoader, Class<T> factoryClass) {
        final List<Factory> factories = getFactories(classLoader);

        return factories.stream()
                .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                .map(Factory::identifier)
                .collect(Collectors.toList());
    }

    /**
     * 从缓存获取工厂列表,如果不存在则通过 SPI 发现。
     *
     * @param classLoader 类加载器
     * @return 工厂列表
     */
    private static List<Factory> getFactories(ClassLoader classLoader) {
        return FACTORIES.get(classLoader, s -> discoverFactories(classLoader, Factory.class));
    }

    /**
     * 通过 SPI 机制发现指定类型的所有工厂。
     *
     * <p>该方法使用 Java 的 {@link ServiceLoader} 从类路径中加载工厂实现。
     * 它能够处理加载过程中的各种异常,确保尽可能发现所有可用的工厂。
     *
     * <h3>错误处理</h3>
     * <ul>
     *   <li>{@link NoClassDefFoundError}: 记录调试日志但继续加载其他工厂(预期行为)
     *   <li>其他异常: 抛出 {@link RuntimeException} 并停止加载
     * </ul>
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>发现所有可用的工厂实现
     *   <li>加载自定义扩展点
     *   <li>插件系统的基础设施
     * </ul>
     *
     * @param classLoader 用于加载工厂的类加载器
     * @param klass 要发现的类或接口
     * @param <T> 类型参数
     * @return 发现的对象列表,如果没有找到返回空列表
     * @throws RuntimeException 如果在加载过程中遇到意外错误
     */
    public static <T> List<T> discoverFactories(ClassLoader classLoader, Class<T> klass) {
        final Iterator<T> serviceLoaderIterator = ServiceLoader.load(klass, classLoader).iterator();

        final List<T> loadResults = new ArrayList<>();
        while (true) {
            try {
                // error handling should also be applied to the hasNext() call because service
                // loading might cause problems here as well
                if (!serviceLoaderIterator.hasNext()) {
                    break;
                }

                loadResults.add(serviceLoaderIterator.next());
            } catch (Throwable t) {
                if (t instanceof NoClassDefFoundError) {
                    LOG.debug(
                            "NoClassDefFoundError when loading a {}. This is expected when trying to load factory but no implementation is loaded.",
                            Factory.class.getCanonicalName(),
                            t);
                } else {
                    throw new RuntimeException(
                            "Unexpected error when trying to load service provider.", t);
                }
            }
        }

        return loadResults;
    }

    /**
     * 发现单例工厂实现。
     *
     * <p>该方法用于查找唯一的工厂实现,如果发现零个或多个实现会有不同的处理:
     * <ul>
     *   <li><b>零个实现</b>: 返回 {@link Optional#empty()}
     *   <li><b>一个实现</b>: 返回包含该实现的 {@link Optional}
     *   <li><b>多个实现</b>: 抛出 {@link FactoryException}(歧义错误)
     * </ul>
     *
     * <h3>使用场景</h3>
     * <p>适用于期望只有一个实现的扩展点:
     * <ul>
     *   <li>全局配置提供者
     *   <li>唯一的插件实现
     *   <li>系统级别的单例服务
     * </ul>
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * // 查找全局配置提供者
     * Optional<GlobalConfigProvider> provider =
     *     FactoryUtil.discoverSingletonFactory(
     *         classLoader,
     *         GlobalConfigProvider.class
     *     );
     *
     * if (provider.isPresent()) {
     *     // 使用唯一的提供者
     *     Config config = provider.get().getConfig();
     * } else {
     *     // 使用默认配置
     *     Config config = DefaultConfig.create();
     * }
     * }</pre>
     *
     * @param classLoader 用于加载工厂的类加载器
     * @param klass 要发现的类或接口
     * @param <T> 类型参数
     * @return 包含工厂实例的 Optional,如果没有找到返回空 Optional
     * @throws FactoryException 如果发现多个实现(歧义错误)
     */
    public static <T> Optional<T> discoverSingletonFactory(
            ClassLoader classLoader, Class<T> klass) {
        List<T> factories = FactoryUtil.discoverFactories(classLoader, klass);
        if (factories.isEmpty()) {
            return Optional.empty();
        }

        if (factories.size() > 1) {
            throw new FactoryException(
                    String.format(
                            "Multiple factories that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            klass.getName(),
                            factories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return Optional.of(factories.get(0));
    }
}
