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

package org.apache.paimon.plugin;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.utils.FunctionWithException;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * 组件类加载器,实现类加载的隔离和优先级控制。
 *
 * <p>该类加载器继承自 {@link URLClassLoader},用于限制只能加载指定类路径中的类,
 * 但某些包可以配置为从父类加载器或组件类加载器优先加载。
 *
 * <h2>类加载器层次结构</h2>
 * <pre>
 *       Owner     Bootstrap
 *           ^         ^
 *           |---------|
 *                |
 *            Component
 * </pre>
 *
 * <p>说明:
 * <ul>
 *   <li><b>Owner</b>: 应用的主类加载器</li>
 *   <li><b>Bootstrap</b>: Java 平台类加载器(Java 9+)或 Bootstrap 类加载器(Java 8)</li>
 *   <li><b>Component</b>: 该组件类加载器,用于加载插件类</li>
 * </ul>
 *
 * <h2>类加载策略</h2>
 * <p>根据类的包名,类加载器按以下三种策略之一加载类:
 *
 * <table border="1">
 *   <tr>
 *     <th>策略</th>
 *     <th>加载顺序</th>
 *     <th>适用场景</th>
 *     <th>配置方式</th>
 *   </tr>
 *   <tr>
 *     <td>Component-Only<br>(组件独占)</td>
 *     <td>Component → Bootstrap</td>
 *     <td>
 *       默认策略,确保插件类<br>
 *       完全隔离
 *     </td>
 *     <td>
 *       不在 ownerFirstPackages<br>
 *       和 componentFirstPackages 中
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>Component-First<br>(组件优先)</td>
 *     <td>Component → Bootstrap → Owner</td>
 *     <td>
 *       插件类优先,但允许<br>
 *       回退到主应用版本
 *     </td>
 *     <td>
 *       在 componentFirstPackages 中<br>
 *       (如 org.apache.paimon)
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>Owner-First<br>(父优先)</td>
 *     <td>Owner → Component → Bootstrap</td>
 *     <td>
 *       确保核心库(如日志框架)<br>
 *       版本统一
 *     </td>
 *     <td>
 *       在 ownerFirstPackages 中<br>
 *       (如 org.slf4j, org.apache.log4j)
 *     </td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 定义父优先包(日志框架)
 * String[] ownerFirst = {
 *     "org.slf4j",
 *     "org.apache.log4j",
 *     "ch.qos.logback"
 * };
 *
 * // 定义组件优先包(Paimon 核心)
 * String[] componentFirst = {
 *     "org.apache.paimon"
 * };
 *
 * // 创建组件类加载器
 * URL[] pluginUrls = {new URL("file:///path/to/plugin.jar")};
 * ClassLoader parent = getClass().getClassLoader();
 * ComponentClassLoader loader = new ComponentClassLoader(
 *     pluginUrls,
 *     parent,
 *     ownerFirst,
 *     componentFirst
 * );
 *
 * // 加载类
 * Class<?> pluginClass = loader.loadClass("com.example.Plugin");
 *
 * // 加载资源
 * URL resource = loader.getResource("config.properties");
 * }</pre>
 *
 * <h2>资源加载</h2>
 * <p>资源的加载策略与类加载策略相同,根据资源路径前缀决定:
 * <ul>
 *   <li>owner-first 包的资源从父类加载器优先加载</li>
 *   <li>component-first 包的资源从组件类加载器优先加载</li>
 *   <li>其他资源仅从组件类加载器加载</li>
 * </ul>
 *
 * <h2>线程安全</h2>
 * <p>该类加载器注册为并行加载器(Parallel-Capable),支持并发加载。
 * 通过 {@code getClassLoadingLock()} 方法实现细粒度的同步。
 *
 * <h2>Java 版本兼容性</h2>
 * <ul>
 *   <li><b>Java 9+</b>: 使用 Platform ClassLoader 作为 Bootstrap</li>
 *   <li><b>Java 8</b>: 使用 null(表示 Bootstrap ClassLoader)作为 Bootstrap</li>
 * </ul>
 *
 * @see URLClassLoader
 * @see PluginLoader
 */
public class ComponentClassLoader extends URLClassLoader {
    /** 平台类加载器(Java 9+)或 Bootstrap 类加载器(Java 8)。 */
    private static final ClassLoader PLATFORM_OR_BOOTSTRAP_LOADER;

    /** 父类加载器(应用的主类加载器)。 */
    private final ClassLoader ownerClassLoader;

    /** 需要从父类加载器优先加载的包前缀数组。 */
    private final String[] ownerFirstPackages;

    /** 需要从组件类加载器优先加载的包前缀数组。 */
    private final String[] componentFirstPackages;

    /** 需要从父类加载器优先加载的资源路径前缀数组。 */
    private final String[] ownerFirstResourcePrefixes;

    /** 需要从组件类加载器优先加载的资源路径前缀数组。 */
    /** 需要从组件类加载器优先加载的资源路径前缀数组。 */
    private final String[] componentFirstResourcePrefixes;

    /**
     * 创建组件类加载器。
     *
     * @param classpath 组件的类路径 URL 数组
     * @param ownerClassLoader 父类加载器(应用的主类加载器)
     * @param ownerFirstPackages 需要从父类加载器优先加载的包前缀数组
     * @param componentFirstPackages 需要从组件类加载器优先加载的包前缀数组
     */
    public ComponentClassLoader(
            URL[] classpath,
            ClassLoader ownerClassLoader,
            String[] ownerFirstPackages,
            String[] componentFirstPackages) {
        super(classpath, PLATFORM_OR_BOOTSTRAP_LOADER);
        this.ownerClassLoader = ownerClassLoader;

        this.ownerFirstPackages = ownerFirstPackages;
        this.componentFirstPackages = componentFirstPackages;

        // 将包前缀转换为资源路径前缀(将 "." 替换为 "/")
        ownerFirstResourcePrefixes = convertPackagePrefixesToPathPrefixes(ownerFirstPackages);
        componentFirstResourcePrefixes =
                convertPackagePrefixesToPathPrefixes(componentFirstPackages);
    }

    // ----------------------------------------------------------------------------------------------
    // 类加载
    // ----------------------------------------------------------------------------------------------

    /**
     * 加载指定名称的类。
     *
     * <p>该方法重写了 {@link ClassLoader#loadClass(String, boolean)},
     * 实现自定义的类加载策略。
     *
     * <h3>加载流程</h3>
     * <ol>
     *   <li>同步获取类加载锁(支持并行加载)</li>
     *   <li>检查类是否已加载,如果已加载直接返回</li>
     *   <li>根据类名判断加载策略:
     *     <ul>
     *       <li>Component-First: 组件优先</li>
     *       <li>Owner-First: 父优先</li>
     *       <li>Component-Only: 组件独占(默认)</li>
     *     </ul>
     *   </li>
     *   <li>按策略执行类加载</li>
     *   <li>如果需要,解析类(resolve)</li>
     * </ol>
     *
     * @param name 类的全限定名
     * @param resolve 是否解析类
     * @return 加载的类对象
     * @throws ClassNotFoundException 如果找不到类
     */
    @Override
    protected Class<?> loadClass(final String name, final boolean resolve)
            throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            final Class<?> loadedClass = findLoadedClass(name);
            if (loadedClass != null) {
                return resolveIfNeeded(resolve, loadedClass);
            }

            if (isComponentFirstClass(name)) {
                return loadClassFromComponentFirst(name, resolve);
            }
            if (isOwnerFirstClass(name)) {
                return loadClassFromOwnerFirst(name, resolve);
            }

            // 默认策略: component-only
            // 使这个行为可配置(component-only/component-first/owner-first)
            // 将允许这个类取代 FlinkUserCodeClassLoader(需要添加异常处理器)
            return loadClassFromComponentOnly(name, resolve);
        }
    }

    /**
     * 如果需要,解析类。
     *
     * @param resolve 是否需要解析
     * @param loadedClass 已加载的类
     * @return 类对象
     */
    private Class<?> resolveIfNeeded(final boolean resolve, final Class<?> loadedClass) {
        if (resolve) {
            resolveClass(loadedClass);
        }
        return loadedClass;
    }

    /**
     * 检查类是否需要父优先加载。
     *
     * @param name 类的全限定名
     * @return 如果需要父优先加载返回 true
     */
    private boolean isOwnerFirstClass(final String name) {
        return Arrays.stream(ownerFirstPackages).anyMatch(name::startsWith);
    }

    /**
     * 检查类是否需要组件优先加载。
     *
     * @param name 类的全限定名
     * @return 如果需要组件优先加载返回 true
     */
    private boolean isComponentFirstClass(final String name) {
        return Arrays.stream(componentFirstPackages).anyMatch(name::startsWith);
    }

    /**
     * 组件独占模式加载类。
     *
     * <p>加载顺序: Component → Bootstrap
     *
     * @param name 类的全限定名
     * @param resolve 是否解析类
     * @return 加载的类对象
     * @throws ClassNotFoundException 如果找不到类
     */
    private Class<?> loadClassFromComponentOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }

    /**
     * 组件优先模式加载类。
     *
     * <p>加载顺序: Component → Bootstrap → Owner
     *
     * <p>先尝试从组件加载,如果失败再从父类加载器加载。
     *
     * @param name 类的全限定名
     * @param resolve 是否解析类
     * @return 加载的类对象
     * @throws ClassNotFoundException 如果找不到类
     */
    private Class<?> loadClassFromComponentFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromComponentOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromOwnerOnly(name, resolve);
        }
    }

    /**
     * 仅从父类加载器加载类。
     *
     * @param name 类的全限定名
     * @param resolve 是否解析类
     * @return 加载的类对象
     * @throws ClassNotFoundException 如果找不到类
     */
    private Class<?> loadClassFromOwnerOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return resolveIfNeeded(resolve, ownerClassLoader.loadClass(name));
    }

    /**
     * 父优先模式加载类。
     *
     * <p>加载顺序: Owner → Component → Bootstrap
     *
     * <p>先尝试从父类加载器加载,如果失败再从组件加载。
     *
     * @param name 类的全限定名
     * @param resolve 是否解析类
     * @return 加载的类对象
     * @throws ClassNotFoundException 如果找不到类
     */
    private Class<?> loadClassFromOwnerFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromOwnerOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromComponentOnly(name, resolve);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // 资源加载
    // ----------------------------------------------------------------------------------------------

    /**
     * 获取指定名称的资源 URL。
     *
     * <p>返回第一个找到的资源,加载策略与类加载相同。
     *
     * @param name 资源名称
     * @return 资源的 URL,如果找不到返回 null
     */
    @Override
    public URL getResource(final String name) {
        try {
            final Enumeration<URL> resources = getResources(name);
            if (resources.hasMoreElements()) {
                return resources.nextElement();
            }
        } catch (IOException ignored) {
            // 模拟 JDK 的行为,忽略 IOException
        }
        return null;
    }

    /**
     * 获取指定名称的所有资源 URL。
     *
     * <p>根据资源路径前缀决定加载策略:
     * <ul>
     *   <li>Component-First: 组件优先加载资源</li>
     *   <li>Owner-First: 父优先加载资源</li>
     *   <li>Component-Only: 仅从组件加载资源(默认)</li>
     * </ul>
     *
     * @param name 资源名称
     * @return 资源 URL 的枚举
     * @throws IOException 如果加载资源失败
     */
    @Override
    public Enumeration<URL> getResources(final String name) throws IOException {
        if (isComponentFirstResource(name)) {
            return loadResourceFromComponentFirst(name);
        }
        if (isOwnerFirstResource(name)) {
            return loadResourceFromOwnerFirst(name);
        }

        return loadResourceFromComponentOnly(name);
    }

    /**
     * 检查资源是否需要父优先加载。
     *
     * @param name 资源路径
     * @return 如果需要父优先加载返回 true
     */
    private boolean isOwnerFirstResource(final String name) {
        return Arrays.stream(ownerFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    /**
     * 检查资源是否需要组件优先加载。
     *
     * @param name 资源路径
     * @return 如果需要组件优先加载返回 true
     */
    private boolean isComponentFirstResource(final String name) {
        return Arrays.stream(componentFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    /**
     * 组件独占模式加载资源。
     *
     * @param name 资源名称
     * @return 资源 URL 的枚举
     * @throws IOException 如果加载资源失败
     */
    private Enumeration<URL> loadResourceFromComponentOnly(final String name) throws IOException {
        return super.getResources(name);
    }

    /**
     * 组件优先模式加载资源。
     *
     * <p>先从组件加载,再从父类加载器加载,合并结果。
     *
     * @param name 资源名称
     * @return 资源 URL 的枚举
     * @throws IOException 如果加载资源失败
     */
    private Enumeration<URL> loadResourceFromComponentFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name, this::loadResourceFromComponentOnly, this::loadResourceFromOwnerOnly);
    }

    /**
     * 仅从父类加载器加载资源。
     *
     * @param name 资源名称
     * @return 资源 URL 的枚举
     * @throws IOException 如果加载资源失败
     */
    private Enumeration<URL> loadResourceFromOwnerOnly(final String name) throws IOException {
        return ownerClassLoader.getResources(name);
    }

    /**
     * 父优先模式加载资源。
     *
     * <p>先从父类加载器加载,再从组件加载,合并结果。
     *
     * @param name 资源名称
     * @return 资源 URL 的枚举
     * @throws IOException 如果加载资源失败
     */
    private Enumeration<URL> loadResourceFromOwnerFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name, this::loadResourceFromOwnerOnly, this::loadResourceFromComponentOnly);
    }

    /** 资源加载函数接口。 */
    private interface ResourceLoadingFunction
            extends FunctionWithException<String, Enumeration<URL>, IOException> {}

    /**
     * 按顺序从两个类加载器加载资源,合并结果。
     *
     * @param name 资源名称
     * @param firstClassLoader 第一个类加载器的资源加载函数
     * @param secondClassLoader 第二个类加载器的资源加载函数
     * @return 合并后的资源 URL 枚举
     * @throws IOException 如果加载资源失败
     */
    private Enumeration<URL> loadResourcesInOrder(
            String name,
            ResourceLoadingFunction firstClassLoader,
            ResourceLoadingFunction secondClassLoader)
            throws IOException {
        final Iterator<URL> iterator =
                Iterators.concat(
                        Iterators.forEnumeration(firstClassLoader.apply(name)),
                        Iterators.forEnumeration(secondClassLoader.apply(name)));

        return new IteratorBackedEnumeration<>(iterator);
    }

    /**
     * 基于迭代器的枚举实现。
     *
     * <p>用于将迭代器适配为枚举,方便合并多个类加载器的资源加载结果。
     *
     * @param <T> 元素类型
     */
    @VisibleForTesting
    static class IteratorBackedEnumeration<T> implements Enumeration<T> {
        /** 底层迭代器。 */
        private final Iterator<T> backingIterator;

        /**
         * 创建基于迭代器的枚举。
         *
         * @param backingIterator 底层迭代器
         */
        public IteratorBackedEnumeration(Iterator<T> backingIterator) {
            this.backingIterator = backingIterator;
        }

        @Override
        public boolean hasMoreElements() {
            return backingIterator.hasNext();
        }

        @Override
        public T nextElement() {
            return backingIterator.next();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // 工具方法
    // ----------------------------------------------------------------------------------------------

    /**
     * 将包前缀数组转换为路径前缀数组。
     *
     * <p>将包名中的点(.)替换为斜杠(/),用于资源路径匹配。
     * 例如: "org.apache.paimon" → "org/apache/paimon"
     *
     * @param packagePrefixes 包前缀数组
     * @return 路径前缀数组
     */
    private static String[] convertPackagePrefixesToPathPrefixes(String[] packagePrefixes) {
        return Arrays.stream(packagePrefixes)
                .map(packageName -> packageName.replace('.', '/'))
                .toArray(String[]::new);
    }

    /**
     * 静态初始化块,初始化平台类加载器。
     *
     * <p>根据 Java 版本选择合适的 Bootstrap 类加载器:
     * <ul>
     *   <li><b>Java 9+</b>: 使用 Platform ClassLoader</li>
     *   <li><b>Java 8</b>: 使用 null(表示 Bootstrap ClassLoader)</li>
     * </ul>
     *
     * <p>同时注册该类为并行加载器,支持并发类加载。
     */
    static {
        ClassLoader platformLoader = null;
        try {
            // Java 9+ 的 getPlatformClassLoader 方法
            platformLoader =
                    (ClassLoader)
                            ClassLoader.class.getMethod("getPlatformClassLoader").invoke(null);
        } catch (NoSuchMethodException e) {
            // Java 8 上此方法不存在,使用 null 表示 Bootstrap 加载器
        } catch (Exception e) {
            throw new IllegalStateException("Cannot retrieve platform classloader on Java 9+", e);
        }
        PLATFORM_OR_BOOTSTRAP_LOADER = platformLoader;
        // 注册为并行加载器
        ClassLoader.registerAsParallelCapable();
    }
}
