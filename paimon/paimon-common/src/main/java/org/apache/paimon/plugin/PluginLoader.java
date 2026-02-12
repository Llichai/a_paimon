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

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 插件加载器,用于加载外部插件 JAR 文件。
 *
 * <p>该类实现了插件的动态加载机制,支持:
 * <ul>
 *   <li>基于 SPI (Service Provider Interface) 的服务发现</li>
 *   <li>隔离的类加载环境,避免类冲突</li>
 *   <li>父优先加载策略,确保日志等核心类的一致性</li>
 *   <li>按类名动态实例化插件类</li>
 * </ul>
 *
 * <h2>类加载策略</h2>
 * <p>PluginLoader 使用 {@link ComponentClassLoader} 实现类加载隔离:
 * <table border="1">
 *   <tr>
 *     <th>类路径类别</th>
 *     <th>加载策略</th>
 *     <th>包含的包</th>
 *     <th>原因</th>
 *   </tr>
 *   <tr>
 *     <td>父优先类路径<br>(OWNER_CLASSPATH)</td>
 *     <td>Owner-First</td>
 *     <td>
 *       • 日志框架(slf4j, log4j, logback等)<br>
 *       • XML绑定(javax.xml.bind)<br>
 *       • 代码生成(janino, commons)<br>
 *       • 工具库(commons.lang3)
 *     </td>
 *     <td>
 *       确保这些核心类库<br>
 *       使用应用的统一版本,<br>
 *       避免日志输出混乱
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>组件优先类路径<br>(COMPONENT_CLASSPATH)</td>
 *     <td>Component-First</td>
 *     <td>
 *       • org.apache.paimon
 *     </td>
 *     <td>
 *       插件中的 Paimon 类<br>
 *       优先从插件加载,<br>
 *       支持版本隔离
 *     </td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 示例1: 加载文件格式插件
 * PluginLoader loader = new PluginLoader("formats/parquet");
 *
 * // 使用 SPI 自动发现插件
 * FileFormatFactory factory = loader.discover(FileFormatFactory.class);
 *
 * // 示例2: 按类名加载插件
 * PluginLoader loader2 = new PluginLoader("catalogs/hive");
 * CatalogFactory catalogFactory =
 *     loader2.newInstance("org.apache.paimon.hive.HiveCatalogFactory");
 *
 * // 示例3: 获取插件的类加载器
 * ClassLoader pluginClassLoader = loader.submoduleClassLoader();
 * Class<?> pluginClass = pluginClassLoader.loadClass("com.example.Plugin");
 * }</pre>
 *
 * <h2>SPI 服务发现</h2>
 * <p>{@link #discover(Class)} 方法使用 {@link ServiceLoader} 发现服务实现:
 * <ul>
 *   <li>在插件 JAR 的 META-INF/services/ 目录下查找服务配置文件</li>
 *   <li>文件名为接口的全限定名</li>
 *   <li>文件内容为实现类的全限定名</li>
 *   <li>要求有且仅有一个实现,否则抛出异常</li>
 * </ul>
 *
 * <h2>目录路径要求</h2>
 * <ul>
 *   <li>dirName 参数是相对于 classpath 的路径</li>
 *   <li>路径必须以 "/" 结尾,否则会自动添加</li>
 *   <li>目录应包含插件的 JAR 文件或类文件</li>
 * </ul>
 *
 * @see ComponentClassLoader
 * @see ServiceLoader
 */
public class PluginLoader {

    /**
     * 父优先加载的日志框架包前缀。
     *
     * <p>这些包必须使用应用主类加载器的版本,确保日志配置和输出的统一性。
     */
    public static final String[] PARENT_FIRST_LOGGING_PATTERNS =
            new String[] {
                "org.slf4j",
                "org.apache.log4j",
                "org.apache.logging",
                "org.apache.commons.logging",
                "ch.qos.logback"
            };

    /**
     * 父优先加载的类路径。
     *
     * <p>包含日志框架和一些核心库,这些类从应用的类加载器加载,而非从插件加载。
     */
    private static final String[] OWNER_CLASSPATH =
            Stream.concat(
                            Arrays.stream(PARENT_FIRST_LOGGING_PATTERNS),
                            Stream.of(
                                    "javax.xml.bind",
                                    "org.codehaus.janino",
                                    "org.codehaus.commons",
                                    "org.apache.commons.lang3"))
                    .toArray(String[]::new);

    /**
     * 组件优先加载的类路径。
     *
     * <p>Paimon 的类优先从插件的类加载器加载,支持插件使用不同版本的 Paimon。
     */
    private static final String[] COMPONENT_CLASSPATH = new String[] {"org.apache.paimon"};

    /** 插件的子模块类加载器。 */
    /** 插件的子模块类加载器。 */
    private final ComponentClassLoader submoduleClassLoader;

    /**
     * 创建插件加载器。
     *
     * <p>构造流程:
     * <ol>
     *   <li>规范化目录路径,确保以 "/" 结尾</li>
     *   <li>获取应用的类加载器作为父类加载器</li>
     *   <li>解析目录 URL</li>
     *   <li>创建 ComponentClassLoader,配置类加载策略</li>
     * </ol>
     *
     * @param dirName 插件目录的相对路径(相对于 classpath),例如: "formats/parquet"
     */
    public PluginLoader(String dirName) {
        // URL 必须以斜杠结尾,否则 URLClassLoader 无法将此路径识别为目录
        if (!dirName.endsWith("/")) {
            dirName += "/";
        }

        ClassLoader ownerClassLoader = PluginLoader.class.getClassLoader();
        this.submoduleClassLoader =
                new ComponentClassLoader(
                        new URL[] {ownerClassLoader.getResource(dirName)},
                        ownerClassLoader,
                        OWNER_CLASSPATH,
                        COMPONENT_CLASSPATH);
    }

    /**
     * 使用 SPI 机制发现并加载插件服务。
     *
     * <p>该方法通过 {@link ServiceLoader} 在插件的类加载器中查找指定接口的实现。
     * 要求插件 JAR 包含正确的 META-INF/services 配置文件。
     *
     * <h3>服务发现流程</h3>
     * <ol>
     *   <li>在插件的类加载器中使用 ServiceLoader 查找服务</li>
     *   <li>收集所有找到的实现</li>
     *   <li>验证找到的实现数量必须为 1</li>
     *   <li>返回唯一的实现实例</li>
     * </ol>
     *
     * <h3>META-INF/services 配置示例</h3>
     * <pre>
     * 文件路径: META-INF/services/org.apache.paimon.format.FileFormatFactory
     * 文件内容: org.apache.paimon.format.parquet.ParquetFileFormatFactory
     * </pre>
     *
     * @param clazz 要发现的服务接口类
     * @param <T> 服务接口类型
     * @return 发现的服务实例
     * @throws RuntimeException 如果找到 0 个或多个实现
     */
    public <T> T discover(Class<T> clazz) {
        List<T> results = new ArrayList<>();
        ServiceLoader.load(clazz, submoduleClassLoader).iterator().forEachRemaining(results::add);
        if (results.size() != 1) {
            throw new RuntimeException(
                    "Found "
                            + results.size()
                            + " classes implementing "
                            + clazz.getName()
                            + ". They are:\n"
                            + results.stream()
                                    .map(t -> t.getClass().getName())
                                    .collect(Collectors.joining("\n")));
        }
        return results.get(0);
    }

    /**
     * 按类名实例化插件类。
     *
     * <p>该方法直接通过类名加载并实例化插件类,不依赖 SPI 配置。
     * 要求类必须有无参构造函数。
     *
     * <h3>实例化流程</h3>
     * <ol>
     *   <li>使用插件类加载器加载指定类</li>
     *   <li>调用类的无参构造函数创建实例</li>
     *   <li>将实例转换为目标类型</li>
     * </ol>
     *
     * @param name 类的全限定名,例如: "org.apache.paimon.hive.HiveCatalogFactory"
     * @param <T> 期望的返回类型
     * @return 类的新实例
     * @throws RuntimeException 如果类不存在、无法实例化或无法访问
     */
    @SuppressWarnings("unchecked")
    public <T> T newInstance(String name) {
        try {
            return (T) submoduleClassLoader.loadClass(name).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取插件的子模块类加载器。
     *
     * <p>返回的类加载器可用于:
     * <ul>
     *   <li>手动加载插件中的类</li>
     *   <li>创建插件类的实例</li>
     *   <li>访问插件中的资源文件</li>
     * </ul>
     *
     * @return 插件的类加载器
     */
    public ClassLoader submoduleClassLoader() {
        return submoduleClassLoader;
    }
}
