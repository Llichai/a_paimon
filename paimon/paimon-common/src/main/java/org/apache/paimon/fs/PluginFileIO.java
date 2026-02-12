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

package org.apache.paimon.fs;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import java.io.IOException;

/**
 * 支持插件化的 {@link FileIO} 抽象基类。
 *
 * <p>PluginFileIO 用于支持从独立 JAR 包加载的文件系统实现。由于 FileIO 需要可序列化,
 * 而插件类加载器不可序列化,因此实际的 FileIO 实例被标记为 transient,在需要时延迟创建。
 *
 * <h3>设计要点</h3>
 * <ul>
 *   <li><b>类加载器隔离</b>: 插件使用独立的类加载器,避免版本冲突</li>
 *   <li><b>延迟初始化</b>: FileIO 实例在首次使用时才创建(双检锁)</li>
 *   <li><b>可序列化支持</b>: 保存配置选项,反序列化后重建 FileIO</li>
 *   <li><b>线程安全</b>: 使用 volatile + synchronized 确保线程安全</li>
 *   <li><b>ClassLoader 切换</b>: 每次调用时切换到插件类加载器</li>
 * </ul>
 *
 * <h3>工作流程</h3>
 * <ol>
 *   <li>配置阶段: configure() 保存 Options</li>
 *   <li>首次访问: fileIO() 触发延迟初始化</li>
 *   <li>创建 FileIO: createFileIO() 由子类实现</li>
 *   <li>调用包装: wrap() 切换 ClassLoader 并执行操作</li>
 *   <li>恢复 ClassLoader: finally 块确保恢复原始 ClassLoader</li>
 * </ol>
 *
 * <h3>序列化行为</h3>
 * <ul>
 *   <li><b>序列化</b>: 只保存 options,lazyFileIO 被标记为 transient</li>
 *   <li><b>反序列化</b>: lazyFileIO 为 null,首次访问时重新创建</li>
 *   <li><b>注意事项</b>: 确保 options 包含所有必要的配置信息</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 实现插件 FileIO
 * public class MyPluginFileIO extends PluginFileIO {
 *     private final ClassLoader pluginLoader;
 *
 *     public MyPluginFileIO(ClassLoader pluginLoader) {
 *         this.pluginLoader = pluginLoader;
 *     }
 *
 *     @Override
 *     protected FileIO createFileIO(Path path) {
 *         // 使用插件类加载器创建 FileIO 实例
 *         return pluginLoader.loadClass("MyFileIOImpl")
 *                            .getDeclaredConstructor()
 *                            .newInstance();
 *     }
 *
 *     @Override
 *     protected ClassLoader pluginClassLoader() {
 *         return pluginLoader;
 *     }
 * }
 *
 * // 使用
 * PluginFileIO fileIO = new MyPluginFileIO(pluginClassLoader);
 * fileIO.configure(catalogContext);
 * // 首次调用时创建底层 FileIO
 * SeekableInputStream in = fileIO.newInputStream(path);
 * }</pre>
 *
 * <h3>ClassLoader 管理</h3>
 * <p>每个 FileIO 操作都会:
 * <ol>
 *   <li>保存当前线程的 ClassLoader</li>
 *   <li>切换到插件 ClassLoader</li>
 *   <li>执行操作</li>
 *   <li>恢复原始 ClassLoader(即使发生异常)</li>
 * </ol>
 *
 * <h3>注意事项</h3>
 * <ul>
 *   <li><b>避免配置泄露</b>: configure() 中不获取 Hadoop Configuration,
 *       因为它可能来自不同的类加载器</li>
 *   <li><b>线程安全</b>: 延迟初始化使用双检锁,确保多线程环境下只创建一次</li>
 *   <li><b>ClassLoader 污染</b>: wrap() 确保操作完成后恢复 ClassLoader</li>
 *   <li><b>插件隔离</b>: 不同插件应使用不同的 PluginFileIO 实例</li>
 * </ul>
 *
 * @see FileIO
 * @see ClassLoader
 */
public abstract class PluginFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    /** 保存的配置选项,用于序列化和反序列化后重建 FileIO。 */
    protected Options options;

    /**
     * 延迟创建的底层 FileIO 实例。
     * 标记为 transient,不参与序列化,反序列化后首次使用时重新创建。
     * 使用 volatile 确保多线程可见性。
     */
    private transient volatile FileIO lazyFileIO;

    @Override
    public void configure(CatalogContext context) {
        // Do not get Hadoop Configuration in CatalogOptions
        // The class is in different classloader from pluginClassLoader!
        this.options = context.options();
    }

    /**
     * 获取配置选项。
     *
     * @return 配置选项
     */
    public Options options() {
        return options;
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return wrap(() -> fileIO(path).newInputStream(path));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return wrap(() -> fileIO(path).newOutputStream(path, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return wrap(() -> fileIO(path).getFileStatus(path));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return wrap(() -> fileIO(path).listStatus(path));
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return wrap(() -> fileIO(path).exists(path));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return wrap(() -> fileIO(path).delete(path, recursive));
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return wrap(() -> fileIO(path).mkdirs(path));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return wrap(() -> fileIO(src).rename(src, dst));
    }

    /**
     * 获取或创建底层 FileIO 实例(双检锁延迟初始化)。
     *
     * @param path 路径(用于创建 FileIO)
     * @return 底层 FileIO 实例
     * @throws IOException 如果创建失败
     */
    private FileIO fileIO(Path path) throws IOException {
        if (lazyFileIO == null) {
            synchronized (this) {
                if (lazyFileIO == null) {
                    lazyFileIO = wrap(() -> createFileIO(path));
                }
            }
        }
        return lazyFileIO;
    }

    /**
     * 创建底层 FileIO 实例。
     * 由子类实现,使用插件类加载器加载具体的 FileIO 实现。
     *
     * @param path 路径信息
     * @return 创建的 FileIO 实例
     */
    protected abstract FileIO createFileIO(Path path);

    /**
     * 获取插件类加载器。
     * 由子类实现,返回用于加载插件类的 ClassLoader。
     *
     * @return 插件类加载器
     */
    protected abstract ClassLoader pluginClassLoader();

    /**
     * 包装函数调用,在插件类加载器上下文中执行。
     *
     * <p>该方法会:
     * <ol>
     *   <li>保存当前线程的 ClassLoader</li>
     *   <li>切换到插件 ClassLoader</li>
     *   <li>执行传入的函数</li>
     *   <li>恢复原始 ClassLoader</li>
     * </ol>
     *
     * @param func 要执行的函数
     * @param <T> 返回值类型
     * @return 函数执行结果
     * @throws IOException 如果执行过程中发生 I/O 错误
     */
    private <T> T wrap(Func<T> func) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader());
            return func.apply();
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    /**
     * 函数式接口,用于包装需要在插件类加载器上下文中执行的操作。
     *
     * @param <T> 返回值类型
     */
    @FunctionalInterface
    protected interface Func<T> {
        /**
         * 执行操作并返回结果。
         *
         * @return 操作结果
         * @throws IOException 如果操作失败
         */
        T apply() throws IOException;
    }
}
