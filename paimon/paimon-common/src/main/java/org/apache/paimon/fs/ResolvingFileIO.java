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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;

/**
 * 支持多文件系统 scheme 的 {@link FileIO} 实现。
 *
 * <p>ResolvingFileIO 根据路径的 URI scheme 动态选择合适的 FileIO 实现,
 * 使得单个 Paimon 实例可以同时访问多种文件系统。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>动态路由</b>: 根据路径的 scheme 和 authority 路由到对应的 FileIO</li>
 *   <li><b>实例缓存</b>: FileIO 实例按 (scheme, authority) 缓存,避免重复创建</li>
 *   <li><b>类加载器隔离</b>: 使用 ResolvingFileIO 的类加载器执行操作</li>
 *   <li><b>线程安全</b>: 使用 ConcurrentHashMap 确保并发安全</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>混合存储架构: 元数据在 HDFS,数据在 S3</li>
 *   <li>跨云迁移: 从一个云平台迁移到另一个云平台</li>
 *   <li>多租户环境: 不同租户使用不同的存储后端</li>
 *   <li>测试环境: 本地文件系统 + 远程对象存储</li>
 * </ul>
 *
 * <h3>工作原理</h3>
 * <ol>
 *   <li>接收文件操作请求(如 newInputStream)</li>
 *   <li>从路径中提取 scheme 和 authority</li>
 *   <li>查找或创建对应的 FileIO 实例</li>
 *   <li>委托给具体的 FileIO 执行操作</li>
 *   <li>返回操作结果</li>
 * </ol>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 配置 ResolvingFileIO
 * ResolvingFileIO fileIO = new ResolvingFileIO();
 * fileIO.configure(catalogContext);
 *
 * // 自动路由到本地文件系统
 * InputStream localIn = fileIO.newInputStream(
 *     new Path("file:///tmp/data.parquet"));
 *
 * // 自动路由到 HDFS
 * InputStream hdfsIn = fileIO.newInputStream(
 *     new Path("hdfs://namenode:9000/warehouse/table/file.parquet"));
 *
 * // 自动路由到 S3
 * InputStream s3In = fileIO.newInputStream(
 *     new Path("s3://bucket/data/file.parquet"));
 * }</pre>
 *
 * <h3>缓存策略</h3>
 * <p>FileIO 实例按 {@code (scheme, authority)} 组合缓存:
 * <ul>
 *   <li>{@code file://} → 单例 LocalFileIO</li>
 *   <li>{@code hdfs://namenode1:9000} → HadoopFileIO 实例 1</li>
 *   <li>{@code hdfs://namenode2:9000} → HadoopFileIO 实例 2</li>
 *   <li>{@code s3://bucket1} → S3FileIO 实例 1</li>
 *   <li>{@code s3://bucket2} → S3FileIO 实例 2</li>
 * </ul>
 *
 * <h3>对象存储检测</h3>
 * <p>通过 warehouse 路径的 scheme 判断是否为对象存储:
 * <ul>
 *   <li><b>非对象存储</b>: file, hdfs</li>
 *   <li><b>对象存储</b>: s3, oss, gcs, azure 等</li>
 * </ul>
 *
 * <h3>配置说明</h3>
 * <p>ResolvingFileIO 会禁用递归解析,避免无限循环:
 * <pre>
 * resolving-file-io.enabled = false
 * </pre>
 *
 * <h3>注意事项</h3>
 * <ul>
 *   <li><b>路径格式</b>: 确保路径包含完整的 scheme 和 authority</li>
 *   <li><b>权限配置</b>: 每种文件系统的认证配置需要分别设置</li>
 *   <li><b>性能考虑</b>: 首次访问新 scheme 会创建 FileIO,有初始化开销</li>
 *   <li><b>资源管理</b>: 缓存的 FileIO 实例不会自动关闭,需注意资源泄漏</li>
 * </ul>
 *
 * @see FileIO
 * @see FileIOLoader
 * @see CatalogOptions#RESOLVING_FILE_IO_ENABLED
 */
public class ResolvingFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    /**
     * FileIO 实例缓存,按 (scheme, authority) 组合缓存。
     * 使用 ConcurrentHashMap 确保线程安全。
     */
    private final Map<CacheKey, FileIO> fileIOMap = new ConcurrentHashMap<>();

    /** 目录上下文,用于创建 FileIO 实例。 */
    private CatalogContext context;

    // TODO, how to decide the real fileio is object store or not?
    @Override
    public boolean isObjectStore() {
        String warehouse = context.options().get(CatalogOptions.WAREHOUSE);
        if (warehouse == null) {
            return false;
        }
        Path path = new Path(warehouse);
        String scheme = path.toUri().getScheme();
        return scheme != null
                && !scheme.equalsIgnoreCase("file")
                && !scheme.equalsIgnoreCase("hdfs");
    }

    @Override
    public void configure(CatalogContext context) {
        Options options = new Options();
        context.options().toMap().forEach(options::set);
        options.set(RESOLVING_FILE_IO_ENABLED, false);
        this.context =
                CatalogContext.create(
                        options, context.hadoopConf(), context.preferIO(), context.fallbackIO());
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
     * 获取或创建指定路径的 FileIO 实例。
     *
     * <p>根据路径的 scheme 和 authority 从缓存中获取,如果不存在则创建新实例。
     *
     * @param path 文件路径
     * @return 对应的 FileIO 实例
     * @throws IOException 如果创建 FileIO 失败
     */
    @VisibleForTesting
    public FileIO fileIO(Path path) throws IOException {
        CacheKey cacheKey = new CacheKey(path.toUri().getScheme(), path.toUri().getAuthority());
        return fileIOMap.computeIfAbsent(
                cacheKey,
                k -> {
                    try {
                        return FileIO.get(path, context);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * 包装函数调用,在 ResolvingFileIO 的类加载器上下文中执行。
     *
     * @param func 要执行的函数
     * @param <T> 返回值类型
     * @return 函数执行结果
     * @throws IOException 如果执行失败
     */
    private <T> T wrap(Func<T> func) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ResolvingFileIO.class.getClassLoader());
            return func.apply();
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    /**
     * 函数式接口,用于包装需要在特定类加载器上下文中执行的操作。
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

    /**
     * 缓存键,用于标识 FileIO 实例。
     *
     * <p>由 scheme 和 authority 组成,用于区分不同的文件系统实例。
     * 例如:
     * <ul>
     *   <li>(hdfs, namenode1:9000) → HDFS FileIO 实例 1</li>
     *   <li>(hdfs, namenode2:9000) → HDFS FileIO 实例 2</li>
     *   <li>(s3, bucket1) → S3 FileIO 实例 1</li>
     * </ul>
     */
    private static class CacheKey implements Serializable {
        private final String scheme;
        private final String authority;

        private CacheKey(String scheme, String authority) {
            this.scheme = scheme;
            this.authority = authority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(scheme, cacheKey.scheme)
                    && Objects.equals(authority, cacheKey.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scheme, authority);
        }
    }
}
