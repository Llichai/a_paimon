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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hadoop.SerializableConfiguration;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.HadoopUtils.getHadoopConfiguration;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Catalog 上下文类。
 *
 * <p>该类封装了 Catalog 操作所需的配置信息和运行时上下文。主要包含：
 * <ul>
 *   <li>Options：Catalog 配置选项（如仓库路径等）
 *   <li>Hadoop Configuration：Hadoop 环境配置
 *   <li>FileIO Loaders：文件 I/O 加载器（优先和备用）
 * </ul>
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>配置管理</b>：管理 Catalog 的各种配置选项</li>
 *   <li><b>Hadoop 集成</b>：提供 Hadoop 环境配置支持</li>
 *   <li><b>文件 IO 定制</b>：支持自定义文件 I/O 实现</li>
 *   <li><b>序列化支持</b>：可在分布式环境中序列化传输</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 使用仓库路径创建上下文
 * CatalogContext context = CatalogContext.create(new Path("hdfs://warehouse"));
 *
 * // 2. 使用配置选项创建
 * Options options = new Options();
 * options.set(WAREHOUSE, "hdfs://warehouse");
 * CatalogContext context = CatalogContext.create(options);
 *
 * // 3. 指定 Hadoop 配置
 * Configuration hadoopConf = new Configuration();
 * CatalogContext context = CatalogContext.create(options, hadoopConf);
 *
 * // 4. 自定义文件 IO 加载器
 * FileIOLoader preferLoader = ...;
 * FileIOLoader fallbackLoader = ...;
 * CatalogContext context = CatalogContext.create(
 *     options, preferLoader, fallbackLoader);
 * }</pre>
 *
 * <h2>设计考虑</h2>
 * <ul>
 *   <li><b>不可变性</b>：上下文对象创建后配置不可变，保证线程安全</li>
 *   <li><b>序列化</b>：使用 SerializableConfiguration 包装 Hadoop 配置以支持序列化</li>
 *   <li><b>灵活性</b>：支持多种创建方式，适应不同使用场景</li>
 *   <li><b>默认值</b>：自动从 Options 中提取 Hadoop 配置，简化使用</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public class CatalogContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Options options;
    private final SerializableConfiguration hadoopConf;
    @Nullable private final FileIOLoader preferIOLoader;
    @Nullable private final FileIOLoader fallbackIOLoader;

    private CatalogContext(
            Options options,
            @Nullable Configuration hadoopConf,
            @Nullable FileIOLoader preferIOLoader,
            @Nullable FileIOLoader fallbackIOLoader) {
        this.options = checkNotNull(options);
        this.hadoopConf =
                new SerializableConfiguration(
                        hadoopConf == null ? getHadoopConfiguration(options) : hadoopConf);
        this.preferIOLoader = preferIOLoader;
        this.fallbackIOLoader = fallbackIOLoader;
    }

    public static CatalogContext create(Path warehouse) {
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toString());
        return create(options);
    }

    public static CatalogContext create(Options options) {
        return new CatalogContext(options, null, null, null);
    }

    public static CatalogContext create(Options options, Configuration hadoopConf) {
        return new CatalogContext(options, hadoopConf, null, null);
    }

    public static CatalogContext create(Options options, FileIOLoader fallbackIOLoader) {
        return new CatalogContext(options, null, null, fallbackIOLoader);
    }

    public static CatalogContext create(
            Options options, FileIOLoader preferIOLoader, FileIOLoader fallbackIOLoader) {
        return new CatalogContext(options, null, preferIOLoader, fallbackIOLoader);
    }

    public static CatalogContext create(
            Options options,
            Configuration hadoopConf,
            FileIOLoader preferIOLoader,
            FileIOLoader fallbackIOLoader) {
        return new CatalogContext(options, hadoopConf, preferIOLoader, fallbackIOLoader);
    }

    public Options options() {
        return options;
    }

    /** Return hadoop {@link Configuration}. */
    public Configuration hadoopConf() {
        return hadoopConf.get();
    }

    @Nullable
    public FileIOLoader preferIO() {
        return preferIOLoader;
    }

    @Nullable
    public FileIOLoader fallbackIO() {
        return fallbackIOLoader;
    }
}
