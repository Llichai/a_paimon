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

import org.apache.paimon.annotation.Public;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * FileIO 加载器接口,用于加载 {@link FileIO} 实现。
 *
 * <p>该接口定义了 FileIO 的服务提供者接口(SPI)规范。通过实现此接口,
 * 可以插入自定义的 FileIO 实现来支持特定的文件系统或存储后端。
 *
 * <p><b>SPI 发现机制:</b>
 * <ol>
 *   <li>实现 FileIOLoader 接口</li>
 *   <li>在 META-INF/services/org.apache.paimon.fs.FileIOLoader 文件中注册实现类</li>
 *   <li>Paimon 会通过 Java SPI 机制自动发现并加载</li>
 * </ol>
 *
 * <p><b>选择策略:</b>
 * 当访问某个路径时,Paimon 会根据路径的 URI scheme 选择合适的 FileIOLoader:
 * <ol>
 *   <li>检查 preferIO (配置的首选 FileIO)</li>
 *   <li>通过 SPI 发现的 FileIOLoader,匹配 scheme</li>
 *   <li>检查 fallbackIO (配置的备用 FileIO)</li>
 *   <li>最后尝试 HadoopFileIO</li>
 * </ol>
 *
 * <p><b>实现示例:</b>
 * <pre>{@code
 * public class S3FileIOLoader implements FileIOLoader {
 *     @Override
 *     public String getScheme() {
 *         return "s3";
 *     }
 *
 *     @Override
 *     public List<String[]> requiredOptions() {
 *         return Arrays.asList(
 *             new String[]{"s3.access-key"},
 *             new String[]{"s3.secret-key"}
 *         );
 *     }
 *
 *     @Override
 *     public FileIO load(Path path) {
 *         return new S3FileIO();
 *     }
 * }
 * }</pre>
 *
 * @see FileIO
 * @since 0.4.0
 */
@Public
public interface FileIOLoader extends Serializable {

    /**
     * 获取此 FileIO 支持的 URI scheme。
     *
     * <p>scheme 用于匹配文件路径的协议部分,例如:
     * <ul>
     *   <li>"file" - 本地文件系统</li>
     *   <li>"hdfs" - Hadoop 分布式文件系统</li>
     *   <li>"s3" - Amazon S3</li>
     *   <li>"oss" - 阿里云对象存储</li>
     * </ul>
     *
     * @return URI scheme 字符串,不包含冒号
     */
    String getScheme();

    /**
     * 返回此 FileIO 实现所需的配置选项集合。
     *
     * <p>只有当这些选项在配置中存在时,才会选择此 FileIO 实现,
     * 否则会回退到 HadoopFileIO 或计算引擎自己的 FileIO。
     *
     * <p>选项键是不区分大小写的,可以以多种方式书写。每个 String[] 数组表示一组等效的键,
     * 只要其中一个存在即可满足要求。
     *
     * <p><b>示例:</b>
     * <pre>{@code
     * // 要求必须有 access-key 和 secret-key
     * return Arrays.asList(
     *     new String[]{"s3.access-key", "s3.access.key"},  // 两种写法都可以
     *     new String[]{"s3.secret-key", "s3.secret.key"}   // 两种写法都可以
     * );
     * }</pre>
     *
     * @return 必需的配置选项列表,如果没有必需选项则返回空列表
     */
    default List<String[]> requiredOptions() {
        return Collections.emptyList();
    }

    /**
     * 加载并创建 FileIO 实例。
     *
     * <p>此方法应该创建并返回一个新的 FileIO 实例。返回的 FileIO 将被配置
     * (通过 {@link FileIO#configure(org.apache.paimon.catalog.CatalogContext)})
     * 后使用。
     *
     * <p><b>注意:</b> 此方法可能被多次调用,实现应该确保每次调用都返回新的实例
     * 或者返回可安全共享的单例实例。
     *
     * @param path 要访问的路径,可用于决定如何配置 FileIO
     * @return FileIO 实例
     */
    FileIO load(Path path);
}
