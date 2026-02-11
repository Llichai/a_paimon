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

import org.apache.paimon.fs.Path;

/**
 * 路径工厂接口
 *
 * <p>PathFactory 定义了路径创建的通用接口，用于生成和转换文件路径。
 *
 * <p>主要功能：
 * <ul>
 *   <li>创建新路径：{@link #newPath()} - 生成新的唯一文件路径
 *   <li>路径转换：{@link #toPath(String)} - 将文件名转换为完整路径
 * </ul>
 *
 * <p>实现类包括：
 * <ul>
 *   <li>Manifest 文件工厂：{@link FileStorePathFactory#manifestFileFactory()}
 *   <li>Manifest List 工厂：{@link FileStorePathFactory#manifestListFactory()}
 *   <li>Index Manifest 工厂：{@link FileStorePathFactory#indexManifestFileFactory()}
 *   <li>统计文件工厂：{@link FileStorePathFactory#statsFileFactory()}
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 Manifest 文件工厂
 * PathFactory manifestFactory = fileStorePathFactory.manifestFileFactory();
 *
 * // 生成新的 Manifest 文件路径
 * Path newManifest = manifestFactory.newPath();
 * // 示例: /path/to/table/manifest/manifest-uuid-0
 *
 * // 将文件名转换为完整路径
 * Path existingManifest = manifestFactory.toPath("manifest-uuid-123");
 * // 结果: /path/to/table/manifest/manifest-uuid-123
 * }</pre>
 */
public interface PathFactory {

    /**
     * 创建新路径
     *
     * <p>生成一个新的、唯一的文件路径，通常包含 UUID 和递增计数器。
     *
     * @return 新生成的文件路径
     */
    Path newPath();

    /**
     * 将文件名转换为完整路径
     *
     * <p>将给定的文件名转换为完整的文件系统路径。
     *
     * @param fileName 文件名（不含父目录路径）
     * @return 完整的文件路径
     */
    Path toPath(String fileName);
}
