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

package org.apache.paimon.index;

import org.apache.paimon.fs.Path;

/**
 * 索引路径工厂接口。
 *
 * <p>用于创建和管理索引文件的路径。
 */
public interface IndexPathFactory {

    /**
     * 根据文件名获取路径。
     *
     * @param fileName 文件名
     * @return 文件路径
     */
    Path toPath(String fileName);

    /**
     * 创建新的索引文件路径。
     *
     * @return 新文件路径
     */
    Path newPath();

    /**
     * 从索引文件元数据获取路径。
     *
     * @param file 索引文件元数据
     * @return 文件路径
     */
    default Path toPath(IndexFileMeta file) {
        return toPath(file.fileName());
    }

    /**
     * 是否使用外部路径。
     *
     * @return 如果使用外部路径则返回true
     */
    boolean isExternalPath();
}
