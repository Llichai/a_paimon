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

import javax.annotation.Nullable;

/**
 * 表示文件系统独立的客户端文件信息的接口。
 *
 * <p>该接口提供了获取文件元数据的标准方法,包括文件大小、路径、修改时间等信息。
 * 所有文件系统实现都应提供 FileStatus 的实现。
 *
 * @since 0.4.0
 */
@Public
public interface FileStatus {

    /**
     * 返回此文件的长度。
     *
     * @return 文件长度(字节)
     */
    long getLen();

    /**
     * 检查此对象是否表示目录。
     *
     * @return 如果是目录返回 <code>true</code>,否则返回 <code>false</code>
     */
    boolean isDir();

    /**
     * 返回与 FileStatus 对应的路径。
     *
     * @return 与 FileStatus 对应的路径
     */
    Path getPath();

    /**
     * 获取文件的最后修改时间。
     *
     * @return 表示文件最后修改时间的长整型值,
     *         以自纪元(UTC 1970年1月1日)以来的毫秒数表示
     */
    long getModificationTime();

    /**
     * 获取文件的最后访问时间。
     *
     * @return 表示文件最后访问时间的长整型值,
     *         以自纪元(UTC 1970年1月1日)以来的毫秒数表示
     */
    default long getAccessTime() {
        return 0;
    }

    /**
     * 返回此文件的所有者。
     *
     * @return 文件所有者,可能为 null
     */
    @Nullable
    default String getOwner() {
        return null;
    }
}
