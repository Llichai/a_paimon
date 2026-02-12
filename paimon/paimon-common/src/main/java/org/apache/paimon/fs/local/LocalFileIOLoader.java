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

package org.apache.paimon.fs.local;

import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;

/**
 * 用于加载 {@link LocalFileIO} 的 {@link FileIOLoader} 实现。
 *
 * <p>该加载器负责识别和创建本地文件系统(file://)的 FileIO 实例。
 * 它通过 Java SPI 机制被自动发现和注册。
 *
 * <h3>支持的 Scheme</h3>
 * <ul>
 *   <li><b>file</b>: 本地文件系统协议</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <p>当 Paimon 遇到 {@code file://} 开头的路径时,会自动通过该加载器创建 LocalFileIO 实例。
 *
 * <h3>示例路径</h3>
 * <pre>
 * file:///tmp/paimon/warehouse
 * file:///var/data/tables
 * file://localhost/home/user/data
 * </pre>
 *
 * @see LocalFileIO
 * @see FileIOLoader
 */
public class LocalFileIOLoader implements FileIOLoader {

    private static final long serialVersionUID = 1L;

    /** 本地文件系统的 URI scheme: "file"。 */
    public static final String SCHEME = "file";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public LocalFileIO load(Path path) {
        return new LocalFileIO();
    }
}
