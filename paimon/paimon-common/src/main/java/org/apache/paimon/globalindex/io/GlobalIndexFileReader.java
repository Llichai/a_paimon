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

package org.apache.paimon.globalindex.io;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;

import java.io.IOException;

/**
 * 全局索引文件读取器。
 *
 * <p>该接口定义了全局索引文件的读取操作,负责根据索引元数据创建输入流。
 * 支持随机访问的流式读取,用于高效地读取索引文件的不同部分。
 *
 * <p>主要用途:
 * <ul>
 *   <li>根据索引文件元数据打开输入流
 *   <li>支持随机访问和顺序读取
 *   <li>为索引查询提供底层 I/O 支持
 * </ul>
 */
public interface GlobalIndexFileReader {

    /**
     * 根据索引元数据获取可随机访问的输入流。
     *
     * @param meta 全局索引 I/O 元数据,包含文件路径、大小等信息
     * @return 可随机访问的输入流
     * @throws IOException 如果打开流失败
     */
    SeekableInputStream getInputStream(GlobalIndexIOMeta meta) throws IOException;
}
