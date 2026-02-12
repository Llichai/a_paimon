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

import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;

/**
 * 全局索引文件写入器。
 *
 * <p>该接口定义了全局索引文件的写入操作,负责生成文件名和创建输出流。
 * 为索引数据的持久化提供底层 I/O 支持。
 *
 * <p>主要功能:
 * <ul>
 *   <li>根据前缀生成唯一的索引文件名
 *   <li>创建支持位置操作的输出流
 *   <li>管理索引文件的写入生命周期
 * </ul>
 */
public interface GlobalIndexFileWriter {

    /**
     * 生成新的索引文件名。
     *
     * @param prefix 文件名前缀,通常是索引类型标识符
     * @return 唯一的索引文件名
     */
    String newFileName(String prefix);

    /**
     * 为指定文件名创建新的输出流。
     *
     * @param fileName 索引文件名
     * @return 支持位置操作的输出流
     * @throws IOException 如果创建流失败
     */
    PositionOutputStream newOutputStream(String fileName) throws IOException;
}
