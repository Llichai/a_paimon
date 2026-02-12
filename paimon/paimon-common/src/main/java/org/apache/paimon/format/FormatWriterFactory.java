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

package org.apache.paimon.format;

import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;

/**
 * 创建文件格式写入器的工厂接口。
 *
 * <p>该接口定义了创建 {@link FormatWriter} 的标准方法，用于向不同格式的文件写入数据。
 */
public interface FormatWriterFactory {

    /**
     * 创建写入器，将编码后的数据写入给定的输出流。
     *
     * @param out 输出流，用于写入编码后的数据
     * @param compression 压缩类型，如 "none"、"snappy"、"gzip" 等
     * @return 格式写入器实例
     * @throws IOException 如果无法打开写入器，或输出流抛出异常
     */
    FormatWriter create(PositionOutputStream out, String compression) throws IOException;
}
