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

import org.apache.paimon.data.InternalRow;

import java.io.Closeable;
import java.io.IOException;

/**
 * 记录写入器接口。
 *
 * <p>该接口定义了向文件格式写入数据行的标准方法，支持批量缓冲和自动刷新。
 */
public interface FormatWriter extends Closeable {

    /**
     * 向编码器添加一个元素。编码器可能会临时缓冲该元素，或立即将其写入流中。
     *
     * <p>添加此元素可能会填满内部缓冲区，并触发一批内部缓冲元素的编码和刷新操作。
     *
     * @param element 要添加的数据行
     * @throws IOException 如果无法将元素添加到编码器，或输出流抛出异常
     */
    void addElement(InternalRow element) throws IOException;

    /**
     * 检查写入器是否已达到指定的目标大小。
     *
     * <p>该方法用于判断是否需要滚动到新文件，避免文件过大。
     *
     * @param suggestedCheck 是否需要检查的建议，但子类也可以自己决定是否检查
     * @param targetSize 目标大小（字节）
     * @return 如果已达到目标大小返回 true，否则返回 false
     * @throws IOException 如果计算长度失败
     */
    boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException;
}
