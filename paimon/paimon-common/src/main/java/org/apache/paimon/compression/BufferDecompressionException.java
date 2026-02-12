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

package org.apache.paimon.compression;

/**
 * 缓冲区解压缩异常。
 *
 * <p>当目标数据无法被解压缩时抛出 {@code BufferDecompressionException},
 * 例如数据损坏、目标缓冲区空间不足无法存储解压缩结果等情况。
 *
 * <p>常见场景:
 * <ul>
 *   <li>压缩数据已损坏或格式错误</li>
 *   <li>目标缓冲区太小,无法容纳解压缩后的数据</li>
 *   <li>解压缩算法内部错误</li>
 *   <li>压缩数据不完整</li>
 * </ul>
 */
public class BufferDecompressionException extends RuntimeException {

    public BufferDecompressionException() {
        super();
    }

    public BufferDecompressionException(String message) {
        super(message);
    }

    public BufferDecompressionException(String message, Throwable e) {
        super(message, e);
    }

    public BufferDecompressionException(Throwable e) {
        super(e);
    }
}
