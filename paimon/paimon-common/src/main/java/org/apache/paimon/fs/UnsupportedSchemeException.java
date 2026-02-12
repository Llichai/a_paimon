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

import java.io.IOException;

/**
 * 表示不支持特定文件系统 scheme 的异常。
 *
 * <p>当尝试访问 Paimon 不支持的文件系统协议时抛出该异常。
 * 例如,尝试访问 {@code s3://} 路径但未配置 S3 FileIO 实现。
 *
 * <h3>常见场景</h3>
 * <ul>
 *   <li>未注册对应的 FileIOLoader 实现</li>
 *   <li>缺少必要的依赖包(如 S3、OSS 等)</li>
 *   <li>FileIO 配置错误或缺失</li>
 *   <li>使用了不支持的协议</li>
 * </ul>
 *
 * <h3>支持的 Scheme</h3>
 * <p>Paimon 默认支持:
 * <ul>
 *   <li>{@code file://} - 本地文件系统</li>
 *   <li>{@code hdfs://} - Hadoop 分布式文件系统</li>
 *   <li>{@code viewfs://} - Hadoop ViewFS</li>
 * </ul>
 *
 * <p>其他文件系统需要额外配置:
 * <ul>
 *   <li>{@code s3://} - AWS S3 (需要 paimon-s3 依赖)</li>
 *   <li>{@code oss://} - 阿里云 OSS (需要 paimon-oss 依赖)</li>
 *   <li>{@code gcs://} - Google Cloud Storage (需要 paimon-gcs 依赖)</li>
 *   <li>{@code azure://} - Azure Blob Storage (需要 paimon-azure 依赖)</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * try {
 *     FileIO fileIO = FileIO.get(new Path("ftp://example.com/data"));
 * } catch (UnsupportedSchemeException e) {
 *     // 处理不支持的 scheme
 *     System.err.println("不支持的文件系统: " + e.getMessage());
 * }
 * }</pre>
 *
 * <h3>解决方法</h3>
 * <ol>
 *   <li>检查路径的 URI scheme 是否正确</li>
 *   <li>确认对应的 FileIO 依赖已添加到类路径</li>
 *   <li>验证 FileIOLoader 是否正确注册(通过 SPI)</li>
 *   <li>检查配置选项是否正确设置</li>
 * </ol>
 *
 * @see FileIO
 * @see FileIOLoader
 * @since 0.4.0
 */
@Public
public class UnsupportedSchemeException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * 创建一个带有指定消息的新异常。
     *
     * @param message 异常消息,通常包含不支持的 scheme 信息
     */
    public UnsupportedSchemeException(String message) {
        super(message);
    }

    /**
     * 创建一个带有指定消息和原因的新异常。
     *
     * @param message 异常消息
     * @param cause 导致该异常的原因
     */
    public UnsupportedSchemeException(String message, Throwable cause) {
        super(message, cause);
    }
}
