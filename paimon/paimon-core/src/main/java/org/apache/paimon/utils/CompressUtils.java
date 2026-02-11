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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * 压缩工具类
 *
 * <p>CompressUtils 提供文件压缩功能。
 *
 * <p>核心功能：
 * <ul>
 *   <li>GZIP 压缩：{@link #gzipCompressFile} - 使用 GZIP 压缩文件
 * </ul>
 *
 * <p>支持的压缩格式：
 * <ul>
 *   <li>GZIP：GNU zip 压缩格式
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>日志压缩：压缩日志文件以节省存储空间
 *   <li>备份压缩：压缩备份文件
 *   <li>数据归档：压缩归档数据
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // GZIP 压缩文件
 * String sourceFile = "/path/to/file.txt";
 * String compressedFile = "/path/to/file.txt.gz";
 * CompressUtils.gzipCompressFile(sourceFile, compressedFile);
 * }</pre>
 */
public class CompressUtils {

    /**
     * 使用 GZIP 压缩文件
     *
     * @param src 源文件路径
     * @param dest 目标文件路径（压缩后）
     * @throws IOException 如果 I/O 错误
     */
    public static void gzipCompressFile(String src, String dest) throws IOException {
        FileInputStream fis = new FileInputStream(src);
        FileOutputStream fos = new FileOutputStream(dest);
        GZIPOutputStream gzipOs = new GZIPOutputStream(fos);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while (true) {
            bytesRead = fis.read(buffer);
            if (bytesRead == -1) {
                fis.close();
                gzipOs.close();
                return;
            }

            gzipOs.write(buffer, 0, bytesRead);
        }
    }
}
