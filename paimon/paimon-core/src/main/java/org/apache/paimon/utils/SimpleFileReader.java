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

import org.apache.paimon.annotation.Public;

import java.util.List;

/**
 * 简单文件读取器接口
 *
 * <p>SimpleFileReader 提供了一个简化的文件读取接口，用于将文件内容读取为条目列表。
 *
 * <p>核心功能：
 * <ul>
 *   <li>文件读取：根据文件名读取文件内容
 *   <li>条目转换：将文件内容解析为类型化的条目列表
 *   <li>简化接口：提供简单易用的读取方法
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>元数据读取：读取 manifest、snapshot 等元数据文件
 *   <li>配置读取：读取配置文件并解析为对象列表
 *   <li>批量加载：一次性加载文件的所有条目
 * </ul>
 *
 * <p>实现要点：
 * <ul>
 *   <li>类型安全：使用泛型 T 表示条目类型
 *   <li>简单接口：只需实现一个 read() 方法
 *   <li>异常处理：实现类应处理 IO 异常
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 实现文件读取器
 * class MyFileReader implements SimpleFileReader<MyEntry> {
 *     private final FileIO fileIO;
 *     private final Path basePath;
 *
 *     @Override
 *     public List<MyEntry> read(String fileName) {
 *         Path filePath = new Path(basePath, fileName);
 *         try (InputStream input = fileIO.newInputStream(filePath)) {
 *             // 解析文件内容
 *             return parseEntries(input);
 *         } catch (IOException e) {
 *             throw new RuntimeException("Failed to read file: " + fileName, e);
 *         }
 *     }
 * }
 *
 * // 使用文件读取器
 * SimpleFileReader<MyEntry> reader = new MyFileReader(fileIO, basePath);
 * List<MyEntry> entries = reader.read("manifest-list-uuid-0");
 *
 * // 处理条目
 * for (MyEntry entry : entries) {
 *     System.out.println("Entry: " + entry);
 * }
 * }</pre>
 *
 * @param <T> 条目类型
 * @since 0.9.0
 */
@Public
public interface SimpleFileReader<T> {

    /**
     * 读取文件并返回条目列表
     *
     * <p>根据文件名读取文件内容，并将其解析为条目列表。
     *
     * @param fileName 文件名（不含父目录路径）
     * @return 条目列表
     * @throws RuntimeException 如果读取或解析文件失败
     */
    List<T> read(String fileName);
}
