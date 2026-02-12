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

package org.apache.paimon.data;

/**
 * Blob消费者接口,用于处理Blob描述符。
 *
 * <h2>设计目的</h2>
 * <p>BlobConsumer提供了一个回调机制,用于处理行中的Blob字段:
 * <ul>
 *   <li>当序列化或写入行数据时,遇到Blob字段会调用此接口</li>
 *   <li>消费者可以决定如何处理Blob(如保存到文件、发送到远程存储等)</li>
 *   <li>返回值指示是否需要将Blob数据刷新到输出流</li>
 * </ul>
 *
 * <h2>典型使用场景</h2>
 * <ul>
 *   <li>将Blob数据写入单独的文件,行中只保存引用</li>
 *   <li>将Blob上传到对象存储,返回URL</li>
 *   <li>压缩或加密Blob数据</li>
 *   <li>收集Blob统计信息</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 实现将Blob保存到文件的消费者
 * class FileBlobConsumer implements BlobConsumer {
 *     private final Path blobDir;
 *
 *     @Override
 *     public boolean accept(String blobFieldName, BlobDescriptor descriptor) {
 *         // 保存Blob到文件
 *         Path file = blobDir.resolve(blobFieldName + "_" + System.nanoTime());
 *         Files.copy(new File(descriptor.uri()).toPath(), file);
 *
 *         // 更新descriptor指向新文件
 *         // ...
 *
 *         // 返回true表示需要刷新到输出流
 *         return true;
 *     }
 * }
 *
 * // 使用消费者
 * BlobConsumer consumer = new FileBlobConsumer(Paths.get("/blob/storage"));
 * // 在序列化时传递consumer
 * // ...
 * }</pre>
 *
 * <h2>返回值说明</h2>
 * <table border="1">
 *   <tr>
 *     <th>返回值</th>
 *     <th>含义</th>
 *     <th>适用场景</th>
 *   </tr>
 *   <tr>
 *     <td>true</td>
 *     <td>需要刷新到输出流</td>
 *     <td>Blob已处理,需要立即写入</td>
 *   </tr>
 *   <tr>
 *     <td>false</td>
 *     <td>不需要刷新</td>
 *     <td>Blob处理延迟,或批量处理</td>
 *   </tr>
 * </table>
 */
public interface BlobConsumer {

    /**
     * 接受一个Blob描述符并进行处理。
     *
     * <p>处理逻辑:
     * <ol>
     *   <li>接收Blob字段名和描述符</li>
     *   <li>根据业务需求处理Blob(如保存到文件、上传等)</li>
     *   <li>返回是否需要刷新到输出流</li>
     * </ol>
     *
     * @param blobFieldName Blob字段的名称
     * @param blobDescriptor Blob描述符,包含URI、偏移量和长度
     * @return 如果需要刷新到输出流返回true,否则返回false
     */
    boolean accept(String blobFieldName, BlobDescriptor blobDescriptor);
}
