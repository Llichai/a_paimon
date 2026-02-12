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

import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * 基于流供应商的Blob实现,通过提供者函数延迟创建输入流。
 *
 * <h2>设计目的</h2>
 * <p>BlobStream用于包装动态或延迟创建的输入流:
 * <ul>
 *   <li>延迟加载: 只在需要时才创建输入流</li>
 *   <li>动态数据: 适用于每次读取可能不同的数据源</li>
 *   <li>资源控制: 由调用方决定何时创建和关闭流</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>动态生成的数据(如加密、压缩后的数据)</li>
 *   <li>需要延迟打开的资源(避免过早占用)</li>
 *   <li>需要多次独立读取的数据源</li>
 *   <li>自定义的流创建逻辑</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 延迟打开文件
 * BlobStream blob1 = new BlobStream(() -> {
 *     try {
 *         return new FileInputStream("/path/to/file");
 *     } catch (IOException e) {
 *         throw new UncheckedIOException(e);
 *     }
 * });
 *
 * // 动态生成数据
 * BlobStream blob2 = new BlobStream(() -> {
 *     byte[] data = generateData(); // 每次调用生成新数据
 *     return new ByteArraySeekableStream(data);
 * });
 *
 * // 使用流
 * SeekableInputStream stream1 = blob1.newInputStream();
 * stream1.read(...);
 * stream1.close();
 *
 * // 再次创建新流
 * SeekableInputStream stream2 = blob1.newInputStream();
 * stream2.read(...);
 * stream2.close();
 * }</pre>
 *
 * <h2>方法行为</h2>
 * <ul>
 *   <li>toData(): 不支持,抛出UnsupportedOperationException</li>
 *   <li>toDescriptor(): 不支持,抛出UnsupportedOperationException</li>
 *   <li>newInputStream(): 调用supplier.get()获取新流</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>每次调用newInputStream()都会调用supplier创建新流</li>
 *   <li>supplier必须线程安全(如果在多线程环境使用)</li>
 *   <li>supplier抛出的异常会被传播给调用方</li>
 *   <li>不支持序列化(Supplier通常不可序列化)</li>
 * </ul>
 */
public class BlobStream implements Blob {

    /** 输入流提供者,用于延迟创建输入流 */
    private final Supplier<SeekableInputStream> inputStreamSupplier;

    /**
     * 创建BlobStream实例。
     *
     * @param inputStreamSupplier 输入流提供者函数
     */
    public BlobStream(Supplier<SeekableInputStream> inputStreamSupplier) {
        this.inputStreamSupplier = inputStreamSupplier;
    }

    /**
     * 尝试转换为字节数组。
     *
     * <p>BlobStream不支持此操作,因为:
     * <ul>
     *   <li>流可能每次返回不同的数据</li>
     *   <li>流可能无法全部加载到内存</li>
     *   <li>设计上就是用于流式访问</li>
     * </ul>
     *
     * @throws UnsupportedOperationException 总是抛出异常
     */
    @Override
    public byte[] toData() {
        throw new UnsupportedOperationException("Blob stream can not convert to data.");
    }

    /**
     * 尝试转换为描述符。
     *
     * <p>BlobStream不支持此操作,因为流数据没有固定的URI或位置。
     *
     * @throws UnsupportedOperationException 总是抛出异常
     */
    @Override
    public BlobDescriptor toDescriptor() {
        throw new UnsupportedOperationException("Blob stream can not convert to descriptor.");
    }

    /**
     * 创建新的输入流。
     *
     * <p>调用inputStreamSupplier.get()获取新的输入流实例。
     * 每次调用都会创建新的独立流。
     *
     * @return 新创建的输入流
     * @throws IOException 如果supplier抛出IOException
     */
    @Override
    public SeekableInputStream newInputStream() throws IOException {
        return inputStreamSupplier.get();
    }
}
