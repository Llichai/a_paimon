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

package org.apache.paimon.disk;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 文件 I/O 通道接口
 *
 * <p>通道表示逻辑上属于同一资源的文件集合。例如，来自同一数据流的已排序运行文件集合，
 * 这些文件稍后将被归并在一起。
 *
 * <p>核心功能：
 * <ul>
 *   <li>通道管理：管理临时文件的生命周期
 *   <li>文件操作：获取文件大小、关闭、删除
 *   <li>通道 ID：唯一标识临时文件
 *   <li>NIO 集成：提供底层 FileChannel 访问
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>外部排序：溢写排序数据到磁盘
 *   <li>哈希表溢写：BytesHashMap 溢写分区到磁盘
 *   <li>缓冲区溢写：内存缓冲区溢出时写入磁盘
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface FileIOChannel {

    /**
     * 获取通道 ID
     *
     * @return 通道 ID
     */
    ID getChannelID();

    /**
     * 获取底层文件的大小
     *
     * @return 文件大小（字节）
     * @throws IOException IO 异常
     */
    long getSize() throws IOException;

    /**
     * 检查通道是否已关闭
     *
     * @return true 表示已关闭，false 表示未关闭
     */
    boolean isClosed();

    /**
     * 关闭通道
     *
     * <p>对于异步实现，此方法会等待所有待处理的请求完成。
     * 即使异常中断关闭过程，底层 FileChannel 也会被关闭。
     *
     * @throws IOException 等待待处理请求时发生的错误
     */
    void close() throws IOException;

    /**
     * 删除底层文件
     *
     * @throws IllegalStateException 通道仍然打开时抛出
     */
    void deleteChannel();

    /**
     * 获取底层 NIO FileChannel
     *
     * @return FileChannel
     */
    FileChannel getNioFileChannel();

    /**
     * 关闭通道并删除底层文件
     *
     * <p>对于异步实现，此方法会等待所有待处理的请求完成。
     *
     * @throws IOException 等待待处理请求时发生的错误
     */
    void closeAndDelete() throws IOException;

    // --------------------------------------------------------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * 文件通道 ID
     *
     * <p>唯一标识底层临时文件的 ID。
     *
     * <p>ID 组成：
     * <ul>
     *   <li>path：临时文件的绝对路径
     *   <li>bucketNum：所属的桶编号（用于负载均衡）
     * </ul>
     *
     * <p>文件命名规则：
     * <pre>
     * [prefix-]RANDOMHEX.channel
     * </pre>
     * 其中 RANDOMHEX 是 16 字节随机数的十六进制表示。
     */
    class ID {

        /** 随机字节长度 */
        private static final int RANDOM_BYTES_LENGTH = 16;

        /** 文件路径 */
        private final File path;

        /** 桶编号 */
        private final int bucketNum;

        /**
         * 私有构造函数
         *
         * @param path 文件路径
         * @param bucketNum 桶编号
         */
        private ID(File path, int bucketNum) {
            this.path = path;
            this.bucketNum = bucketNum;
        }

        /**
         * 创建通道 ID
         *
         * @param basePath 基础路径
         * @param bucketNum 桶编号
         * @param random 随机数生成器
         */
        public ID(File basePath, int bucketNum, Random random) {
            this.path = new File(basePath, randomString(random) + ".channel");
            this.bucketNum = bucketNum;
        }

        /**
         * 创建带前缀的通道 ID
         *
         * @param basePath 基础路径
         * @param bucketNum 桶编号
         * @param prefix 文件名前缀
         * @param random 随机数生成器
         */
        public ID(File basePath, int bucketNum, String prefix, Random random) {
            this.path = new File(basePath, prefix + "-" + randomString(random) + ".channel");
            this.bucketNum = bucketNum;
        }

        /**
         * 获取临时文件的绝对路径
         *
         * @return 文件路径字符串
         */
        public String getPath() {
            return path.getAbsolutePath();
        }

        /**
         * 获取临时文件的 File 对象
         *
         * @return File 对象
         */
        public File getPathFile() {
            return path;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ID) {
                ID other = (ID) obj;
                return this.path.equals(other.path) && this.bucketNum == other.bucketNum;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return path.hashCode();
        }

        @Override
        public String toString() {
            return path.getAbsolutePath();
        }

        /**
         * 生成随机字符串
         *
         * @param random 随机数生成器
         * @return 随机字符串（16字节的十六进制表示）
         */
        private static String randomString(Random random) {
            byte[] bytes = new byte[RANDOM_BYTES_LENGTH];
            random.nextBytes(bytes);
            return StringUtils.byteToHexString(bytes);
        }
    }

    /**
     * 通道枚举器
     *
     * <p>用于顺序生成逻辑上属于同一组的通道 ID。
     *
     * <p>枚举器按轮询方式将通道分配到不同的临时目录（桶），实现负载均衡。
     *
     * <p>文件命名规则：
     * <pre>
     * PREFIX.NNNNNN.channel
     * </pre>
     * 其中 PREFIX 是随机前缀，NNNNNN 是 6 位序列号。
     *
     * <p>使用场景：
     * <ul>
     *   <li>外部排序：为每个排序运行创建通道
     *   <li>哈希表溢写：为每个溢写分区创建通道
     * </ul>
     */
    final class Enumerator {

        /** 全局计数器（用于桶轮询） */
        private static final AtomicInteger GLOBAL_NUMBER = new AtomicInteger();

        /** 临时目录路径数组 */
        private final File[] paths;

        /** 文件名前缀（随机生成） */
        private final String namePrefix;

        /** 本地计数器（用于序列号） */
        private int localCounter;

        /**
         * 创建枚举器
         *
         * @param basePaths 临时目录数组
         * @param random 随机数生成器
         */
        public Enumerator(File[] basePaths, Random random) {
            this.paths = basePaths;
            this.namePrefix = FileIOChannel.ID.randomString(random);
            this.localCounter = 0;
        }

        /**
         * 生成下一个通道 ID
         *
         * <p>按轮询方式选择临时目录，生成格式为 "PREFIX.NNNNNN.channel" 的文件名
         *
         * @return 通道 ID
         */
        public FileIOChannel.ID next() {
            // 轮询选择桶（临时目录）
            int bucketNum = GLOBAL_NUMBER.getAndIncrement() % paths.length;
            // 生成文件名：前缀.序列号.channel
            String filename = String.format("%s.%06d.channel", namePrefix, (localCounter++));
            return new FileIOChannel.ID(new File(paths[bucketNum], filename), bucketNum);
        }
    }
}
