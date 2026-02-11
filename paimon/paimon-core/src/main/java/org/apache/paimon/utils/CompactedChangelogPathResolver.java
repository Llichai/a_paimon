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

import org.apache.paimon.fs.Path;

/**
 * 压缩 Changelog 文件路径解析器
 *
 * <p>该工具类用于解析压缩的 changelog 文件路径，支持"假路径"到"真实路径"的转换。
 *
 * <p><b>为什么需要路径解析？</b>
 * 在 FULL_COMPACTION/LOOKUP 模式下，多个 bucket 可能共享同一个 changelog 文件，
 * 为了节省存储空间，使用"假路径"机制：
 * <ul>
 *   <li>真实文件：只存储一份，位于某个 bucket 目录下
 *   <li>假路径：记录在其他 bucket 的 manifest 中，指向真实文件的某个片段
 * </ul>
 *
 * <p><b>文件名协议：</b>
 * <pre>
 * 1. 真实文件名格式：
 *    bucket-{bid1}/compacted-changelog-xxx${bid1}-{len1}.avro
 *    - bid1：真实文件所在的 bucket ID
 *    - len1：文件总长度
 *    - 读取时：从偏移量 0 开始，读取 len1 个字节
 *
 * 2. 假文件名格式：
 *    bucket-{bid2}/compacted-changelog-xxx${bid1}-{len1}-{off}-{len2}.avro
 *    - bid2：假路径所在的 bucket ID（不是真实文件位置）
 *    - bid1：真实文件所在的 bucket ID
 *    - len1：真实文件的总长度
 *    - off：读取的起始偏移量
 *    - len2：需要读取的长度
 *    - 读取时：读取 bucket-{bid1} 目录下的文件，从偏移量 off 开始，读取 len2 个字节
 * </pre>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * 场景：bucket-0 和 bucket-1 共享同一个 changelog 文件
 *
 * 1. 真实文件（存储在 bucket-0）：
 *    path: bucket-0/compacted-changelog-20240101$0-1000.avro
 *    内容：[0-999] 字节，包含 bucket-0 的 changelog
 *
 * 2. bucket-1 的 manifest 记录（假路径）：
 *    path: bucket-1/compacted-changelog-20240101$0-1000-100-200.avro
 *    解析：
 *      - 真实文件：bucket-0/compacted-changelog-20240101$0-1000.avro
 *      - 偏移量：100
 *      - 长度：200
 *    读取时：读取 bucket-0 的文件，从第 100 字节开始，读取 200 字节
 * </pre>
 *
 * <p><b>优势：</b>
 * <ul>
 *   <li>节省存储：多个 bucket 共享一个物理文件，避免重复存储
 *   <li>减少写入：只需写入一次物理文件，其他 bucket 使用假路径引用
 *   <li>提高性能：减少磁盘 I/O 和网络传输
 * </ul>
 *
 * <p><b>适用场景：</b>
 * <ul>
 *   <li>FULL_COMPACTION/LOOKUP 模式的 changelog 文件
 *   <li>多个 bucket 的 changelog 内容相同或部分相同
 *   <li>需要优化存储空间的场景
 * </ul>
 *
 * @see org.apache.paimon.Changelog
 * @see org.apache.paimon.mergetree.compact.FullChangelogMergeTreeCompactRewriter
 * @see org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter
 */
public class CompactedChangelogPathResolver {

    /**
     * 检查路径是否为压缩 changelog 文件路径
     *
     * <p>判断依据：文件名是否以 "compacted-changelog-" 开头。
     *
     * @param path 文件路径
     * @return true 如果是压缩 changelog 文件，false 否则
     */
    public static boolean isCompactedChangelogPath(Path path) {
        return path.getName().startsWith("compacted-changelog-");
    }

    /**
     * 解析文件路径，将假路径转换为真实路径
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>如果不是压缩 changelog 文件，直接返回原路径
     *   <li>如果是压缩 changelog 文件，解码路径并返回真实文件路径
     * </ol>
     *
     * <p>注意：返回的是真实文件路径，但偏移量和长度信息存储在 {@link DecodeResult} 中。
     *
     * @param path 待解析的文件路径（可能是真实路径或假路径）
     * @return 解析后的真实文件路径（如果是假路径）或原路径（如果不是压缩 changelog）
     */
    public static Path resolveCompactedChangelogPath(Path path) {
        if (!isCompactedChangelogPath(path)) {
            return path;
        }
        return decodePath(path).getPath();
    }

    /**
     * 解码压缩 changelog 文件路径
     *
     * <p>解析文件名，提取真实路径、偏移量和长度信息。
     *
     * <p>解码规则：
     * <pre>
     * 1. 真实文件名：compacted-changelog-xxx${bid}-{len}.avro
     *    解码结果：
     *      - path: 原路径
     *      - offset: 0
     *      - length: len
     *
     * 2. 假文件名：compacted-changelog-xxx${bid1}-{len1}-{off}-{len2}.avro
     *    解码结果：
     *      - path: bucket-{bid1}/compacted-changelog-xxx${bid1}-{len1}.avro
     *      - offset: off
     *      - length: len2
     * </pre>
     *
     * @param path 待解码的文件路径
     * @return 解码结果，包含真实路径、偏移量和长度
     */
    public static DecodeResult decodePath(Path path) {
        // 1. 分离文件名和扩展名：compacted-changelog-xxx$0-1000.avro -> [compacted-changelog-xxx$0-1000, avro]
        String[] nameAndFormat = path.getName().split("\\.");

        // 2. 分离名称和参数：compacted-changelog-xxx$0-1000 -> [compacted-changelog-xxx, 0-1000]
        String[] names = nameAndFormat[0].split("\\$");

        // 3. 分离参数：0-1000 或 0-1000-100-200
        String[] split = names[1].split("-");

        if (split.length == 2) {
            // 情况1：真实文件名 ${bid}-{len}
            // 示例：compacted-changelog-xxx$0-1000.avro
            return new DecodeResult(path, 0, Long.parseLong(split[1]));
        } else {
            // 情况2：假文件名 ${bid1}-{len1}-{off}-{len2}
            // 示例：compacted-changelog-xxx$0-1000-100-200.avro
            // 需要构造真实路径：bucket-{bid1}/compacted-changelog-xxx${bid1}-{len1}.avro
            Path realPath =
                    new Path(
                            path.getParent().getParent(), // 回到上级目录（表目录）
                            "bucket-"
                                    + split[0] // bid1
                                    + "/"
                                    + names[0] // compacted-changelog-xxx
                                    + "$"
                                    + split[0] // bid1
                                    + "-"
                                    + split[1] // len1
                                    + "."
                                    + nameAndFormat[1]); // 扩展名（如 avro）
            return new DecodeResult(realPath, Long.parseLong(split[2]), Long.parseLong(split[3]));
        }
    }

    /**
     * 路径解码结果
     *
     * <p>包含解码后的真实路径、读取偏移量和长度信息。
     */
    public static class DecodeResult {

        /** 真实文件路径 */
        private final Path path;

        /** 读取起始偏移量（字节） */
        private final long offset;

        /** 需要读取的长度（字节） */
        private final long length;

        /**
         * 构造 DecodeResult
         *
         * @param path 真实文件路径
         * @param offset 读取起始偏移量
         * @param length 需要读取的长度
         */
        public DecodeResult(Path path, long offset, long length) {
            this.path = path;
            this.offset = offset;
            this.length = length;
        }

        /**
         * 获取真实文件路径
         *
         * @return 真实文件路径
         */
        public Path getPath() {
            return path;
        }

        /**
         * 获取读取起始偏移量
         *
         * @return 偏移量（字节）
         */
        public long getOffset() {
            return offset;
        }

        /**
         * 获取需要读取的长度
         *
         * @return 长度（字节）
         */
        public long getLength() {
            return length;
        }
    }
}
