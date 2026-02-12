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

package org.apache.paimon.fileindex;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fileindex.empty.EmptyFileIndexReader;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 文件索引文件格式定义。
 *
 * <p>定义了索引文件的存储格式,将所有列和偏移量信息放在文件头部。
 *
 * <p>文件格式说明:
 *
 * <pre>
 *  ______________________________________    _____________________
 * |     magic    ｜version｜head length  |
 * |--------------------------------------|
 * |            column number             |
 * |--------------------------------------|
 * |   column 1        ｜ index number    |
 * |--------------------------------------|
 * |  index name 1 ｜start pos ｜length   |
 * |--------------------------------------|
 * |  index name 2 ｜start pos ｜length   |
 * |--------------------------------------|
 * |  index name 3 ｜start pos ｜length   |
 * |--------------------------------------|            HEAD (文件头)
 * |   column 2        ｜ index number    |
 * |--------------------------------------|
 * |  index name 1 ｜start pos ｜length   |
 * |--------------------------------------|
 * |  index name 2 ｜start pos ｜length   |
 * |--------------------------------------|
 * |  index name 3 ｜start pos ｜length   |
 * |--------------------------------------|
 * |                 ...                  |
 * |--------------------------------------|
 * |                 ...                  |
 * |--------------------------------------|
 * |  redundant length ｜redundant bytes  |
 * |--------------------------------------|    ---------------------
 * |                BODY                  |
 * |                BODY                  |
 * |                BODY                  |             BODY (索引数据)
 * |                BODY                  |
 * |______________________________________|    _____________________
 *
 * 字段说明:
 * - magic:              8 字节 long,文件魔数标识
 * - version:            4 字节 int,版本号
 * - head length:        4 字节 int,文件头长度
 * - column number:      4 字节 int,列数量
 * - column x:           变长 UTF 字符串(长度+字节数组)
 * - index number:       4 字节 int,该列的索引数量
 * - index name x:       变长 UTF 字符串,索引类型名称
 * - start pos:          4 字节 int,索引数据在 BODY 中的起始位置
 * - length:             4 字节 int,索引数据的长度
 * - redundant length:   4 字节 int,预留字段长度(用于版本兼容,当前版本为0)
 * - redundant bytes:    变长字节数组(预留字段内容,当前版本为空)
 * - BODY:               索引数据主体,包含所有列的索引字节序列
 *
 * </pre>
 *
 * <p>该格式设计特点:
 * <ul>
 *   <li>文件头包含所有元数据,支持快速定位索引数据</li>
 *   <li>支持多列多索引类型,每列可以有多个不同类型的索引</li>
 *   <li>包含版本信息和预留字段,便于向后兼容</li>
 *   <li>通过偏移量支持随机访问索引数据</li>
 * </ul>
 */
public final class FileIndexFormat {

    /** 文件魔数,用于识别文件格式 */
    private static final long MAGIC = 1493475289347502L;

    /** 空索引标记,当某列的某种索引为空时使用 */
    private static final int EMPTY_INDEX_FLAG = -1;

    /** 文件格式版本枚举 */
    enum Version {
        /** 版本 1 */
        V_1(1);

        private final int version;

        Version(int version) {
            this.version = version;
        }

        /**
         * 获取版本号。
         *
         * @return 版本号整数值
         */
        public int version() {
            return version;
        }
    }

    /**
     * 创建索引文件写入器。
     *
     * @param outputStream 输出流
     * @return Writer 实例
     */
    public static Writer createWriter(OutputStream outputStream) {
        return new Writer(outputStream);
    }

    /**
     * 创建索引文件读取器。
     *
     * @param inputStream 可定位输入流
     * @param fileRowType 文件的行类型定义
     * @return Reader 实例
     */
    public static Reader createReader(SeekableInputStream inputStream, RowType fileRowType) {
        return new Reader(inputStream, fileRowType);
    }

    /**
     * 文件索引写入器。
     *
     * <p>负责将索引数据按照定义的格式写入输出流。写入过程分为两个阶段:
     * <ol>
     *   <li>构建文件头:记录所有列和索引的元数据信息</li>
     *   <li>写入 BODY:按顺序写入所有索引的字节数据</li>
     * </ol>
     */
    public static class Writer implements Closeable {

        private final DataOutputStream dataOutputStream;

        /** 预留字段长度,用于版本兼容 */
        private static final int REDUNDANT_LENGTH = 0;

        /**
         * 构造写入器。
         *
         * @param outputStream 输出流
         */
        public Writer(OutputStream outputStream) {
            this.dataOutputStream = new DataOutputStream(outputStream);
        }

        /**
         * 写入列索引数据。
         *
         * <p>处理流程:
         * <ol>
         *   <li>遍历所有列的索引,构建 BODY 数据</li>
         *   <li>记录每个索引在 BODY 中的位置和长度</li>
         *   <li>根据位置信息写入文件头</li>
         *   <li>最后写入 BODY 数据</li>
         * </ol>
         *
         * @param indexes 列名 -> (索引类型 -> 索引字节数据) 的映射
         * @throws IOException 写入失败时抛出
         */
        public void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes)
                throws IOException {

            // 记录每个索引在 BODY 中的位置信息
            Map<String, Map<String, Pair<Integer, Integer>>> bodyInfo = new LinkedHashMap<>();

            // 构建 BODY 数据,并记录位置信息
            ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            for (Map.Entry<String, Map<String, byte[]>> columnMap : indexes.entrySet()) {
                Map<String, Pair<Integer, Integer>> innerMap =
                        bodyInfo.computeIfAbsent(columnMap.getKey(), k -> new LinkedHashMap<>());
                Map<String, byte[]> bytesMap = columnMap.getValue();
                for (Map.Entry<String, byte[]> entry : bytesMap.entrySet()) {
                    int startPosition = baos.size();
                    byte[] v = entry.getValue();
                    if (v == null) {
                        // 空索引,使用特殊标记
                        innerMap.put(entry.getKey(), Pair.of(EMPTY_INDEX_FLAG, 0));
                    } else {
                        // 写入索引数据,记录位置和长度
                        baos.write(entry.getValue());
                        innerMap.put(
                                entry.getKey(),
                                Pair.of(startPosition, baos.size() - startPosition));
                    }
                }
            }
            byte[] body = baos.toByteArray();

            // 写入文件头
            writeHead(bodyInfo);

            // 写入 BODY
            dataOutputStream.write(body);
        }

        /**
         * 写入文件头。
         *
         * <p>文件头包含:
         * <ul>
         *   <li>魔数、版本号、头长度</li>
         *   <li>列数量和每列的索引信息</li>
         *   <li>每个索引的名称、起始位置、长度</li>
         *   <li>预留字段(用于版本兼容)</li>
         * </ul>
         *
         * @param bodyInfo 索引位置信息,列名 -> (索引类型 -> (起始位置, 长度))
         * @throws IOException 写入失败时抛出
         */
        private void writeHead(Map<String, Map<String, Pair<Integer, Integer>>> bodyInfo)
                throws IOException {

            int headLength = calculateHeadLength(bodyInfo);

            // 写入魔数
            dataOutputStream.writeLong(MAGIC);
            // 写入版本号
            dataOutputStream.writeInt(Version.V_1.version());
            // 写入文件头长度
            dataOutputStream.writeInt(headLength);
            // 写入列数量
            dataOutputStream.writeInt(bodyInfo.size());

            // 写入每列的索引信息
            for (Map.Entry<String, Map<String, Pair<Integer, Integer>>> entry :
                    bodyInfo.entrySet()) {
                // 写入列名
                dataOutputStream.writeUTF(entry.getKey());
                // 写入该列的索引类型数量
                dataOutputStream.writeInt(entry.getValue().size());

                // 写入每个索引的信息(名称、起始位置、长度)
                for (Map.Entry<String, Pair<Integer, Integer>> indexEntry :
                        entry.getValue().entrySet()) {
                    dataOutputStream.writeUTF(indexEntry.getKey());
                    int start = indexEntry.getValue().getLeft();
                    // 起始位置需要加上文件头长度
                    dataOutputStream.writeInt(
                            start == EMPTY_INDEX_FLAG ? EMPTY_INDEX_FLAG : start + headLength);
                    dataOutputStream.writeInt(indexEntry.getValue().getRight());
                }
            }
            // 写入预留字段长度
            dataOutputStream.writeInt(REDUNDANT_LENGTH);
        }

        /**
         * 计算文件头长度。
         *
         * <p>文件头长度包括:
         * <ul>
         *   <li>固定长度字段:魔数(8) + 版本(4) + 头长度(4) + 列数(4) + 预留长度(4) = 24字节</li>
         *   <li>每列的索引数量字段:4字节 × 列数</li>
         *   <li>每个索引的位置信息:8字节 × 索引总数</li>
         *   <li>所有变长字符串(列名和索引名)的长度</li>
         * </ul>
         *
         * @param bodyInfo 索引位置信息
         * @return 文件头总长度(字节)
         * @throws IOException 计算失败时抛出
         */
        private int calculateHeadLength(Map<String, Map<String, Pair<Integer, Integer>>> bodyInfo)
                throws IOException {
            // 基础长度:魔数(8) + 版本(4) + 头长度(4) + 列数(4)
            //         + 索引位置信息(8×索引数) + 索引数量字段(4×列数) + 预留长度(4)
            int baseLength =
                    8
                            + 4
                            + 4
                            + 4
                            + bodyInfo.values().stream().mapToInt(Map::size).sum() * 8
                            + bodyInfo.size() * 4
                            + 4;

            // 计算所有变长字符串的长度
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(baos);
            for (Map.Entry<String, Map<String, Pair<Integer, Integer>>> entry :
                    bodyInfo.entrySet()) {
                // 列名
                dataOutput.writeUTF(entry.getKey());
                // 该列的所有索引类型名称
                for (String s : entry.getValue().keySet()) {
                    dataOutput.writeUTF(s);
                }
            }

            return baseLength + baos.size();
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(dataOutputStream);
        }
    }

    /**
     * 文件索引读取器。
     *
     * <p>负责从索引文件中读取索引数据。读取过程:
     * <ol>
     *   <li>构造时读取并解析文件头,缓存所有元数据</li>
     *   <li>根据列名按需读取对应的索引数据</li>
     *   <li>创建相应类型的 FileIndexReader 实例</li>
     * </ol>
     */
    public static class Reader implements Closeable {

        /** 可定位输入流,用于随机访问索引数据 */
        private final SeekableInputStream seekableInputStream;

        /** 文件头缓存,列名 -> (索引类型 -> (起始位置, 长度)) */
        private final Map<String, Map<String, Pair<Integer, Integer>>> header = new HashMap<>();

        /** 字段定义映射,列名 -> 字段信息 */
        private final Map<String, DataField> fields = new HashMap<>();

        /**
         * 构造读取器并解析文件头。
         *
         * <p>初始化过程:
         * <ol>
         *   <li>验证魔数和版本号</li>
         *   <li>读取文件头长度</li>
         *   <li>解析所有列的索引元数据</li>
         *   <li>缓存到 header 映射中</li>
         * </ol>
         *
         * @param seekableInputStream 可定位输入流
         * @param fileRowType 文件的行类型定义
         * @throws RuntimeException 如果文件格式不正确或版本不支持
         */
        public Reader(SeekableInputStream seekableInputStream, RowType fileRowType) {
            this.seekableInputStream = seekableInputStream;
            DataInputStream dataInputStream = new DataInputStream(seekableInputStream);

            // 缓存字段定义
            fileRowType.getFields().forEach(field -> this.fields.put(field.name(), field));

            try {
                // 读取并验证魔数
                long magic = dataInputStream.readLong();
                if (magic != MAGIC) {
                    throw new RuntimeException("This file is not file index file.");
                }

                // 读取并验证版本号
                int version = dataInputStream.readInt();
                if (version != Version.V_1.version()) {
                    throw new RuntimeException(
                            "This index file is version of "
                                    + version
                                    + ", not in supported version list ["
                                    + Version.V_1.version()
                                    + "]");
                }

                // 读取文件头长度,并读取文件头内容
                int headLength = dataInputStream.readInt();
                byte[] head = new byte[headLength - 8 - 4 - 4]; // 减去已读取的魔数、版本、长度字段
                dataInputStream.readFully(head);

                // 解析文件头,提取所有列的索引元数据
                try (DataInputStream dataInput =
                        new DataInputStream(new ByteArrayInputStream(head))) {
                    int columnSize = dataInput.readInt();
                    for (int i = 0; i < columnSize; i++) {
                        String columnName = dataInput.readUTF();
                        int indexSize = dataInput.readInt();
                        Map<String, Pair<Integer, Integer>> indexMap =
                                this.header.computeIfAbsent(columnName, n -> new HashMap<>());

                        // 读取该列的所有索引类型及其位置信息
                        for (int j = 0; j < indexSize; j++) {
                            String indexType = dataInput.readUTF();
                            int startPos = dataInput.readInt();
                            int length = dataInput.readInt();
                            indexMap.put(indexType, Pair.of(startPos, length));
                        }
                    }
                }
            } catch (IOException e) {
                IOUtils.closeQuietly(seekableInputStream);
                throw new RuntimeException(
                        "Exception happens while construct file index reader.", e);
            }
        }

        /**
         * 读取指定列的所有索引。
         *
         * @param columnName 列名
         * @return 该列的所有 FileIndexReader 实例集合,如果列不存在返回空集合
         */
        public Set<FileIndexReader> readColumnIndex(String columnName) {
            return Optional.ofNullable(header.getOrDefault(columnName, null))
                    .map(
                            f ->
                                    f.entrySet().stream()
                                            .map(
                                                    entry ->
                                                            getFileIndexReader(
                                                                    columnName,
                                                                    entry.getKey(),
                                                                    entry.getValue()))
                                            .collect(Collectors.toSet()))
                    .orElse(Collections.emptySet());
        }

        /**
         * 创建指定列和索引类型的 FileIndexReader。
         *
         * @param columnName 列名
         * @param indexType 索引类型(如 "bloom-filter", "bitmap" 等)
         * @param startAndLength 索引数据在文件中的位置和长度
         * @return FileIndexReader 实例
         */
        private FileIndexReader getFileIndexReader(
                String columnName, String indexType, Pair<Integer, Integer> startAndLength) {
            // 空索引返回特殊实例
            if (startAndLength.getLeft() == EMPTY_INDEX_FLAG) {
                return EmptyFileIndexReader.INSTANCE;
            }

            // 根据索引类型创建相应的读取器
            return FileIndexer.create(
                            indexType,
                            FileIndexCommon.getFieldType(fields, columnName),
                            new Options())
                    .createReader(
                            seekableInputStream,
                            startAndLength.getLeft(),
                            startAndLength.getRight());
        }

        /**
         * 根据位置和长度读取字节数据。
         *
         * <p>使用可定位输入流随机访问指定位置的数据。
         *
         * @param startAndLength 起始位置和长度
         * @return 读取的字节数组
         * @throws RuntimeException 如果读取失败
         */
        private byte[] getBytesWithStartAndLength(Pair<Integer, Integer> startAndLength) {
            byte[] b = new byte[startAndLength.getRight()];
            try {
                // 定位到起始位置
                seekableInputStream.seek(startAndLength.getLeft());
                int n = 0;
                int len = b.length;

                // 循环读取,直到填满缓冲区
                while (n < len) {
                    int count = seekableInputStream.read(b, n, len - n);
                    if (count < 0) {
                        throw new EOFException();
                    }
                    n += count;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return b;
        }

        /**
         * 读取所有索引数据。
         *
         * @return 列名 -> (索引类型 -> 索引字节数据) 的映射
         */
        public Map<String, Map<String, byte[]>> readAll() {
            Map<String, Map<String, byte[]>> result = new HashMap<>();
            for (Map.Entry<String, Map<String, Pair<Integer, Integer>>> entryOuter :
                    header.entrySet()) {
                for (Map.Entry<String, Pair<Integer, Integer>> entryInner :
                        entryOuter.getValue().entrySet()) {
                    result.computeIfAbsent(entryOuter.getKey(), key -> new HashMap<>())
                            .put(
                                    entryInner.getKey(),
                                    getBytesWithStartAndLength(entryInner.getValue()));
                }
            }
            return result;
        }

        /**
         * 读取指定列和索引类型的字节数据。
         *
         * <p>注意:此方法仅用于测试。
         *
         * @param columnName 列名
         * @param indexType 索引类型
         * @return 索引的字节数据,如果不存在返回空
         */
        @VisibleForTesting
        Optional<byte[]> getBytesWithNameAndType(String columnName, String indexType) {
            return Optional.ofNullable(header.getOrDefault(columnName, null))
                    .map(i -> i.getOrDefault(indexType, null))
                    .map(this::getBytesWithStartAndLength);
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(seekableInputStream);
        }
    }
}
