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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.deletionvectors.Bitmap64DeletionVector.toLittleEndianInt;

/**
 * 删除向量接口,用于高效记录文件中已删除行的位置信息。
 *
 * <p>删除向量(DeletionVector)是一种高效的数据结构,用于跟踪数据文件中哪些行已被删除。
 * 它允许在不修改原始数据文件的情况下标记删除的行,然后在读取时过滤掉这些删除的行。
 *
 * <h2>核心功能:</h2>
 * <ul>
 *   <li>标记删除: 记录特定位置的行已被删除
 *   <li>删除判断: 检查某个位置的行是否已被删除
 *   <li>向量合并: 合并多个删除向量
 *   <li>序列化: 支持持久化到文件系统
 * </ul>
 *
 * <h2>实现类型:</h2>
 * <ul>
 *   <li>{@link BitmapDeletionVector}: 基于RoaringBitmap32,支持最多2^31-1行
 *   <li>{@link Bitmap64DeletionVector}: 基于OptimizedRoaringBitmap64,支持更大文件
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>追加表的DELETE操作: 标记已删除的行而不重写整个文件
 *   <li>主键表的更新: 标记旧版本的行
 *   <li>MERGE操作: 跟踪需要删除或替换的行
 *   <li>过期数据清理: 标记过期的行待后续清理
 * </ul>
 *
 * <h2>存储格式:</h2>
 * 删除向量通常存储在单独的文件中,与数据文件关联。存储格式包括:
 * <pre>
 * [Length(4 bytes)][Magic Number(4 bytes)][Bitmap Data][CRC32(4 bytes)]
 * </pre>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li>空间效率: 使用压缩位图减少存储开销
 *   <li>时间效率: O(1)的删除标记和检查操作
 *   <li>内存友好: 稀疏删除时占用内存极少
 * </ul>
 *
 * @see BitmapDeletionVector 基于32位位图的实现
 * @see Bitmap64DeletionVector 基于64位位图的实现
 * @see DeletionVectorJudger 删除判断接口
 */
public interface DeletionVector extends DeletionVectorJudger {

    /**
     * 将指定位置的行标记为已删除。
     *
     * <p>此方法用于记录某一行已被删除。删除标记是幂等的,
     * 多次标记同一位置不会产生副作用。
     *
     * @param position 要标记为已删除的行位置,从0开始的索引
     * @throws IllegalArgumentException 如果位置超出实现类支持的最大范围
     */
    void delete(long position);

    /**
     * 将另一个删除向量合并到当前删除向量。
     *
     * <p>合并操作执行位OR操作,结果向量包含两个向量中所有被标记为删除的位置。
     * 这对于合并多个删除操作或聚合分布式删除非常有用。
     *
     * <p><b>注意:</b> 两个向量必须是相同的实现类型,否则会抛出异常。
     *
     * @param deletionVector 要合并的其他删除向量
     * @throws RuntimeException 如果deletionVector的类型与当前实例不匹配
     */
    void merge(DeletionVector deletionVector);

    /**
     * 检查并标记指定位置的行为已删除。
     *
     * <p>此方法首先检查位置是否已被删除,如果未删除则标记为删除。
     * 这是一个原子性的"检查然后设置"操作,用于避免重复标记。
     *
     * @param position 要标记为已删除的行位置
     * @return 如果该位置之前未被删除(即成功添加了新的删除标记)返回true,如果已经删除则返回false
     */
    default boolean checkedDelete(long position) {
        if (isDeleted(position)) {
            return false;
        } else {
            delete(position);
            return true;
        }
    }

    /**
     * 判断删除向量是否为空。
     *
     * @return 如果删除向量为空(不包含任何删除标记)返回true,否则返回false
     */
    boolean isEmpty();

    /**
     * 获取删除向量的基数(已删除的行数)。
     *
     * @return 添加到删除向量中的不同整数的数量,即被标记为删除的行数
     */
    long getCardinality();

    /**
     * 将删除向量序列化到输出流。
     *
     * <p>序列化格式包括:
     * <ul>
     *   <li>长度字段(4字节)
     *   <li>魔数(4字节,用于标识版本)
     *   <li>位图数据
     *   <li>CRC32校验和(4字节)
     * </ul>
     *
     * @param out 目标输出流
     * @return 写入的总字节数
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    int serializeTo(DataOutputStream out) throws IOException;

    /**
     * 从文件中读取删除向量。
     *
     * <p>根据魔数自动识别并反序列化对应版本的删除向量:
     * <ul>
     *   <li>V1格式: {@link BitmapDeletionVector}
     *   <li>V2格式: {@link Bitmap64DeletionVector}
     * </ul>
     *
     * @param fileIO 文件I/O接口
     * @param deletionFile 删除文件的元数据,包含路径、偏移量和长度
     * @return 反序列化的删除向量实例
     * @throws IOException 如果读取或反序列化失败
     */
    static DeletionVector read(FileIO fileIO, DeletionFile deletionFile) throws IOException {
        Path path = new Path(deletionFile.path());
        try (SeekableInputStream input = fileIO.newInputStream(path)) {
            input.seek(deletionFile.offset());
            DataInputStream dis = new DataInputStream(input);
            return read(dis, deletionFile.length());
        }
    }

    /**
     * 从数据输入流读取删除向量。
     *
     * <p>此方法处理向后兼容性,支持多个版本的删除向量格式:
     * <ul>
     *   <li>如果魔数为{@link BitmapDeletionVector#MAGIC_NUMBER},读取V1格式
     *   <li>如果魔数为{@link Bitmap64DeletionVector#MAGIC_NUMBER},读取V2格式
     *   <li>否则抛出异常
     * </ul>
     *
     * @param dis 数据输入流
     * @param length 预期的数据长度,可为null(不进行长度校验)
     * @return 反序列化的删除向量实例
     * @throws IOException 如果读取失败
     * @throws RuntimeException 如果魔数无效或长度不匹配
     */
    static DeletionVector read(DataInputStream dis, @Nullable Long length) throws IOException {
        // read bitmap length
        int bitmapLength = dis.readInt();
        // read magic number
        int magicNumber = dis.readInt();

        if (magicNumber == BitmapDeletionVector.MAGIC_NUMBER) {
            if (length != null && bitmapLength != length) {
                throw new RuntimeException(
                        "Size not match, actual size: "
                                + bitmapLength
                                + ", expected size: "
                                + length);
            }

            // magic number has been read
            byte[] bytes = new byte[bitmapLength - BitmapDeletionVector.MAGIC_NUMBER_SIZE_BYTES];
            dis.readFully(bytes);
            dis.skipBytes(4); // skip crc
            return BitmapDeletionVector.deserializeFromByteBuffer(ByteBuffer.wrap(bytes));
        } else if (toLittleEndianInt(magicNumber) == Bitmap64DeletionVector.MAGIC_NUMBER) {
            if (length != null) {
                long expectedBitmapLength =
                        length
                                - Bitmap64DeletionVector.LENGTH_SIZE_BYTES
                                - Bitmap64DeletionVector.CRC_SIZE_BYTES;
                if (bitmapLength != expectedBitmapLength) {
                    throw new RuntimeException(
                            "Size not match, actual size: "
                                    + bitmapLength
                                    + ", expected size: "
                                    + expectedBitmapLength);
                }
            }

            // magic number have been read
            byte[] bytes = new byte[bitmapLength - Bitmap64DeletionVector.MAGIC_NUMBER_SIZE_BYTES];
            dis.readFully(bytes);
            dis.skipBytes(4); // skip crc
            return Bitmap64DeletionVector.deserializeFromBitmapDataBytes(bytes);
        } else {
            throw new RuntimeException(
                    "Invalid magic number: "
                            + magicNumber
                            + ", v1 dv magic number: "
                            + BitmapDeletionVector.MAGIC_NUMBER
                            + ", v2 magic number: "
                            + Bitmap64DeletionVector.MAGIC_NUMBER);
        }
    }

    /**
     * 创建一个空的删除向量工厂。
     *
     * @return 总是返回空Optional的工厂实例
     */
    static Factory emptyFactory() {
        return fileName -> Optional.empty();
    }

    /**
     * 从删除向量维护器创建工厂。
     *
     * @param dvMaintainer 删除向量维护器,如果为null则返回空工厂
     * @return 删除向量工厂实例
     */
    static Factory factory(@Nullable BucketedDvMaintainer dvMaintainer) {
        if (dvMaintainer == null) {
            return emptyFactory();
        }
        return dvMaintainer::deletionVectorOf;
    }

    /**
     * 从文件元数据和删除文件列表创建工厂。
     *
     * <p>此工厂根据文件名查找对应的删除文件,并从中读取删除向量。
     *
     * @param fileIO 文件I/O接口
     * @param files 数据文件元数据列表
     * @param deletionFiles 删除文件列表,可为null
     * @return 删除向量工厂实例
     */
    static Factory factory(
            FileIO fileIO, List<DataFileMeta> files, @Nullable List<DeletionFile> deletionFiles) {
        DeletionFile.Factory factory = DeletionFile.factory(files, deletionFiles);
        return fileName -> {
            Optional<DeletionFile> deletionFile = factory.create(fileName);
            if (deletionFile.isPresent()) {
                return Optional.of(DeletionVector.read(fileIO, deletionFile.get()));
            }
            return Optional.empty();
        };
    }

    /**
     * 将删除向量序列化为字节数组。
     *
     * @param deletionVector 要序列化的删除向量
     * @return 序列化后的字节数组
     * @throws RuntimeException 如果序列化失败
     */
    static byte[] serializeToBytes(DeletionVector deletionVector) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            deletionVector.serializeTo(dos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从字节数组反序列化删除向量。
     *
     * @param bytes 包含序列化数据的字节数组
     * @return 反序列化的删除向量实例
     * @throws RuntimeException 如果反序列化失败
     */
    static DeletionVector deserializeFromBytes(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);
        try {
            return read(dis, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除向量工厂接口。
     *
     * <p>用于根据文件名创建对应的删除向量实例。
     */
    interface Factory {
        /**
         * 根据文件名创建删除向量。
         *
         * @param fileName 数据文件名
         * @return 删除向量的Optional包装,如果文件没有关联的删除向量则为空
         * @throws IOException 如果创建过程中发生I/O错误
         */
        Optional<DeletionVector> create(String fileName) throws IOException;
    }
}
