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

import org.apache.paimon.utils.OptimizedRoaringBitmap64;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * 基于{@link OptimizedRoaringBitmap64}的64位删除向量实现。
 *
 * <p>此实现使用OptimizedRoaringBitmap64作为底层数据结构,支持行数高达
 * {@link OptimizedRoaringBitmap64#MAX_VALUE}的超大文件。相比{@link BitmapDeletionVector},
 * 它可以处理行数超过2^31-1的文件。
 *
 * <h2>技术特点:</h2>
 * <ul>
 *   <li><b>64位支持</b>: 可处理超过21亿行的超大文件
 *   <li><b>运行长度编码</b>: 序列化前自动执行RLE压缩,进一步减少存储空间
 *   <li><b>优化的位图</b>: 使用分层位图结构,高效处理稀疏数据
 *   <li><b>向后兼容</b>: 可以从BitmapDeletionVector转换而来
 * </ul>
 *
 * <h2>存储格式(V2):</h2>
 * <pre>
 * [总长度:4字节][魔数:4字节(小端序)][位图数据][CRC32:4字节]
 * 魔数: 1681511377 (用于识别V2格式)
 * 所有数据使用小端序(Little-Endian)编码
 * </pre>
 *
 * <h2>性能优化:</h2>
 * <ul>
 *   <li>序列化时自动运行长度编码以减少存储
 *   <li>支持超过2GB的索引大小限制检查
 *   <li>使用CRC32校验和确保数据完整性
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建64位删除向量
 * Bitmap64DeletionVector dv = new Bitmap64DeletionVector();
 *
 * // 标记超大位置
 * dv.delete(3000000000L);
 *
 * // 从32位版本转换
 * BitmapDeletionVector dv32 = new BitmapDeletionVector();
 * Bitmap64DeletionVector dv64 = Bitmap64DeletionVector.fromBitmapDeletionVector(dv32);
 * }</pre>
 *
 * <p><b>参考:</b> 大部分代码从Apache Iceberg项目移植而来。
 *
 * @see DeletionVector 删除向量接口
 * @see BitmapDeletionVector 32位版本
 * @see OptimizedRoaringBitmap64 底层位图实现
 */
public class Bitmap64DeletionVector implements DeletionVector {

    /** 魔数,用于标识V2格式的删除向量 */
    public static final int MAGIC_NUMBER = 1681511377;
    /** 长度字段的字节大小 */
    public static final int LENGTH_SIZE_BYTES = 4;
    /** CRC校验和字段的字节大小 */
    public static final int CRC_SIZE_BYTES = 4;
    /** 魔数字段的字节大小 */
    public static final int MAGIC_NUMBER_SIZE_BYTES = 4;
    /** 位图数据在字节数组中的偏移量 */
    private static final int BITMAP_DATA_OFFSET = 4;

    /** 底层OptimizedRoaringBitmap64实例,存储已删除行的位置 */
    private final OptimizedRoaringBitmap64 roaringBitmap;

    /** 创建一个空的64位删除向量 */
    public Bitmap64DeletionVector() {
        this.roaringBitmap = new OptimizedRoaringBitmap64();
    }

    /**
     * 使用指定的OptimizedRoaringBitmap64创建删除向量。
     *
     * @param roaringBitmap 已存在的64位位图实例
     */
    private Bitmap64DeletionVector(OptimizedRoaringBitmap64 roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    /**
     * 从32位删除向量转换为64位版本。
     *
     * <p>这个方法用于升级现有的32位删除向量到64位版本,
     * 以支持更大的文件。
     *
     * @param bitmapDeletionVector 要转换的32位删除向量
     * @return 转换后的64位删除向量
     */
    public static Bitmap64DeletionVector fromBitmapDeletionVector(
            BitmapDeletionVector bitmapDeletionVector) {
        RoaringBitmap32 roaringBitmap32 = bitmapDeletionVector.get();
        return new Bitmap64DeletionVector(
                OptimizedRoaringBitmap64.fromRoaringBitmap32(roaringBitmap32));
    }

    @Override
    public void delete(long position) {
        roaringBitmap.add(position);
    }

    @Override
    public void merge(DeletionVector deletionVector) {
        if (deletionVector instanceof Bitmap64DeletionVector) {
            roaringBitmap.or(((Bitmap64DeletionVector) deletionVector).roaringBitmap);
        } else {
            throw new RuntimeException("Only instance with the same class type can be merged.");
        }
    }

    @Override
    public boolean isDeleted(long position) {
        return roaringBitmap.contains(position);
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    @Override
    public long getCardinality() {
        return roaringBitmap.cardinality();
    }

    @Override
    public int serializeTo(DataOutputStream out) throws IOException {
        roaringBitmap.runLengthEncode(); // run-length encode the bitmap before serializing
        int bitmapDataLength = computeBitmapDataLength(roaringBitmap); // magic bytes + bitmap
        byte[] bytes = new byte[LENGTH_SIZE_BYTES + bitmapDataLength + CRC_SIZE_BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(bitmapDataLength);
        serializeBitmapData(bytes, bitmapDataLength, roaringBitmap);
        int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
        int crc = computeChecksum(bytes, bitmapDataLength);
        buffer.putInt(crcOffset, crc);
        buffer.rewind();
        out.write(bytes);
        return bytes.length;
    }

    /**
     * 从位图数据字节反序列化删除向量。
     *
     * <p>此方法不包括长度和CRC字段,仅反序列化魔数和位图数据部分。
     *
     * @param bytes 包含魔数和位图数据的字节数组
     * @return 反序列化的删除向量实例
     */
    public static DeletionVector deserializeFromBitmapDataBytes(byte[] bytes) {
        ByteBuffer bitmapData = ByteBuffer.wrap(bytes);
        bitmapData.order(ByteOrder.LITTLE_ENDIAN);
        OptimizedRoaringBitmap64 bitmap = OptimizedRoaringBitmap64.deserialize(bitmapData);
        return new Bitmap64DeletionVector(bitmap);
    }

    /**
     * 计算并验证位图数据的长度(魔数+位图)。
     *
     * <p>此方法确保序列化后的大小不超过整数范围。
     *
     * @param bitmap 要序列化的位图
     * @return 位图数据的字节长度
     * @throws IllegalStateException 如果缓冲区大小超过2GB
     */
    private static int computeBitmapDataLength(OptimizedRoaringBitmap64 bitmap) {
        long length = MAGIC_NUMBER_SIZE_BYTES + bitmap.serializedSizeInBytes();
        long bufferSize = LENGTH_SIZE_BYTES + length + CRC_SIZE_BYTES;
        Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Can't serialize index > 2GB");
        return (int) length;
    }

    /**
     * 将位图数据序列化到字节数组。
     *
     * @param bytes 目标字节数组
     * @param bitmapDataLength 位图数据长度
     * @param bitmap 要序列化的位图
     */
    private static void serializeBitmapData(
            byte[] bytes, int bitmapDataLength, OptimizedRoaringBitmap64 bitmap) {
        ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
        bitmapData.putInt(MAGIC_NUMBER);
        bitmap.serialize(bitmapData);
    }

    /**
     * 获取指向字节数组中位图数据部分的ByteBuffer。
     *
     * @param bytes 完整的字节数组
     * @param bitmapDataLength 位图数据长度
     * @return 指向位图数据的ByteBuffer(小端序)
     */
    private static ByteBuffer pointToBitmapData(byte[] bytes, int bitmapDataLength) {
        ByteBuffer bitmapData = ByteBuffer.wrap(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        bitmapData.order(ByteOrder.LITTLE_ENDIAN);
        return bitmapData;
    }

    /**
     * 读取并验证位图数据长度。
     *
     * <p>检查长度字段是否等于总大小减去长度和CRC字段。
     *
     * @param buffer 包含数据的ByteBuffer
     * @param size 总大小
     * @return 位图数据长度
     * @throws IllegalArgumentException 如果长度不匹配
     */
    private static int readBitmapDataLength(ByteBuffer buffer, int size) {
        int length = buffer.getInt();
        int expectedLength = size - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
        Preconditions.checkArgument(
                length == expectedLength,
                "Invalid bitmap data length: %s, expected %s",
                length,
                expectedLength);
        return length;
    }

    /**
     * 验证魔数并反序列化位图。
     *
     * @param bytes 包含数据的字节数组
     * @param bitmapDataLength 位图数据长度
     * @return 反序列化的位图
     * @throws IllegalArgumentException 如果魔数无效
     */
    private static OptimizedRoaringBitmap64 deserializeBitmap(byte[] bytes, int bitmapDataLength) {
        ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
        int magicNumber = bitmapData.getInt();
        Preconditions.checkArgument(
                magicNumber == MAGIC_NUMBER,
                "Invalid magic number: %s, expected %s",
                magicNumber,
                MAGIC_NUMBER);
        return OptimizedRoaringBitmap64.deserialize(bitmapData);
    }

    /**
     * 为魔数和序列化位图生成32位无符号CRC校验和。
     *
     * @param bytes 包含数据的字节数组
     * @param bitmapDataLength 位图数据长度
     * @return CRC32校验和值
     */
    private static int computeChecksum(byte[] bytes, int bitmapDataLength) {
        CRC32 crc = new CRC32();
        crc.update(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        return (int) crc.getValue();
    }

    /**
     * 将大端序整数转换为小端序。
     *
     * @param bigEndianInt 大端序整数
     * @return 小端序整数
     */
    protected static int toLittleEndianInt(int bigEndianInt) {
        byte[] bytes = ByteBuffer.allocate(4).putInt(bigEndianInt).array();
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Bitmap64DeletionVector that = (Bitmap64DeletionVector) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(roaringBitmap);
    }
}
