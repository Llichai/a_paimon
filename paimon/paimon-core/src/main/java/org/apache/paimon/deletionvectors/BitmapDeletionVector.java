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

import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * 基于{@link RoaringBitmap32}的删除向量实现。
 *
 * <p>此实现使用RoaringBitmap32作为底层数据结构,提供高效的删除标记和查询功能。
 * 由于使用32位整数,它只支持行数不超过{@link RoaringBitmap32#MAX_VALUE}(2^31-1)的文件。
 *
 * <h2>技术特点:</h2>
 * <ul>
 *   <li><b>压缩存储</b>: RoaringBitmap使用混合容器(数组、位图、运行长度编码)实现高效压缩
 *   <li><b>快速操作</b>: 添加、删除、查询操作时间复杂度为O(1)或O(log n)
 *   <li><b>内存高效</b>: 对于稀疏删除场景,内存占用远小于普通位图
 *   <li><b>持久化支持</b>: 内置序列化/反序列化,支持持久化到文件
 * </ul>
 *
 * <h2>存储格式(V1):</h2>
 * <pre>
 * [总长度:4字节][魔数:4字节][RoaringBitmap数据][CRC32:4字节]
 * 魔数: 1581511376 (用于识别V1格式)
 * </pre>
 *
 * <h2>使用限制:</h2>
 * <ul>
 *   <li>仅支持位置范围: 0 到 2,147,483,647
 *   <li>对于更大的文件,应使用{@link Bitmap64DeletionVector}
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建删除向量
 * BitmapDeletionVector dv = new BitmapDeletionVector();
 *
 * // 标记删除
 * dv.delete(100);
 * dv.delete(200);
 *
 * // 检查是否删除
 * boolean deleted = dv.isDeleted(100); // true
 *
 * // 获取删除数量
 * long count = dv.getCardinality(); // 2
 *
 * // 序列化
 * byte[] bytes = DeletionVector.serializeToBytes(dv);
 * }</pre>
 *
 * @see DeletionVector 删除向量接口
 * @see Bitmap64DeletionVector 64位版本,支持更大文件
 * @see RoaringBitmap32 底层位图实现
 */
public class BitmapDeletionVector implements DeletionVector {

    /** 魔数,用于标识V1格式的删除向量 */
    public static final int MAGIC_NUMBER = 1581511376;
    /** 魔数字段的字节大小 */
    public static final int MAGIC_NUMBER_SIZE_BYTES = 4;

    /** 底层RoaringBitmap32实例,存储已删除行的位置 */
    private final RoaringBitmap32 roaringBitmap;

    /** 创建一个空的删除向量 */
    public BitmapDeletionVector() {
        this.roaringBitmap = new RoaringBitmap32();
    }

    /**
     * 使用指定的RoaringBitmap创建删除向量。
     *
     * @param roaringBitmap 已存在的位图实例
     */
    private BitmapDeletionVector(RoaringBitmap32 roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    @Override
    public void delete(long position) {
        checkPosition(position);
        roaringBitmap.add((int) position);
    }

    @Override
    public void merge(DeletionVector deletionVector) {
        if (deletionVector instanceof BitmapDeletionVector) {
            roaringBitmap.or(((BitmapDeletionVector) deletionVector).roaringBitmap);
        } else {
            throw new RuntimeException("Only instance with the same class type can be merged.");
        }
    }

    @Override
    public boolean checkedDelete(long position) {
        checkPosition(position);
        return roaringBitmap.checkedAdd((int) position);
    }

    @Override
    public boolean isDeleted(long position) {
        checkPosition(position);
        return roaringBitmap.contains((int) position);
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    @Override
    public long getCardinality() {
        return roaringBitmap.getCardinality();
    }

    @Override
    public int serializeTo(DataOutputStream out) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(MAGIC_NUMBER);
            roaringBitmap.serialize(dos);
            byte[] data = bos.toByteArray();
            int size = data.length;
            out.writeInt(size);
            out.write(data);
            out.writeInt(calculateChecksum(data));
            return size;
        } catch (Exception e) {
            throw new RuntimeException("Unable to serialize deletion vector", e);
        }
    }

    /**
     * 获取底层的RoaringBitmap实例。
     *
     * <p><b>注意:</b> 返回的位图是只读的,不应在外部调用任何修改操作。
     * 修改返回的位图可能导致删除向量状态不一致。
     *
     * @return 存储删除位置的RoaringBitmap32
     */
    public RoaringBitmap32 get() {
        return roaringBitmap;
    }

    /**
     * 从ByteBuffer反序列化删除向量。
     *
     * @param buffer 包含序列化数据的ByteBuffer
     * @return 反序列化的删除向量实例
     * @throws IOException 如果反序列化失败
     */
    public static DeletionVector deserializeFromByteBuffer(ByteBuffer buffer) throws IOException {
        RoaringBitmap32 bitmap = new RoaringBitmap32();
        bitmap.deserialize(buffer);
        return new BitmapDeletionVector(bitmap);
    }

    /**
     * 检查位置是否在支持的范围内。
     *
     * @param position 要检查的位置
     * @throws IllegalArgumentException 如果位置超过RoaringBitmap32的最大值
     */
    private void checkPosition(long position) {
        if (position > RoaringBitmap32.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The file has too many rows, RoaringBitmap32 only supports files with row count not exceeding 2147483647.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitmapDeletionVector that = (BitmapDeletionVector) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(roaringBitmap);
    }

    /**
     * 计算字节数组的CRC32校验和。
     *
     * @param bytes 要计算校验和的字节数组
     * @return CRC32校验和值
     */
    public static int calculateChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
