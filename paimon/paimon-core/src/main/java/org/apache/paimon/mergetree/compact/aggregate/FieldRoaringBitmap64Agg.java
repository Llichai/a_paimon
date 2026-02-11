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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.RoaringBitmap64;

import java.io.IOException;

/**
 * ROARING_BITMAP_64 聚合器
 * 使用Roaring Bitmap进行64位长整数集合的压缩存储和聚合
 * 支持更大范围的整数值（64位），适用于需要存储大整数ID的场景
 */
public class FieldRoaringBitmap64Agg extends FieldAggregator {

    private static final long serialVersionUID = 1L;
    private final RoaringBitmap64 roaringBitmapAcc; // 累加器位图（可复用对象）
    private final RoaringBitmap64 roaringBitmapInput; // 输入位图（可复用对象）

    /**
     * 构造 ROARING_BITMAP_64 聚合器
     * @param name 聚合函数名称
     * @param dataType 二进制数据类型（Roaring Bitmap序列化后的字节数组）
     */
    public FieldRoaringBitmap64Agg(String name, VarBinaryType dataType) {
        super(name, dataType);
        this.roaringBitmapAcc = new RoaringBitmap64(); // 创建可复用的累加器位图对象
        this.roaringBitmapInput = new RoaringBitmap64(); // 创建可复用的输入位图对象
    }

    /**
     * 执行 ROARING_BITMAP_64 聚合
     * 合并两个位图（OR操作），得到所有长整数的并集
     * @param accumulator 累加器（已有的位图字节数组）
     * @param inputField 输入字段（新的位图字节数组）
     * @return 合并后的位图字节数组
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有一个为null，返回非null的那个
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        try {
            // 反序列化累加器和输入位图
            roaringBitmapAcc.deserialize((byte[]) accumulator);
            roaringBitmapInput.deserialize((byte[]) inputField);
            // 执行OR操作，合并两个位图
            roaringBitmapAcc.or(roaringBitmapInput);
            // 序列化合并后的位图
            return roaringBitmapAcc.serialize();
        } catch (IOException e) {
            throw new RuntimeException("Unable to se/deserialize roaring bitmap.", e);
        } finally {
            // 清空可复用对象，准备下次使用
            roaringBitmapAcc.clear();
            roaringBitmapInput.clear();
        }
    }
}
