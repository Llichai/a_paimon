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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * 二进制大对象(Binary Large Object)数据类型。
 *
 * <p>该类型用于存储大型二进制数据,如图片、视频、音频文件等。
 * BLOB 类型通常用于存储超大尺寸的二进制内容,不同于 VARBINARY,
 * BLOB 专门针对大对象进行了优化。
 *
 * <p><b>存储特性:</b>
 * <ul>
 *   <li>默认大小为 1MB (1024 * 1024 字节)</li>
 *   <li>可以存储任意大小的二进制数据</li>
 *   <li>与普通字段分离存储,提高查询性能</li>
 * </ul>
 *
 * <p><b>使用场景:</b> 适用于:
 * <ul>
 *   <li>图片、视频、音频等多媒体文件</li>
 *   <li>大型文档或归档文件</li>
 *   <li>序列化后的大对象</li>
 * </ul>
 *
 * @since 1.4.0
 */
@Public
public final class BlobType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 默认存储大小: 1MB。 */
    public static final int DEFAULT_SIZE = 1024 * 1024;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "BLOB";

    /**
     * 构造一个 Blob 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     */
    public BlobType(boolean isNullable) {
        super(isNullable, DataTypeRoot.BLOB);
    }

    /**
     * 构造一个默认允许 null 的 Blob 类型实例。
     */
    public BlobType() {
        this(true);
    }

    /**
     * 返回该类型的默认存储大小。
     *
     * @return 默认大小 1MB (1024 * 1024 字节)
     */
    @Override
    public int defaultSize() {
        return DEFAULT_SIZE;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 BlobType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new BlobType(isNullable);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "BLOB" 或 "BLOB NOT NULL"(取决于可空性)
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
    }

    /**
     * 接受访问者访问,实现访问者模式。
     *
     * @param visitor 数据类型访问者
     * @param <R> 访问结果类型
     * @return 访问结果
     */
    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    /**
     * 将行类型拆分为普通字段和 BLOB 字段两部分。
     *
     * <p>该方法用于将包含 BLOB 类型的行类型分离为两个独立的行类型:
     * 一个包含所有非 BLOB 字段,另一个包含所有 BLOB 字段。
     * 这种分离可以优化存储和查询性能,因为 BLOB 字段通常较大且访问频率较低。
     *
     * @param rowType 要拆分的行类型
     * @return 一对行类型,第一个包含普通字段,第二个包含 BLOB 字段
     */
    public static Pair<RowType, RowType> splitBlob(RowType rowType) {
        List<DataField> fields = rowType.getFields();
        List<DataField> normalFields = new ArrayList<>();
        List<DataField> blobFields = new ArrayList<>();

        for (DataField field : fields) {
            DataTypeRoot type = field.type().getTypeRoot();
            if (type == DataTypeRoot.BLOB) {
                blobFields.add(field);
            } else {
                normalFields.add(field);
            }
        }

        return Pair.of(new RowType(normalFields), new RowType(blobFields));
    }
}
