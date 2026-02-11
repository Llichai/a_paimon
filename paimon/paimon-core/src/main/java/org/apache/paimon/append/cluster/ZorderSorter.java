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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.zorder.ZIndexer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Z-Order 曲线排序器
 *
 * <p>ZorderSorter 使用 Z-Order 曲线对多维数据进行空间排序,
 * 通过交错维度的比特位实现空间映射。
 *
 * <p>Z-Order 曲线特点:
 * <ul>
 *   <li>计算效率高:只需要比特交错操作
 *   <li>空间局部性好:相邻点大概率在曲线上接近
 *   <li>存在长跳跃:某些情况下会出现大跨度跳跃
 * </ul>
 *
 * <p>排序键格式:
 * Z-Order 索引为 BYTES 类型:
 * <pre>
 * 输入: row(region="Asia", city="Beijing")
 * 计算: zorderIndex(row) → byte[16]
 * 输出: JoinedRow(byte[16], row)
 * </pre>
 *
 * @see ZIndexer Z-Order 索引器
 * @see Sorter 排序器基类
 */
public class ZorderSorter extends Sorter {

    private static final RowType KEY_TYPE =
            new RowType(Collections.singletonList(new DataField(0, "Z_INDEX", DataTypes.BYTES())));

    private final ZorderKeyAbstract zorderKeyAbstract;

    public ZorderSorter(
            RecordReaderIterator<InternalRow> reader,
            RowType valueType,
            CoreOptions options,
            List<String> orderColNames,
            IOManager ioManager) {
        super(reader, KEY_TYPE, valueType, options, ioManager);
        this.zorderKeyAbstract = new ZorderKeyAbstract(valueType, options, orderColNames);
        this.zorderKeyAbstract.open();
    }

    @Override
    public InternalRow assignSortKey(InternalRow row) {
        byte[] key = zorderKeyAbstract.apply(row);
        return new JoinedRow(GenericRow.of(key), row);
    }

    private static class ZorderKeyAbstract implements KeyAbstract<byte[]> {

        private final ZIndexer zIndexer;

        public ZorderKeyAbstract(RowType rowType, CoreOptions options, List<String> orderColNames) {
            zIndexer = new ZIndexer(rowType, orderColNames, options.varTypeSize());
        }

        @Override
        public void open() {
            zIndexer.open();
        }

        @Override
        public byte[] apply(InternalRow value) {
            byte[] zorder = zIndexer.index(value);
            return Arrays.copyOf(zorder, zorder.length);
        }
    }
}
