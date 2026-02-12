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

package org.apache.paimon.format.variant;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.InferVariantShreddingSchema;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Variant 类型自动推断模式写入器。
 *
 * <p>该写入器在实际写入数据前，会先缓冲一定数量的行，从缓冲的数据中推断出最优的碎片化模式（shredding schema），
 * 然后使用推断出的模式写入所有数据。这种方式可以自动优化 Variant 类型字段的存储结构。
 *
 * <p>工作流程：
 * <ol>
 *   <li>缓冲阶段：接收并缓冲数据行，直到达到配置的阈值
 *   <li>推断阶段：分析缓冲的数据，推断最优的 Variant 碎片化模式
 *   <li>写入阶段：使用推断的模式创建实际写入器，并写入所有数据
 * </ol>
 *
 * <p>该写入器适用于任何实现了 {@link SupportsVariantInference} 接口的文件格式。
 *
 * <p><b>使用场景</b>：
 * <ul>
 *   <li>处理包含 Variant 类型的半结构化数据
 *   <li>需要自动优化 JSON 等动态数据的存储结构
 *   <li>在写入前不确定数据的具体模式
 * </ul>
 *
 * <p><b>性能优化</b>：
 * <ul>
 *   <li>通过推断模式，可以将 Variant 类型拆分为多个具体类型列，提高查询性能
 *   <li>缓冲区大小可配置，在推断准确性和内存占用之间取得平衡
 * </ul>
 */
public class InferVariantShreddingWriter implements BundleFormatWriter {

    /** 支持 Variant 推断的写入器工厂 */
    private final SupportsVariantInference writerFactory;
    /** 数据行类型 */
    private final RowType rowType;
    /** Variant 碎片化模式推断器 */
    private final InferVariantShreddingSchema shreddingSchemaInfer;
    /** 缓冲行的最大数量 */
    private final int maxBufferRow;
    /** 输出流 */
    private final PositionOutputStream out;
    /** 压缩算法 */
    private final String compression;

    /** 缓冲的数据行列表 */
    private final List<InternalRow> bufferedRows;
    /** 缓冲的数据束列表 */
    private final List<BundleRecords> bufferedBundles;

    /** 实际的格式写入器 */
    private FormatWriter actualWriter;
    /** 模式是否已经确定 */
    private boolean schemaFinalized = false;
    /** 缓冲的总行数 */
    private long totalBufferedRowCount = 0;

    /**
     * 构造函数。
     *
     * @param writerFactory 支持 Variant 推断的写入器工厂
     * @param rowType 数据行类型
     * @param shreddingSchemaInfer Variant 碎片化模式推断器
     * @param maxBufferRow 缓冲行的最大数量
     * @param out 输出流
     * @param compression 压缩算法
     */
    public InferVariantShreddingWriter(
            SupportsVariantInference writerFactory,
            RowType rowType,
            InferVariantShreddingSchema shreddingSchemaInfer,
            int maxBufferRow,
            PositionOutputStream out,
            String compression) {
        this.writerFactory = writerFactory;
        this.rowType = rowType;
        this.shreddingSchemaInfer = shreddingSchemaInfer;
        this.maxBufferRow = maxBufferRow;
        this.out = out;
        this.compression = compression;
        this.bufferedRows = new ArrayList<>();
        this.bufferedBundles = new ArrayList<>();
    }

    /**
     * 添加单个数据行。
     *
     * <p>如果模式尚未确定，则缓冲该行；否则直接写入。
     * 当缓冲的行数达到阈值时，会触发模式推断和数据刷新。
     *
     * @param row 要写入的数据行
     * @throws IOException 如果写入失败
     */
    @Override
    public void addElement(InternalRow row) throws IOException {
        if (!schemaFinalized) {
            bufferedRows.add(InternalRowUtils.copyInternalRow(row, rowType));
            totalBufferedRowCount++;
            if (totalBufferedRowCount >= maxBufferRow) {
                finalizeSchemaAndFlush();
            }
        } else {
            actualWriter.addElement(row);
        }
    }

    /**
     * 写入一个数据束。
     *
     * <p>如果模式尚未确定，则缓冲该数据束；否则直接写入。
     * 当缓冲的行数达到阈值时，会触发模式推断和数据刷新。
     *
     * @param bundle 要写入的数据束
     * @throws IOException 如果写入失败
     */
    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        if (!schemaFinalized) {
            final List<InternalRow> rows = new ArrayList<>();
            bundle.forEach(row -> rows.add(InternalRowUtils.copyInternalRow(row, rowType)));
            BundleRecords copiedBundle =
                    new BundleRecords() {
                        @Override
                        @Nonnull
                        public Iterator<InternalRow> iterator() {
                            return rows.iterator();
                        }

                        @Override
                        public long rowCount() {
                            return rows.size();
                        }
                    };
            bufferedBundles.add(copiedBundle);
            totalBufferedRowCount += bundle.rowCount();
            if (totalBufferedRowCount >= maxBufferRow) {
                finalizeSchemaAndFlush();
            }
        } else {
            ((BundleFormatWriter) actualWriter).writeBundle(bundle);
        }
    }

    /**
     * 检查是否达到目标大小。
     *
     * <p>如果模式尚未确定，返回 false（因为还在缓冲阶段）。
     * 否则委托给实际的写入器进行检查。
     *
     * @param suggestedCheck 是否建议进行检查
     * @param targetSize 目标大小（字节）
     * @return 如果已达到目标大小返回 true，否则返回 false
     * @throws IOException 如果检查失败
     */
    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (!schemaFinalized) {
            return false;
        }
        return actualWriter.reachTargetSize(suggestedCheck, targetSize);
    }

    /**
     * 关闭写入器并刷新所有缓冲的数据。
     *
     * <p>如果模式尚未确定，会先触发模式推断和数据刷新。
     * 然后关闭实际的写入器。
     *
     * @throws IOException 如果关闭或刷新失败
     */
    @Override
    public void close() throws IOException {
        try {
            if (!schemaFinalized) {
                finalizeSchemaAndFlush();
            }
        } finally {
            if (actualWriter != null) {
                actualWriter.close();
            }
        }
    }

    /**
     * 完成模式推断并刷新所有缓冲的数据。
     *
     * <p>该方法执行以下步骤：
     * <ol>
     *   <li>从缓冲的数据中推断最优的 Variant 碎片化模式
     *   <li>使用推断的模式创建实际的写入器
     *   <li>将所有缓冲的数据写入实际的写入器
     * </ol>
     *
     * @throws IOException 如果推断或写入失败
     */
    private void finalizeSchemaAndFlush() throws IOException {
        RowType inferredShreddingSchema = shreddingSchemaInfer.inferSchema(collectAllRows());
        actualWriter =
                writerFactory.createWithShreddingSchema(out, compression, inferredShreddingSchema);
        schemaFinalized = true;

        if (!bufferedBundles.isEmpty()) {
            BundleFormatWriter bundleWriter = (BundleFormatWriter) actualWriter;
            for (BundleRecords bundle : bufferedBundles) {
                bundleWriter.writeBundle(bundle);
            }
            bufferedBundles.clear();
        } else {
            for (InternalRow row : bufferedRows) {
                actualWriter.addElement(row);
            }
            bufferedRows.clear();
        }
    }

    /**
     * 收集所有缓冲的数据行。
     *
     * <p>如果使用了数据束缓冲，则从所有数据束中提取行；
     * 否则直接返回缓冲的行列表。
     *
     * @return 所有缓冲的数据行列表
     */
    private List<InternalRow> collectAllRows() {
        if (!bufferedBundles.isEmpty()) {
            List<InternalRow> allRows = new ArrayList<>();
            for (BundleRecords bundle : bufferedBundles) {
                for (InternalRow row : bundle) {
                    allRows.add(row);
                }
            }
            return allRows;
        } else {
            return bufferedRows;
        }
    }
}
