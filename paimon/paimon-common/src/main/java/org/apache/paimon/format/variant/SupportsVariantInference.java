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

import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import java.io.IOException;

/**
 * 支持 Variant 类型模式推断的格式写入器工厂接口。
 *
 * <p>实现该接口的格式写入器工厂可以基于推断的 Variant 碎片化模式动态更新其模式。
 * 这允许写入器在处理半结构化数据时，自动优化存储结构。
 *
 * <p><b>使用场景</b>：
 * <ul>
 *   <li>处理包含 JSON 或其他动态类型数据的 Variant 字段
 *   <li>自动将 Variant 类型拆分为多个具体类型列，提高查询性能
 *   <li>在写入前根据实际数据推断最优的存储模式
 * </ul>
 *
 * <p><b>实现要求</b>：
 * <ul>
 *   <li>必须支持使用推断的碎片化模式创建新的写入器
 *   <li>必须保持与原始配置（压缩、输出流等）的兼容性
 * </ul>
 */
public interface SupportsVariantInference {

    /**
     * 使用推断的碎片化模式创建写入器。
     *
     * <p>该方法使用相同的输出流和压缩设置，但应用推断的 Variant 碎片化模式。
     * 碎片化模式定义了如何将 Variant 类型字段拆分为多个具体类型的列。
     *
     * @param out 输出流
     * @param compression 压缩编解码器
     * @param inferredShreddingSchema 推断的 Variant 碎片化模式
     * @return 配置了推断模式的新格式写入器
     * @throws IOException 如果无法创建写入器
     */
    FormatWriter createWithShreddingSchema(
            PositionOutputStream out, String compression, RowType inferredShreddingSchema)
            throws IOException;
}
