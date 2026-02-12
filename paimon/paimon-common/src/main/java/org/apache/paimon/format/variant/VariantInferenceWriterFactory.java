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
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;

/**
 * Variant 模式推断装饰器工厂。
 *
 * <p>该工厂为任何 {@link FormatWriterFactory} 添加 Variant 模式推断能力。
 * 它是一个装饰器模式的实现，包装现有的格式写入器工厂，并在满足条件时自动启用 Variant 模式推断。
 *
 * <p><b>工作原理</b>：
 * <ol>
 *   <li>检查配置是否启用了 Variant 模式推断
 *   <li>检查被装饰的工厂是否支持 Variant 推断（实现 {@link SupportsVariantInference}）
 *   <li>如果满足条件，返回 {@link InferVariantShreddingWriter} 包装的写入器
 *   <li>否则直接使用被装饰工厂创建标准写入器
 * </ol>
 *
 * <p><b>使用场景</b>：
 * <ul>
 *   <li>为 Parquet、ORC 等格式添加 Variant 推断功能
 *   <li>在不修改原有格式实现的情况下扩展功能
 *   <li>根据配置动态决定是否启用推断
 * </ul>
 *
 * <p><b>示例</b>：
 * <pre>{@code
 * // 创建基础的 Parquet 写入器工厂
 * FormatWriterFactory parquetFactory = new ParquetWriterFactory(...);
 *
 * // 包装为支持 Variant 推断的工厂
 * VariantInferenceConfig config = new VariantInferenceConfig(rowType, options);
 * FormatWriterFactory inferenceFactory =
 *     new VariantInferenceWriterFactory(parquetFactory, config);
 *
 * // 创建写入器（会根据配置自动决定是否启用推断）
 * FormatWriter writer = inferenceFactory.create(out, "snappy");
 * }</pre>
 */
public class VariantInferenceWriterFactory implements FormatWriterFactory {

    /** 被装饰的格式写入器工厂 */
    private final FormatWriterFactory delegate;
    /** Variant 推断配置 */
    private final VariantInferenceConfig config;

    /**
     * 构造函数。
     *
     * @param delegate 被装饰的格式写入器工厂
     * @param config Variant 推断配置
     */
    public VariantInferenceWriterFactory(
            FormatWriterFactory delegate, VariantInferenceConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    /**
     * 创建格式写入器。
     *
     * <p>根据配置和被装饰工厂的能力，决定返回标准写入器还是推断写入器：
     * <ul>
     *   <li>如果配置未启用推断，返回标准写入器
     *   <li>如果被装饰工厂不支持推断，返回标准写入器
     *   <li>否则返回 {@link InferVariantShreddingWriter} 包装的推断写入器
     * </ul>
     *
     * @param out 输出流
     * @param compression 压缩算法
     * @return 格式写入器（可能包含推断功能）
     * @throws IOException 如果创建写入器失败
     */
    @Override
    public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
        if (!config.shouldEnableInference()) {
            return delegate.create(out, compression);
        }

        if (!(delegate instanceof SupportsVariantInference)) {
            return delegate.create(out, compression);
        }

        return new InferVariantShreddingWriter(
                (SupportsVariantInference) delegate,
                config.rowType(),
                config.createInferrer(),
                config.getMaxBufferRow(),
                out,
                compression);
    }
}
