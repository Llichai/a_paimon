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

package org.apache.paimon.operation;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 分片读取接口 - 提供从 Split 创建 RecordReader 的能力
 *
 * <p><b>什么是 Split？</b>
 * <ul>
 *   <li>Split 是 Paimon 表的读取单元，类似于 Flink 的 InputSplit
 *   <li>一个 Split 通常对应一个分区的一个 bucket 的一批数据文件
 *   <li>分布式读取时，每个 Task 读取一个或多个 Split
 * </ul>
 *
 * <p><b>SplitRead 的职责</b>：
 * <ol>
 *   <li>接收 Split 对象（包含文件列表、过滤条件等元信息）
 *   <li>创建对应的 RecordReader（实际执行读取的对象）
 *   <li>应用过滤器、投影、TopN 等优化
 * </ol>
 *
 * <p><b>主要方法</b>：
 * <ul>
 *   <li>{@link #withReadType}：设置投影（只读取部分列）
 *   <li>{@link #withFilter}：设置过滤条件（谓词下推）
 *   <li>{@link #withTopN}：设置 TopN（读取前 N 条记录）
 *   <li>{@link #withLimit}：设置 Limit（读取最多 N 条记录）
 *   <li>{@link #forceKeepDelete}：强制保留删除标记（用于 CDC）
 *   <li>{@link #withIOManager}：设置 IO 管理器（用于外部排序）
 *   <li>{@link #createReader}：根据 Split 创建 RecordReader
 * </ul>
 *
 * <p><b>实现类</b>：
 * <ul>
 *   <li>{@link MergeFileSplitRead}：用于主键表（KeyValue表），处理 LSM 合并
 *   <li>{@link RawFileSplitRead}：用于 Append-Only 表，直接读取原始文件
 * </ul>
 *
 * <p><b>投影下推（Projection Pushdown）</b>：
 * <pre>
 * 查询：SELECT id, name FROM t WHERE age > 18
 * - withReadType(RowType{id, name})：只读取 id 和 name 列
 * - 文件格式（如 Parquet）可以跳过其他列的读取
 * - 减少 IO 和反序列化开销
 * </pre>
 *
 * <p><b>过滤器下推（Filter Pushdown）</b>：
 * <pre>
 * 查询：SELECT * FROM t WHERE age > 18 AND city = 'Beijing'
 * - withFilter(Predicate{age > 18 AND city = 'Beijing'})
 * - 在读取时应用过滤条件，减少数据传输
 * - 支持的过滤器：
 *   - 列式格式：可以直接在文件层面过滤（ORC/Parquet）
 *   - 行式格式：在内存中过滤
 * </pre>
 *
 * <p><b>TopN 和 Limit</b>：
 * <ul>
 *   <li>TopN：读取排序后的前 N 条（需要排序键）
 *   <li>Limit：读取最多 N 条（不需要排序）
 *   <li>优化：提前停止读取，避免扫描所有数据
 * </ul>
 *
 * <p><b>forceKeepDelete（保留删除标记）</b>：
 * <ul>
 *   <li>主键表中，删除操作会生成 RowKind.DELETE 记录
 *   <li>普通查询：自动过滤 DELETE 记录
 *   <li>CDC 查询：需要保留 DELETE 记录，用于下游系统同步删除
 * </ul>
 *
 * <p><b>convert 工具方法</b>：
 * <ul>
 *   <li>用于类型转换（如 KeyValue -> InternalRow）
 *   <li>将一个 SplitRead&lt;L&gt; 转换为 SplitRead&lt;R&gt;
 *   <li>使用场景：
 *       <pre>
 *       MergeFileSplitRead&lt;KeyValue&gt; kvRead = ...;
 *       SplitRead&lt;InternalRow&gt; rowRead = SplitRead.convert(
 *           kvRead,
 *           split -> new KeyValueToRowReader(kvRead.createReader(split))
 *       );
 *       </pre>
 *   </li>
 * </ul>
 *
 * @param <T> 记录类型（KeyValue 或 InternalRow）
 * @see MergeFileSplitRead 主键表读取
 * @see RawFileSplitRead Append-Only 表读取
 * @see org.apache.paimon.reader.RecordReader 记录读取器
 */
public interface SplitRead<T> {

    SplitRead<T> forceKeepDelete();

    SplitRead<T> withIOManager(@Nullable IOManager ioManager);

    SplitRead<T> withReadType(RowType readType);

    SplitRead<T> withFilter(@Nullable Predicate predicate);

    default SplitRead<T> withTopN(@Nullable TopN topN) {
        return this;
    }

    default SplitRead<T> withLimit(@Nullable Integer limit) {
        return this;
    }

    /** Create a {@link RecordReader} from split. */
    RecordReader<T> createReader(Split split) throws IOException;

    static <L, R> SplitRead<R> convert(
            SplitRead<L> read, IOFunction<Split, RecordReader<R>> splitConvert) {
        return new SplitRead<R>() {
            @Override
            public SplitRead<R> forceKeepDelete() {
                read.forceKeepDelete();
                return this;
            }

            @Override
            public SplitRead<R> withIOManager(@Nullable IOManager ioManager) {
                read.withIOManager(ioManager);
                return this;
            }

            @Override
            public SplitRead<R> withReadType(RowType readType) {
                read.withReadType(readType);
                return this;
            }

            @Override
            public SplitRead<R> withFilter(@Nullable Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

            @Override
            public RecordReader<R> createReader(Split split) throws IOException {
                return splitConvert.apply(split);
            }
        };
    }
}
