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

package org.apache.paimon.data.columnar;

import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.VectorizedRecordIterator;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 向量化行迭代器,扩展 {@link ColumnarRowIterator} 并实现 {@link VectorizedRecordIterator}。
 *
 * <p>此迭代器除了支持逐行访问外,还提供了对整个 {@link VectorizedColumnBatch} 的访问,
 * 适用于需要批量处理数据的向量化算子。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>向量化执行引擎的数据迭代
 *   <li>需要同时支持行访问和批访问的场景
 *   <li>批量处理优化
 * </ul>
 *
 * @see ColumnarRowIterator 父类迭代器
 * @see VectorizedRecordIterator 向量化记录迭代器接口
 */
public class VectorizedRowIterator extends ColumnarRowIterator implements VectorizedRecordIterator {

    public VectorizedRowIterator(Path filePath, ColumnarRow row, @Nullable Runnable recycler) {
        super(filePath, row, recycler);
    }

    @Override
    public VectorizedColumnBatch batch() {
        return row.batch();
    }

    @Override
    protected VectorizedRowIterator copy(ColumnVector[] vectors) {
        checkArgument(returnedPositionIndex == 0, "copy() should not be called after next()");
        VectorizedRowIterator newIterator =
                new VectorizedRowIterator(filePath, row.copy(vectors), recycler);
        newIterator.reset(positionIterator);
        return newIterator;
    }
}
