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

package org.apache.paimon.disk;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * 通道读取器输入视图迭代器
 *
 * <p>通过 I/O 通道读取输入的简单迭代器，用于遍历从文件中读取的 BinaryRow 数据。
 *
 * <p>核心功能：
 * <ul>
 *   <li>顺序读取：按顺序从通道输入视图读取二进制行
 *   <li>EOF 处理：到达文件末尾时自动关闭视图并释放内存
 *   <li>内存回收：支持将释放的内存段返回到目标列表
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>读取行：使用序列化器从输入视图反序列化 BinaryRow
 *   <li>EOF 检测：捕获 EOFException 表示文件结束
 *   <li>资源清理：关闭输入视图并回收内存段
 *   <li>返回结果：返回 null 表示迭代结束
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>外部排序：读取排序过程中溢写的数据
 *   <li>批量读取：从临时文件批量读取行数据
 * </ul>
 *
 * <p>设计思路：
 * <ul>
 *   <li>简化接口：封装 ChannelReaderInputView 的复杂性
 *   <li>自动管理：自动处理 EOF 和资源释放
 *   <li>内存回收：可选的内存段回收机制
 * </ul>
 *
 * @see ChannelReaderInputView
 * @see MutableObjectIterator
 */
public class ChannelReaderInputViewIterator implements MutableObjectIterator<BinaryRow> {
    /** 通道读取器输入视图 */
    private final ChannelReaderInputView inView;

    /** 二进制行序列化器（访问器） */
    private final BinaryRowSerializer accessors;

    /** 释放的内存段目标列表（可选） */
    private final List<MemorySegment> freeMemTarget;

    /**
     * 构造通道读取器输入视图迭代器
     *
     * @param inView 通道读取器输入视图
     * @param freeMemTarget 释放的内存段目标列表（可为 null）
     * @param accessors 二进制行序列化器
     */
    public ChannelReaderInputViewIterator(
            ChannelReaderInputView inView,
            List<MemorySegment> freeMemTarget,
            BinaryRowSerializer accessors) {
        this.inView = inView;
        this.freeMemTarget = freeMemTarget;
        this.accessors = accessors;
    }

    /**
     * 读取下一行（复用对象）
     *
     * <p>从输入视图反序列化下一个 BinaryRow。到达文件末尾时关闭视图并回收内存。
     *
     * @param reuse 复用的行对象
     * @return 下一行，如果到达末尾则返回 null
     * @throws IOException IO 异常
     */
    @Override
    public BinaryRow next(BinaryRow reuse) throws IOException {
        try {
            // 反序列化行（复用对象）
            return this.accessors.deserialize(reuse, this.inView);
        } catch (EOFException eofex) {
            // 到达文件末尾，关闭并释放内存
            final List<MemorySegment> freeMem = this.inView.close();
            if (this.freeMemTarget != null) {
                this.freeMemTarget.addAll(freeMem);
            }
            return null;
        }
    }

    /**
     * 读取下一行（创建新对象）
     *
     * <p>从输入视图反序列化下一个 BinaryRow（创建新对象）。到达文件末尾时关闭视图并回收内存。
     *
     * @return 下一行，如果到达末尾则返回 null
     * @throws IOException IO 异常
     */
    @Override
    public BinaryRow next() throws IOException {
        try {
            // 反序列化行（创建新对象）
            return this.accessors.deserialize(this.inView);
        } catch (EOFException eofex) {
            // 到达文件末尾，关闭并释放内存
            final List<MemorySegment> freeMem = this.inView.close();
            if (this.freeMemTarget != null) {
                this.freeMemTarget.addAll(freeMem);
            }
            return null;
        }
    }
}
