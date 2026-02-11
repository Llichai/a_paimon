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

package org.apache.paimon.manifest;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * 带删除向量的简单文件条目
 *
 * <p>SimpleFileEntryWithDV 继承自 {@link SimpleFileEntry}，额外包含删除向量（Deletion Vector）文件名。
 *
 * <p>删除向量（DV）：
 * <ul>
 *   <li>Copy-On-Write 模式下的删除标记机制
 *   <li>记录哪些行被删除，而不是重写整个文件
 *   <li>通过 dvFileName 引用删除向量文件
 * </ul>
 *
 * <p>核心字段：
 * <ul>
 *   <li>继承自 SimpleFileEntry 的所有字段
 *   <li>dvFileName：删除向量文件名（可为 null）
 * </ul>
 *
 * <p>标识符扩展：
 * <ul>
 *   <li>{@link IdentifierWithDv}：扩展的标识符类
 *   <li>包含 dvFileName 字段
 *   <li>确保相同数据文件 + 不同 DV 文件被视为不同条目
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Copy-On-Write 表：记录删除操作的文件
 *   <li>增量删除：通过 DV 文件标记删除的行
 *   <li>避免重写：不需要重写整个数据文件
 * </ul>
 *
 * <p>与 SimpleFileEntry 的区别：
 * <ul>
 *   <li>SimpleFileEntry：不包含 DV 信息
 *   <li>SimpleFileEntryWithDV：包含 DV 文件名
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建带 DV 的文件条目
 * SimpleFileEntry entry = SimpleFileEntry.from(manifestEntry);
 * SimpleFileEntryWithDV withDV = new SimpleFileEntryWithDV(
 *     entry,
 *     "dv-abc123.bin"  // DV 文件名
 * );
 *
 * // 获取 DV 文件名
 * String dvFileName = withDV.dvFileName();
 *
 * // 转换为 DELETE 类型（保留 DV 信息）
 * SimpleFileEntry delete = withDV.toDelete();
 * }</pre>
 */
public class SimpleFileEntryWithDV extends SimpleFileEntry {

    /** 删除向量文件名（可为 null） */
    @Nullable private final String dvFileName;

    /**
     * 构造 SimpleFileEntryWithDV
     *
     * @param entry SimpleFileEntry 实例
     * @param dvFileName 删除向量文件名（可为 null）
     */
    public SimpleFileEntryWithDV(SimpleFileEntry entry, @Nullable String dvFileName) {
        super(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.totalBuckets(),
                entry.level(),
                entry.fileName(),
                entry.extraFiles(),
                entry.embeddedIndex(),
                entry.minKey(),
                entry.maxKey(),
                entry.externalPath(),
                entry.rowCount(),
                entry.firstRowId());
        this.dvFileName = dvFileName;
    }

    /**
     * 获取包含 DV 信息的标识符
     *
     * @return IdentifierWithDv 实例
     */
    public Identifier identifier() {
        return new IdentifierWithDv(super.identifier(), dvFileName);
    }

    /**
     * 获取删除向量文件名
     *
     * @return 删除向量文件名（可为 null）
     */
    @Nullable
    public String dvFileName() {
        return dvFileName;
    }

    /**
     * 转换为 DELETE 类型（保留 DV 信息）
     *
     * @return DELETE 类型的 SimpleFileEntryWithDV
     */
    public SimpleFileEntry toDelete() {
        return new SimpleFileEntryWithDV(super.toDelete(), dvFileName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SimpleFileEntryWithDV that = (SimpleFileEntryWithDV) o;
        return Objects.equals(dvFileName, that.dvFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dvFileName);
    }

    @Override
    public String toString() {
        return super.toString() + ", {dvFileName=" + dvFileName + '}';
    }

    /**
     * 包含删除向量的标识符
     *
     * <p>IdentifierWithDv 扩展了 {@link Identifier}，添加了 dvFileName 字段。
     *
     * <p>相同的 IdentifierWithDv 表示：
     * <ul>
     *   <li>相同的数据文件（partition、bucket、level、fileName 等）
     *   <li>相同的删除向量文件（dvFileName）
     * </ul>
     *
     * <p>用途：
     * <ul>
     *   <li>唯一标识一个数据文件 + DV 文件的组合
     *   <li>在合并条目时区分不同的 DV 版本
     *   <li>确保 DV 文件的正确管理
     * </ul>
     */
    static class IdentifierWithDv extends Identifier {

        /** 删除向量文件名 */
        private final String dvFileName;

        /**
         * 构造 IdentifierWithDv
         *
         * @param identifier 基础标识符
         * @param dvFileName 删除向量文件名
         */
        public IdentifierWithDv(Identifier identifier, String dvFileName) {
            super(
                    identifier.partition,
                    identifier.bucket,
                    identifier.level,
                    identifier.fileName,
                    identifier.extraFiles,
                    identifier.embeddedIndex,
                    identifier.externalPath);
            this.dvFileName = dvFileName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            IdentifierWithDv that = (IdentifierWithDv) o;
            return Objects.equals(dvFileName, that.dvFileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dvFileName);
        }
    }
}
