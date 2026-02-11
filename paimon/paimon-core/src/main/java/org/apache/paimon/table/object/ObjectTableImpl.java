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

package org.apache.paimon.table.object;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.paimon.data.BinaryString.fromString;

/**
 * {@link ObjectTable} 接口的实现类。
 *
 * <p>该实现类提供了对象表在 Paimon 系统中的完整功能,包括元数据管理、文件扫描和读取。
 * 与 IcebergTable 和 LanceTable 不同,ObjectTable 是完全可读的,支持扫描和读取操作。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>完整读取支持</b>: 实现了 {@link #newScan()} 和 {@link #newRead()},可以实际读取文件元数据</li>
 *   <li><b>只读表</b>: 实现 {@link ReadonlyTable} 接口,不支持写入操作</li>
 *   <li><b>递归扫描</b>: 递归遍历目录,索引所有子目录中的文件</li>
 *   <li><b>固定 Schema</b>: 使用 {@link ObjectTable#SCHEMA} 定义的固定结构</li>
 *   <li><b>投影支持</b>: 支持字段投影,只读取需要的元数据字段</li>
 * </ul>
 *
 * <h3>内部组件</h3>
 * <p>该类包含三个内部类,协同实现对象表的功能:
 * <ul>
 *   <li><b>{@link ObjectScan}</b>: 扫描器,生成 ObjectSplit</li>
 *   <li><b>{@link ObjectSplit}</b>: 分片,包含目录位置信息</li>
 *   <li><b>{@link ObjectRead}</b>: 读取器,遍历目录并将文件转换为行</li>
 * </ul>
 *
 * <h3>工作流程</h3>
 * <ol>
 *   <li><b>扫描阶段</b>: {@link ObjectScan} 创建一个包含 location 信息的 {@link ObjectSplit}</li>
 *   <li><b>读取阶段</b>: {@link ObjectRead} 使用 FileIO 递归列出所有文件</li>
 *   <li><b>转换阶段</b>: 每个 {@link org.apache.paimon.fs.FileStatus} 被转换为一行数据</li>
 *   <li><b>投影阶段</b>: 如果设置了投影,只返回指定的字段</li>
 * </ol>
 *
 * <h3>字段说明</h3>
 * <ul>
 *   <li><b>identifier</b>: 表的唯一标识符</li>
 *   <li><b>fileIO</b>: 文件 I/O 接口,用于列出和访问文件</li>
 *   <li><b>location</b>: 要监控的目录位置</li>
 *   <li><b>comment</b>: 表的注释(可选)</li>
 * </ul>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li>实现 {@link ObjectTable} 接口,提供对象表的标准功能</li>
 *   <li>实现 {@link ReadonlyTable} 接口,标识为只读表</li>
 *   <li>使用 {@link org.apache.paimon.fs.RemoteIterator} 进行延迟加载的文件遍历</li>
 *   <li>使用 {@link org.apache.paimon.utils.ProjectedRow} 实现字段投影</li>
 *   <li>使用 {@link org.apache.paimon.utils.IteratorRecordReader} 包装迭代器为读取器</li>
 * </ul>
 *
 * <h3>性能特性</h3>
 * <ul>
 *   <li><b>延迟加载</b>: 使用迭代器模式,不会一次性加载所有文件元数据</li>
 *   <li><b>流式处理</b>: 边扫描边返回,适合大目录</li>
 *   <li><b>投影优化</b>: 支持字段投影,减少数据传输</li>
 *   <li><b>递归扫描</b>: 自动递归子目录,无需手动遍历</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 创建对象表
 * ObjectTable table = new ObjectTableImpl(
 *     identifier, fileIO, "hdfs://cluster/data/", "Data files");
 *
 * // 扫描并读取
 * InnerTableScan scan = table.newScan();
 * InnerTableRead read = table.newRead()
 *     .withReadType(ObjectTable.SCHEMA.project(new int[]{0, 1, 2})); // 只读 path, name, length
 *
 * for (Split split : scan.plan().splits()) {
 *     try (RecordReader<InternalRow> reader = read.createReader(split)) {
 *         RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
 *         while ((row = iterator.next()) != null) {
 *             // 处理文件元数据...
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see ObjectTable 该类实现的接口
 * @see ReadonlyTable 只读表标记接口
 * @see org.apache.paimon.table.source.ReadOnceTableScan 单次扫描基类
 */
public class ObjectTableImpl implements ReadonlyTable, ObjectTable {

    /** 表的标识符,包含数据库名和表名。 */
    private final Identifier identifier;

    /** 文件 I/O 接口,用于列出目录和访问文件元数据。 */
    private final FileIO fileIO;

    /** 对象目录在文件系统中的位置路径。 */
    private final String location;

    /** 表的注释说明(可选)。 */
    @Nullable private final String comment;

    /**
     * 构造一个 ObjectTableImpl 实例。
     *
     * @param identifier 表的标识符
     * @param fileIO 文件 I/O 接口
     * @param location 对象目录的位置路径
     * @param comment 表的注释说明(可选)
     */
    public ObjectTableImpl(
            Identifier identifier, FileIO fileIO, String location, @Nullable String comment) {
        this.identifier = identifier;
        this.fileIO = fileIO;
        this.location = location;
        this.comment = comment;
    }

    /**
     * 获取表名。
     *
     * @return 返回表名(不包含数据库名)
     */
    @Override
    public String name() {
        return identifier.getTableName();
    }

    /**
     * 获取完整表名。
     *
     * @return 返回完整表名(格式: database.table)
     */
    @Override
    public String fullName() {
        return identifier.getFullName();
    }

    /**
     * 获取表的行类型。
     *
     * <p>对象表使用固定的 {@link ObjectTable#SCHEMA}。
     *
     * @return 返回固定的对象表 Schema
     */
    @Override
    public RowType rowType() {
        return SCHEMA;
    }

    /**
     * 获取分区键列表。
     *
     * <p>对象表不支持分区。
     *
     * @return 返回空列表
     */
    @Override
    public List<String> partitionKeys() {
        return Collections.emptyList();
    }

    /**
     * 获取主键列表。
     *
     * <p>对象表不支持主键。
     *
     * @return 返回空列表
     */
    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    /**
     * 获取表的配置选项。
     *
     * <p>对象表没有配置选项。
     *
     * @return 返回空映射
     */
    @Override
    public Map<String, String> options() {
        return Collections.emptyMap();
    }

    /**
     * 获取表的注释。
     *
     * @return 返回包含注释的 Optional,如果没有注释则返回空 Optional
     */
    @Override
    public Optional<String> comment() {
        return Optional.ofNullable(comment);
    }

    /**
     * 获取表的统计信息。
     *
     * <p>当前返回默认的统计信息(通常为空)。
     *
     * @return 返回包含统计信息的 Optional
     */
    @Override
    public Optional<Statistics> statistics() {
        return ReadonlyTable.super.statistics();
    }

    /**
     * 获取文件 I/O 接口。
     *
     * @return 返回文件 I/O 实现
     */
    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    /**
     * 获取对象目录的位置路径。
     *
     * @return 返回对象目录在文件系统中的路径
     */
    @Override
    public String location() {
        return location;
    }

    /**
     * 创建带有动态选项的表副本。
     *
     * <p>对于对象表,该方法只是返回一个相同配置的新实例,因为对象表没有配置选项。
     *
     * @param dynamicOptions 动态选项映射(对对象表无实际影响)
     * @return 返回新的 ObjectTable 实例
     */
    @Override
    public ObjectTable copy(Map<String, String> dynamicOptions) {
        return new ObjectTableImpl(identifier, fileIO, location, comment);
    }

    /**
     * 创建表扫描器。
     *
     * <p>返回一个 {@link ObjectScan} 实例,用于生成扫描计划。
     * 对象表的扫描计划非常简单,只包含一个 {@link ObjectSplit}。
     *
     * @return 返回 ObjectScan 实例
     */
    @Override
    public InnerTableScan newScan() {
        return new ObjectScan(this);
    }

    /**
     * 创建表读取器。
     *
     * <p>返回一个 {@link ObjectRead} 实例,用于读取文件元数据。
     *
     * @return 返回 ObjectRead 实例
     */
    @Override
    public InnerTableRead newRead() {
        return new ObjectRead();
    }

    /**
     * 对象表扫描器实现。
     *
     * <p>该扫描器非常简单,只生成一个包含整个目录位置的 Split。
     * 实际的文件遍历发生在读取阶段,而不是扫描阶段。
     *
     * <h3>设计说明</h3>
     * <p>对象表采用"延迟扫描"策略:
     * <ul>
     *   <li>扫描阶段不实际访问文件系统</li>
     *   <li>只创建一个逻辑 Split,包含目录位置</li>
     *   <li>读取阶段才真正列出文件</li>
     * </ul>
     *
     * <p>这种设计的好处:
     * <ul>
     *   <li>扫描阶段非常快,不阻塞作业启动</li>
     *   <li>适合大目录,避免一次性列出所有文件</li>
     *   <li>支持流式处理,边列文件边返回</li>
     * </ul>
     *
     * @see org.apache.paimon.table.source.ReadOnceTableScan 单次扫描基类
     */
    private static class ObjectScan extends ReadOnceTableScan {

        /** 要扫描的对象表。 */
        private final ObjectTable objectTable;

        /**
         * 构造 ObjectScan 实例。
         *
         * @param objectTable 对象表实例
         */
        public ObjectScan(ObjectTable objectTable) {
            this.objectTable = objectTable;
        }

        /**
         * 设置过滤谓词。
         *
         * <p>对象表当前不支持谓词下推,该方法返回 this 以保持链式调用。
         *
         * @param predicate 过滤谓词(当前被忽略)
         * @return 返回当前扫描器实例
         */
        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        /**
         * 生成扫描计划。
         *
         * <p>返回包含单个 {@link ObjectSplit} 的计划,该 Split 封装了目录位置信息。
         *
         * @return 返回包含一个 Split 的扫描计划
         */
        @Override
        public Plan innerPlan() {
            return () ->
                    Collections.singletonList(
                            new ObjectSplit(objectTable.fileIO(), objectTable.location()));
        }
    }

    /**
     * 对象表的 Split 实现。
     *
     * <p>该 Split 代表一个要扫描的目录,包含 FileIO 和目录位置信息。
     * 继承 {@link SingletonSplit} 表示这是一个不可再分割的单例 Split。
     *
     * <h3>设计说明</h3>
     * <p>ObjectSplit 只是一个轻量级的元数据容器,不包含实际的文件列表。
     * 文件列表的生成推迟到读取阶段,由 {@link ObjectRead} 负责。
     *
     * @see SingletonSplit 单例 Split 基类
     */
    private static class ObjectSplit extends SingletonSplit {

        /** 序列化版本 UID。 */
        private static final long serialVersionUID = 1L;

        /** 文件 I/O 接口,用于访问文件系统。 */
        private final FileIO fileIO;

        /** 对象目录的位置路径。 */
        private final String location;

        /**
         * 构造 ObjectSplit 实例。
         *
         * @param fileIO 文件 I/O 接口
         * @param location 目录位置
         */
        public ObjectSplit(FileIO fileIO, String location) {
            this.fileIO = fileIO;
            this.location = location;
        }

        /**
         * 获取文件 I/O 接口。
         *
         * @return 返回 FileIO 实例
         */
        public FileIO fileIO() {
            return fileIO;
        }

        /**
         * 获取目录位置。
         *
         * @return 返回目录路径
         */
        public String location() {
            return location;
        }

        /**
         * 判断两个 Split 是否相等。
         *
         * <p>基于 location 判断相等性。
         *
         * @param o 另一个对象
         * @return 如果 location 相同则返回 true
         */
        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ObjectSplit that = (ObjectSplit) o;
            return Objects.equals(location, that.location);
        }

        /**
         * 计算哈希码。
         *
         * <p>基于 location 计算哈希码。
         *
         * @return 返回哈希码
         */
        @Override
        public int hashCode() {
            return Objects.hash(location);
        }

        /**
         * 获取合并后的行数。
         *
         * <p>对象表无法预先知道行数,返回空。
         *
         * @return 返回空 OptionalLong
         */
        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    /**
     * 对象表的读取器实现。
     *
     * <p>该读取器负责遍历目录,将每个文件的元数据转换为一行数据。
     * 支持字段投影和过滤(过滤功能待实现)。
     *
     * <h3>核心功能</h3>
     * <ul>
     *   <li><b>递归遍历</b>: 使用 {@link FileIO#listFilesIterative} 递归列出所有文件</li>
     *   <li><b>延迟加载</b>: 使用迭代器模式,不会一次性加载所有文件</li>
     *   <li><b>元数据转换</b>: 将 {@link org.apache.paimon.fs.FileStatus} 转换为行</li>
     *   <li><b>字段投影</b>: 支持只读取指定的字段</li>
     * </ul>
     *
     * <h3>工作流程</h3>
     * <ol>
     *   <li>从 Split 中获取 FileIO 和 location</li>
     *   <li>调用 {@link FileIO#listFilesIterative} 获取文件迭代器</li>
     *   <li>为每个文件调用 {@link #toRow} 方法转换为行</li>
     *   <li>如果设置了投影,应用 {@link org.apache.paimon.utils.ProjectedRow}</li>
     *   <li>返回 {@link org.apache.paimon.utils.IteratorRecordReader}</li>
     * </ol>
     *
     * @see InnerTableRead 表读取器接口
     */
    private static class ObjectRead implements InnerTableRead {

        /** 读取类型,用于字段投影(可选)。 */
        private @Nullable RowType readType;

        /**
         * 设置过滤谓词。
         *
         * <p>TODO: 未来可以实现基于文件属性的过滤,如大小、修改时间等。
         *
         * @param predicate 过滤谓词(当前被忽略)
         * @return 返回当前读取器实例
         */
        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        /**
         * 设置读取类型(字段投影)。
         *
         * <p>通过投影可以只读取需要的字段,减少数据传输量。
         * 例如,如果只需要 path 和 length,可以只投影这两个字段。
         *
         * @param readType 投影后的行类型
         * @return 返回当前读取器实例
         */
        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        /**
         * 设置 I/O 管理器。
         *
         * <p>对象表读取器不需要 I/O 管理器,该方法返回 this 以保持链式调用。
         *
         * @param ioManager I/O 管理器(未使用)
         * @return 返回当前读取器实例
         */
        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        /**
         * 根据 Split 创建记录读取器。
         *
         * <p>该方法执行实际的文件遍历和元数据转换:
         * <ol>
         *   <li>验证 Split 类型必须是 ObjectSplit</li>
         *   <li>获取 FileIO 和 location</li>
         *   <li>递归列出所有文件</li>
         *   <li>为每个文件创建一行数据</li>
         *   <li>如果设置了投影,应用字段投影</li>
         *   <li>返回迭代器读取器</li>
         * </ol>
         *
         * @param split 要读取的 Split,必须是 ObjectSplit 类型
         * @return 返回记录读取器
         * @throws IOException 如果访问文件系统时发生 I/O 错误
         * @throws IllegalArgumentException 如果 Split 类型不是 ObjectSplit
         */
        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof ObjectSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            ObjectSplit objSplit = (ObjectSplit) split;

            FileIO fileIO = objSplit.fileIO();
            String location = objSplit.location();

            RemoteIterator<FileStatus> objIter =
                    fileIO.listFilesIterative(new Path(location), true);
            String prefix = new Path(location).toUri().getPath();
            Iterator<InternalRow> iterator =
                    new Iterator<InternalRow>() {
                        @Override
                        public boolean hasNext() {
                            try {
                                return objIter.hasNext();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public InternalRow next() {
                            try {
                                return toRow(prefix, objIter.next());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row ->
                                        ProjectedRow.from(readType, ObjectTable.SCHEMA)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }
    }

    /**
     * 将文件状态转换为表的一行数据。
     *
     * <p>该方法将 {@link FileStatus} 的属性映射到对象表的 Schema:
     * <ul>
     *   <li><b>path</b>: 相对于 location 的相对路径</li>
     *   <li><b>name</b>: 文件名</li>
     *   <li><b>length</b>: 文件大小(字节)</li>
     *   <li><b>mtime</b>: 修改时间(毫秒时间戳)</li>
     *   <li><b>atime</b>: 访问时间(毫秒时间戳)</li>
     *   <li><b>owner</b>: 所有者名称</li>
     * </ul>
     *
     * <h3>路径处理</h3>
     * <p>path 字段存储的是相对路径:
     * <ul>
     *   <li>如果文件路径是 {@code /data/files/2024/01/file.txt}</li>
     *   <li>location 是 {@code /data/files/}</li>
     *   <li>则 path 为 {@code 2024/01/file.txt}(去除前导斜杠)</li>
     * </ul>
     *
     * @param prefix location 的路径前缀,用于计算相对路径
     * @param file 文件状态对象,包含文件的元数据信息
     * @return 返回表示文件元数据的行
     * @throws IllegalArgumentException 如果文件路径不包含指定的前缀
     */
    private static InternalRow toRow(String prefix, FileStatus file) {
        String path = file.getPath().toUri().getPath();
        if (!path.startsWith(prefix)) {
            throw new IllegalArgumentException(
                    String.format("File path '%s' does not contain prefix '%s'", path, prefix));
        }
        String relative = path.substring(prefix.length());
        if (relative.startsWith("/")) {
            relative = relative.substring(1);
        }
        return GenericRow.of(
                fromString(relative),
                fromString(file.getPath().getName()),
                file.getLen(),
                file.getModificationTime(),
                file.getAccessTime(),
                fromString(file.getOwner()));
    }
}
