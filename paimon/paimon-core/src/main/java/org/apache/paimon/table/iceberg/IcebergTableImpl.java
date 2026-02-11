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

package org.apache.paimon.table.iceberg;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link IcebergTable} 接口的实现类。
 *
 * <p>该实现类提供了 Iceberg 表在 Paimon 系统中的具体表示,实现了 {@link ReadonlyTable} 接口,
 * 表明这是一个只读表(当前不支持写入操作)。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>只读访问</b>: 实现 ReadonlyTable 接口,明确表示当前只支持元数据查询</li>
 *   <li><b>元数据管理</b>: 维护表的完整元数据信息,包括 Schema、分区、位置等</li>
 *   <li><b>配置复制</b>: 支持通过动态选项创建表的副本</li>
 *   <li><b>操作限制</b>: 显式禁止扫描和读取操作,抛出 UnsupportedOperationException</li>
 * </ul>
 *
 * <h3>字段说明</h3>
 * <ul>
 *   <li><b>identifier</b>: 表的唯一标识符,包含数据库名和表名</li>
 *   <li><b>fileIO</b>: 文件 I/O 接口,用于访问底层文件系统</li>
 *   <li><b>rowType</b>: 表的 Schema 定义,描述所有列的类型</li>
 *   <li><b>partitionKeys</b>: 分区键列表,定义表的分区结构</li>
 *   <li><b>location</b>: 表在文件系统中的根目录位置</li>
 *   <li><b>options</b>: 表的配置选项映射</li>
 *   <li><b>comment</b>: 表的注释说明(可选)</li>
 *   <li><b>uuid</b>: 表的唯一标识符(可选,如果未设置则使用 fullName)</li>
 * </ul>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li>实现 {@link IcebergTable} 接口,提供 Iceberg 表的标准功能</li>
 *   <li>实现 {@link ReadonlyTable} 接口,标识为只读表</li>
 *   <li>与 {@link org.apache.paimon.table.FileStoreTableImpl} 类似,但针对外部格式</li>
 *   <li>通过 {@link IcebergTable.Builder} 创建实例</li>
 * </ul>
 *
 * <h3>操作限制说明</h3>
 * <p>由于当前 Paimon 不支持直接读写 Iceberg 表,以下操作会抛出异常:
 * <ul>
 *   <li>{@link #newScan()} - 不支持创建扫描器</li>
 *   <li>{@link #newRead()} - 不支持创建读取器</li>
 * </ul>
 *
 * <p>这些限制可能在未来版本中解除,届时 Paimon 将能够读写 Iceberg 格式的表。
 *
 * @see IcebergTable 该类实现的接口
 * @see ReadonlyTable 只读表标记接口
 * @see org.apache.paimon.table.lance.LanceTableImpl 类似的外部格式实现
 */
public class IcebergTableImpl implements ReadonlyTable, IcebergTable {

    /** 表的标识符,包含数据库名和表名。 */
    private final Identifier identifier;

    /** 文件 I/O 接口,用于访问底层文件系统。 */
    private final FileIO fileIO;

    /** 表的行类型定义,描述所有列的类型信息。 */
    private final RowType rowType;

    /** 分区键列表,定义表的分区字段。 */
    private final List<String> partitionKeys;

    /** 表在文件系统中的位置路径。 */
    private final String location;

    /** 表的配置选项映射。 */
    private final Map<String, String> options;

    /** 表的注释说明(可选)。 */
    @Nullable private final String comment;

    /** 表的唯一标识符 UUID(可选)。 */
    @Nullable private final String uuid;

    /**
     * 构造一个 IcebergTableImpl 实例。
     *
     * @param identifier 表的标识符
     * @param fileIO 文件 I/O 接口
     * @param rowType 表的行类型定义
     * @param partitionKeys 分区键列表
     * @param location 表的位置路径
     * @param options 表的配置选项
     * @param comment 表的注释说明(可选)
     * @param uuid 表的 UUID(可选)
     */
    public IcebergTableImpl(
            Identifier identifier,
            FileIO fileIO,
            RowType rowType,
            List<String> partitionKeys,
            String location,
            Map<String, String> options,
            @Nullable String comment,
            @Nullable String uuid) {
        this.identifier = identifier;
        this.fileIO = fileIO;
        this.rowType = rowType;
        this.partitionKeys = partitionKeys;
        this.location = location;
        this.options = options;
        this.comment = comment;
        this.uuid = uuid;
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
     * 获取表的唯一标识符。
     *
     * <p>如果 uuid 字段为空,则返回 fullName 作为标识符。
     *
     * @return 返回表的 UUID 或完整表名
     */
    @Override
    public String uuid() {
        return StringUtils.isEmpty(uuid) ? fullName() : uuid;
    }

    /**
     * 获取表的行类型。
     *
     * @return 返回表的 Schema 定义
     */
    @Override
    public RowType rowType() {
        return rowType;
    }

    /**
     * 获取分区键列表。
     *
     * @return 返回分区字段名称列表
     */
    @Override
    public List<String> partitionKeys() {
        return partitionKeys;
    }

    /**
     * 获取主键列表。
     *
     * <p>Iceberg 表在 Paimon 中表示为无主键表。
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
     * @return 返回配置选项映射
     */
    @Override
    public Map<String, String> options() {
        return options;
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
     * 获取表的位置路径。
     *
     * @return 返回表在文件系统中的根目录
     */
    @Override
    public String location() {
        return location;
    }

    /**
     * 创建带有动态选项的表副本。
     *
     * <p>该方法会合并原有选项和动态选项,动态选项会覆盖同名的原有选项。
     *
     * @param dynamicOptions 动态选项映射
     * @return 返回新的 IcebergTable 实例
     */
    @Override
    public IcebergTable copy(Map<String, String> dynamicOptions) {
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.putAll(dynamicOptions);
        return new IcebergTableImpl(
                identifier, fileIO, rowType, partitionKeys, location, newOptions, comment, uuid);
    }

    /**
     * 创建表扫描器。
     *
     * <p><b>当前不支持该操作。</b>Paimon 尚未实现对 Iceberg 表的扫描功能。
     *
     * @return 不会返回,直接抛出异常
     * @throws UnsupportedOperationException 总是抛出,表示不支持该操作
     */
    @Override
    public InnerTableScan newScan() {
        throw new UnsupportedOperationException(
                "IcebergTable does not support InnerTableScan. Use newRead() instead.");
    }

    /**
     * 创建表读取器。
     *
     * <p><b>当前不支持该操作。</b>Paimon 尚未实现对 Iceberg 表的读取功能。
     *
     * @return 不会返回,直接抛出异常
     * @throws UnsupportedOperationException 总是抛出,表示不支持该操作
     */
    @Override
    public InnerTableRead newRead() {
        throw new UnsupportedOperationException(
                "IcebergTable does not support InnerTableRead. Use newScan() instead.");
    }
}
