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

package org.apache.paimon.iceberg;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.options.description.TextElement;
import org.apache.paimon.utils.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.options.ConfigOptions.key;

/**
 * Iceberg 兼容性配置选项类。
 *
 * <p>此类定义了 Paimon 与 Iceberg 兼容性相关的所有配置选项,用于控制 Iceberg 元数据的生成、存储和管理。
 *
 * <p>主要功能:
 * <ul>
 *   <li>存储类型配置:支持 DISABLED、TABLE_LOCATION、HADOOP_CATALOG、HIVE_CATALOG、REST_CATALOG 等多种存储方式
 *   <li>元数据管理:控制元数据文件的格式版本、压缩方式、保留策略等
 *   <li>Catalog 集成:支持与 Hive、Hadoop、REST 等 Catalog 的集成
 *   <li>性能优化:配置 Manifest 文件压缩触发条件
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>需要让 Iceberg 读取器读取 Paimon 的原始数据文件时
 *   <li>实现 Paimon 表与 Iceberg 表的互操作
 *   <li>将 Paimon 表注册到 Hive Metastore 作为 Iceberg 外部表
 * </ul>
 *
 * @see StorageType Iceberg 元数据存储类型枚举
 * @see StorageLocation Iceberg 元数据存储位置枚举
 */
public class IcebergOptions {

    /** REST Catalog 配置前缀,所有以此前缀开头的配置项将传递给 REST Catalog */
    public static final String REST_CONFIG_PREFIX = "metadata.iceberg.rest.";

    /**
     * Iceberg 元数据存储类型配置。
     *
     * <p>当设置后,每次快照提交后会生成 Iceberg 元数据,使 Iceberg 读取器能够读取 Paimon 的原始数据文件。
     */
    public static final ConfigOption<StorageType> METADATA_ICEBERG_STORAGE =
            key("metadata.iceberg.storage")
                    .enumType(StorageType.class)
                    .defaultValue(StorageType.DISABLED)
                    .withDescription(
                            "When set, produce Iceberg metadata after a snapshot is committed, "
                                    + "so that Iceberg readers can read Paimon's raw data files.");

    /**
     * Iceberg 元数据存储位置配置。
     *
     * <p>指定是将 Iceberg 元数据存储在表目录下,还是独立的 Catalog 目录下。
     */
    public static final ConfigOption<StorageLocation> METADATA_ICEBERG_STORAGE_LOCATION =
            key("metadata.iceberg.storage-location")
                    .enumType(StorageLocation.class)
                    .noDefaultValue()
                    .withDescription(
                            "To store Iceberg metadata in a separate directory or under table location");

    /**
     * Iceberg 表格式版本配置。
     *
     * <p>指定生成的 Iceberg 表的格式版本,支持版本 2 和 3。
     * <p>注意:只有版本 3 支持删除向量(deletion vector)功能。
     */
    public static final ConfigOption<Integer> FORMAT_VERSION =
            ConfigOptions.key("metadata.iceberg.format-version")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The format version of iceberg table, the value can be 2 or 3. "
                                    + "Note that only version 3 supports deletion vector.");

    /**
     * Manifest 元数据压缩最小文件数。
     *
     * <p>当 Iceberg Manifest 元数据文件数量达到此阈值时,触发 Manifest 元数据压缩。
     */
    public static final ConfigOption<Integer> COMPACT_MIN_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.min.file-num")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Minimum number of Iceberg manifest metadata files to trigger manifest metadata compaction.");

    /**
     * Manifest 元数据压缩最大文件数。
     *
     * <p>如果小型 Iceberg Manifest 元数据文件数量超过此限制,无论其总大小如何都会触发 Manifest 元数据压缩。
     */
    public static final ConfigOption<Integer> COMPACT_MAX_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.max.file-num")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "If number of small Iceberg manifest metadata files exceeds this limit, "
                                    + "always trigger manifest metadata compaction regardless of their total size.");

    /**
     * 提交后删除旧元数据开关。
     *
     * <p>控制是否在每次表提交后删除旧的元数据文件,以节省存储空间。
     */
    public static final ConfigOption<Boolean> METADATA_DELETE_AFTER_COMMIT =
            key("metadata.iceberg.delete-after-commit.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to delete old metadata files after each table commit");

    /**
     * 保留的旧版本元数据最大数量。
     *
     * <p>指定每次表提交后保留多少个旧元数据文件。对于 REST Catalog,至少会保留 1 个旧元数据。
     */
    public static final ConfigOption<Integer> METADATA_PREVIOUS_VERSIONS_MAX =
            key("metadata.iceberg.previous-versions-max")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The number of old metadata files to keep after each table commit. "
                                    + "For rest-catalog, it will keep 1 old metadata at least.");

    /** Hive Metastore URI 配置,用于 Iceberg Hive Catalog */
    public static final ConfigOption<String> URI =
            key("metadata.iceberg.uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive metastore uri for Iceberg Hive catalog.");

    /** Hive 配置目录,用于 Iceberg Hive Catalog */
    public static final ConfigOption<String> HIVE_CONF_DIR =
            key("metadata.iceberg.hive-conf-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hive-conf-dir for Iceberg Hive catalog.");

    /** Hadoop 配置目录,用于 Iceberg Hive Catalog */
    public static final ConfigOption<String> HADOOP_CONF_DIR =
            key("metadata.iceberg.hadoop-conf-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hadoop-conf-dir for Iceberg Hive catalog.");

    /**
     * Manifest 文件压缩格式配置。
     *
     * <p>指定 Iceberg Manifest 文件的压缩格式。默认使用 snappy,
     * 因为某些 Iceberg 读取器(例如 DuckDB)不支持 zstd 压缩。
     */
    public static final ConfigOption<String> MANIFEST_COMPRESSION =
            key("metadata.iceberg.manifest-compression")
                    .stringType()
                    .defaultValue(
                            "snappy") // some Iceberg reader cannot support zstd, for example DuckDB
                    .withDescription("Compression for Iceberg manifest files.");

    /**
     * 使用旧版 Manifest 格式开关。
     *
     * <p>是否使用旧版 Manifest 版本来生成 Iceberg 1.4 的 Manifest 文件。
     */
    public static final ConfigOption<Boolean> MANIFEST_LEGACY_VERSION =
            key("metadata.iceberg.manifest-legacy-version")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Should use the legacy manifest version to generate Iceberg's 1.4 manifest files.");

    /** Hive 客户端类名配置,用于 Iceberg Hive Catalog */
    public static final ConfigOption<String> HIVE_CLIENT_CLASS =
            key("metadata.iceberg.hive-client-class")
                    .stringType()
                    .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
                    .withDescription("Hive client class name for Iceberg Hive Catalog.");

    /**
     * Metastore 数据库名配置。
     *
     * <p>指定 Iceberg Catalog 的 Metastore 数据库名。
     * 如果使用集中式 Catalog,可将此设置为 Iceberg 数据库别名。
     */
    public static final ConfigOption<String> METASTORE_DATABASE =
            key("metadata.iceberg.database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Metastore database name for Iceberg Catalog. "
                                    + "Set this as an iceberg database alias if using a centralized Catalog.");

    /**
     * Metastore 表名配置。
     *
     * <p>指定 Iceberg Catalog 的 Metastore 表名。
     * 如果使用集中式 Catalog,可将此设置为 Iceberg 表别名。
     */
    public static final ConfigOption<String> METASTORE_TABLE =
            key("metadata.iceberg.table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Metastore table name for Iceberg Catalog."
                                    + "Set this as an iceberg table alias if using a centralized Catalog.");

    /** AWS Glue Catalog 跳过归档开关 */
    public static final ConfigOption<Boolean> GLUE_SKIP_ARCHIVE =
            key("metadata.iceberg.glue.skip-archive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip archive for AWS Glue catalog.");

    /** Hive 统计信息更新跳过开关 */
    public static final ConfigOption<Boolean> HIVE_SKIP_UPDATE_STATS =
            key("metadata.iceberg.hive-skip-update-stats")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip updating Hive stats.");

    /** 配置选项对象 */
    private final Options options;

    /**
     * 从配置 Map 构造 IcebergOptions。
     *
     * @param options 配置项 Map
     */
    public IcebergOptions(Map<String, String> options) {
        this(Options.fromMap(options));
    }

    /**
     * 从 Options 对象构造 IcebergOptions。
     *
     * @param options Options 配置对象
     */
    public IcebergOptions(Options options) {
        this.options = options;
    }

    /**
     * 获取 Iceberg REST Catalog 配置。
     *
     * <p>提取所有以 {@link #REST_CONFIG_PREFIX} 开头的配置项,
     * 去除前缀后作为 REST Catalog 的配置参数。
     *
     * @return REST Catalog 配置 Map
     * @throws IllegalArgumentException 如果配置键去除前缀后为空
     */
    public Map<String, String> icebergRestConfig() {
        Map<String, String> restConfig = new HashMap<>();
        options.keySet()
                .forEach(
                        key -> {
                            if (key.startsWith(REST_CONFIG_PREFIX)) {
                                String restConfigKey = key.substring(REST_CONFIG_PREFIX.length());
                                Preconditions.checkArgument(
                                        !restConfigKey.isEmpty(),
                                        "config key '%s' for iceberg rest catalog is empty!",
                                        key);
                                restConfig.put(restConfigKey, options.get(key));
                            }
                        });
        return restConfig;
    }

    /**
     * 是否启用提交后删除旧元数据。
     *
     * @return 如果启用返回 true
     */
    public boolean deleteAfterCommitEnabled() {
        return options.get(METADATA_DELETE_AFTER_COMMIT);
    }

    /**
     * 获取保留的旧版本元数据最大数量。
     *
     * @return 保留的旧版本数量
     */
    public int previousVersionsMax() {
        return options.get(METADATA_PREVIOUS_VERSIONS_MAX);
    }

    /**
     * Iceberg 元数据存储类型枚举。
     *
     * <p>定义了 Iceberg 元数据的存储位置和方式:
     * <ul>
     *   <li>DISABLED: 禁用 Iceberg 兼容性支持
     *   <li>TABLE_LOCATION: 将元数据存储在每个表的目录下
     *   <li>HADOOP_CATALOG: 将元数据存储在独立目录下,可作为 Iceberg Hadoop Catalog 的仓库目录
     *   <li>HIVE_CATALOG: 类似 HADOOP_CATALOG,但额外在 Hive 中创建 Iceberg 外部表
     *   <li>REST_CATALOG: 将元数据存储在 REST Catalog 中,可与 Iceberg REST Catalog 服务集成
     * </ul>
     */
    public enum StorageType implements DescribedEnum {
        DISABLED("disabled", "Disable Iceberg compatibility support."),
        TABLE_LOCATION("table-location", "Store Iceberg metadata in each table's directory."),
        HADOOP_CATALOG(
                "hadoop-catalog",
                "Store Iceberg metadata in a separate directory. "
                        + "This directory can be specified as the warehouse directory of an Iceberg Hadoop catalog."),
        HIVE_CATALOG(
                "hive-catalog",
                "Not only store Iceberg metadata like hadoop-catalog, "
                        + "but also create Iceberg external table in Hive."),
        REST_CATALOG(
                "rest-catalog",
                "Store Iceberg metadata in a REST catalog. "
                        + "This allows integration with Iceberg REST catalog services.");

        private final String value;
        private final String description;

        StorageType(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return TextElement.text(description);
        }
    }

    /**
     * Iceberg 元数据存储位置枚举。
     *
     * <p>定义了 Iceberg 元数据的具体存储位置:
     * <ul>
     *   <li>TABLE_LOCATION: 将元数据存储在表目录下,适用于独立的 Iceberg 表或 Java API 访问,也可与 Hive Catalog 一起使用
     *   <li>CATALOG_STORAGE: 将元数据存储在独立目录下,允许与 Hive Catalog 或 Hadoop Catalog 集成
     * </ul>
     */
    public enum StorageLocation implements DescribedEnum {
        TABLE_LOCATION(
                "table-location",
                "Store Iceberg metadata in each table's directory. Useful for standalone "
                        + "Iceberg tables or Java API access. Can also be used with Hive Catalog"),
        CATALOG_STORAGE(
                "catalog-location",
                "Store Iceberg metadata in a separate directory. "
                        + "Allows integration with Hive Catalog or Hadoop Catalog.");

        private final String value;
        private final String description;

        StorageLocation(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return TextElement.text(description);
        }
    }

    /**
     * Returns all ConfigOption fields defined in this class. This method uses reflection to
     * dynamically discover all ConfigOption fields, ensuring that new options are automatically
     * included without code changes.
     */
    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = IcebergOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(IcebergOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }
}
