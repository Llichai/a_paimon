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

package org.apache.paimon.hive;

import org.apache.paimon.utils.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.metastore.Warehouse.getDnsPath;

/**
 * Paimon表位置键提取器，用于从Hive元存储表参数中提取Paimon表的实际存储位置.
 *
 * <p>该类提供多个方法来获取Paimon表在分布式文件系统中的存储路径，支持多种位置配置源， 包括表属性、元存储信息、Hive兼容系统配置和MapReduce任务配置。
 *
 * <h2>位置获取优先级</h2>
 *
 * <p>类中提供的三个重载方法遵循不同的查找优先级：
 *
 * <ul>
 *   <li>{@link #getPaimonLocation(Configuration, Properties)}： 优先级：paimon_location属性 >
 *       META_TABLE_LOCATION属性 > table.original.path配置
 *   <li>{@link #getPaimonLocation(Configuration, Table)}： 优先级：表参数中的paimon_location > 元存储表的存储位置
 *   <li>{@link #getPaimonLocation(JobConf)}： 优先级：paimon.internal.location > paimon_location >
 *       table.original.path > MapReduce输入目录
 * </ul>
 *
 * <h2>分区表处理</h2>
 *
 * <p>对于分区表，该类能够通过分析分区位置推导出表的根目录。通过向上遍历目录层级， 检查是否存在"schema"目录来确定表的实际根路径。
 *
 * <h2>使用场景</h2>
 *
 * <p>该类主要用于：
 *
 * <ul>
 *   <li>Hive Catalog与Paimon表集成时的位置解析
 *   <li>PaimonStorageHandler与Hive元存储的交互
 *   <li>MapReduce任务中获取Paimon表的数据路径
 * </ul>
 *
 * <h2>代码示例</h2>
 *
 * <pre>{@code
 * // 从Properties中获取位置
 * Configuration conf = new Configuration();
 * Properties props = new Properties();
 * props.setProperty("paimon_location", "/user/hive/warehouse/test.db/table");
 * String location = LocationKeyExtractor.getPaimonLocation(conf, props);
 *
 * // 从Hive Table对象中获取位置
 * Table hiveTable = metastoreClient.getTable("db", "table");
 * String location = LocationKeyExtractor.getPaimonLocation(conf, hiveTable);
 *
 * // 从MapReduce JobConf中获取位置
 * JobConf jobConf = new JobConf(conf);
 * String location = LocationKeyExtractor.getPaimonLocation(jobConf);
 * }</pre>
 *
 * @see org.apache.paimon.hive.HiveCatalogOptions
 * @see org.apache.hadoop.hive.metastore.api.Table
 */
public class LocationKeyExtractor {

    /**
     * Hive表属性中用于存储Paimon表位置的键名.
     *
     * <p>当用户通过{@code HiveCatalogOptions#LOCATION_IN_PROPERTIES}设置位置时， 该值会被存储在表的tbproperties参数中。
     */
    public static final String TBPROPERTIES_LOCATION_KEY = "paimon_location";

    /**
     * Paimon内部使用的位置属性键.
     *
     * <p>该属性由PaimonStorageHandler内部设置，用于在MapReduce任务执行过程中 传递Paimon表的位置信息。
     */
    public static final String INTERNAL_LOCATION = "paimon.internal.location";

    /**
     * 从Properties对象中获取Paimon表的实际存储路径.
     *
     * <p>该方法遵循以下优先级顺序查找表的位置：
     *
     * <ol>
     *   <li>检查表属性中的{@code paimon_location}键
     *   <li>检查Hive元存储的META_TABLE_LOCATION属性，并通过分区位置推导表根目录
     *   <li>检查Hive兼容系统的{@code table.original.path}配置
     *   <li>返回null表示未找到位置
     * </ol>
     *
     * <p><b>DNS路径转换</b>：当找到元存储位置时，会调用{@code getDnsPath()}方法将其转换为 DNS格式的标准路径，确保跨系统兼容性。
     *
     * @param conf Hadoop配置对象，可为null。当为null时跳过DNS路径转换。
     * @param properties 包含表元数据的Properties对象。
     * @return Paimon表的存储路径，如果未找到则返回null。
     * @throws RuntimeException 当DNS路径转换失败时抛出异常。
     * @see #tableLocation(String, Properties)
     */
    public static String getPaimonLocation(@Nullable Configuration conf, Properties properties) {
        // 首先从表属性中读取位置
        // 如果用户通过HiveCatalogOptions#LOCATION_IN_PROPERTIES设置了位置
        String location = properties.getProperty(TBPROPERTIES_LOCATION_KEY);
        if (location != null) {
            return location;
        }

        // 其次从元存储读取位置信息
        location = properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
        if (location != null) {
            // 对于分区表，通过分区位置推导表的根目录
            location = tableLocation(location, properties);
            if (conf != null) {
                try {
                    return getDnsPath(new Path(location), conf).toString();
                } catch (MetaException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return location;
            }
        }

        // 对于某些Hive兼容系统，尝试从特殊配置中读取
        if (conf != null) {
            return conf.get("table.original.path");
        }

        return null;
    }

    /**
     * 从Hive元存储的Table对象中获取Paimon表的实际存储路径.
     *
     * <p>该方法遵循以下优先级顺序查找表的位置：
     *
     * <ol>
     *   <li>检查表参数中的{@code paimon_location}键
     *   <li>从表的存储描述符(StorageDescriptor)中获取位置，并进行DNS路径转换
     *   <li>返回null表示未找到位置
     * </ol>
     *
     * <p><b>原地修改</b>：该方法在获取DNS转换后的路径时，会直接修改传入的Table对象 的存储描述符位置，确保元存储中存储的是标准格式的路径。
     *
     * <p><b>异常处理</b>：如果DNS路径转换出现问题，会抛出MetaException异常， 调用方需要适当处理。
     *
     * @param conf Hadoop配置对象，用于DNS路径转换。
     * @param table Hive元存储的Table对象。
     * @return Paimon表的存储路径，如果未找到则返回null。
     * @throws MetaException 当DNS路径转换失败时抛出异常。
     * @see org.apache.hadoop.hive.metastore.api.Table
     * @see org.apache.hadoop.hive.metastore.api.StorageDescriptor
     */
    public static String getPaimonLocation(Configuration conf, Table table) throws MetaException {
        // 首先从表参数中读取位置
        // 如果用户通过HiveCatalogOptions#LOCATION_IN_PROPERTIES设置了位置
        Map<String, String> params = table.getParameters();
        if (params != null) {
            String location = params.get(TBPROPERTIES_LOCATION_KEY);
            if (location != null) {
                return location;
            }
        }

        // 其次从元存储表的存储描述符中读取位置信息
        String location = table.getSd().getLocation();
        if (location != null) {
            // 进行DNS路径转换以获得标准路径
            location = getDnsPath(new Path(location), conf).toString();
            // 原地修改表的存储位置
            table.getSd().setLocation(location);
        }
        return location;
    }

    /**
     * 从MapReduce JobConf中获取Paimon表的实际存储路径.
     *
     * <p>该方法用于在MapReduce/Spark任务执行时从任务配置中获取表位置， 遵循以下优先级顺序：
     *
     * <ol>
     *   <li>检查PaimonStorageHandler设置的{@code paimon.internal.location}属性
     *   <li>检查表属性中的{@code paimon_location}键
     *   <li>检查Hive兼容系统的{@code table.original.path}配置
     *   <li>从MapReduce输入目录推导表位置（用于分区表）
     *   <li>返回null表示未找到位置
     * </ol>
     *
     * <p><b>分区表位置推导</b>：对于分区表扫描任务，MapReduce的输入目录可能是分区目录。 该方法通过向上遍历父目录并检查"schema"文件夹是否存在来找到表的根目录。
     *
     * <p><b>工作流程</b>：
     *
     * <pre>
     * 给定输入路径：/warehouse/db/table/year=2024/month=01/day=15/
     *
     * 1. 检查/warehouse/db/table/year=2024/month=01/day=15/是否有schema目录 → 否
     * 2. 检查/warehouse/db/table/year=2024/month=01/是否有schema目录 → 否
     * 3. 检查/warehouse/db/table/year=2024/是否有schema目录 → 否
     * 4. 检查/warehouse/db/table/是否有schema目录 → 是
     * 5. 返回/warehouse/db/table/作为表的根目录
     * </pre>
     *
     * @param conf MapReduce JobConf对象，包含任务配置信息。
     * @return Paimon表的存储路径。如果未找到则返回null。
     * @throws UncheckedIOException 当文件系统访问出现I/O异常时抛出异常。
     * @throws RuntimeException 当DNS路径转换失败时抛出异常。
     * @see org.apache.hadoop.mapreduce.lib.input.FileInputFormat#INPUT_DIR
     */
    public static String getPaimonLocation(JobConf conf) {
        // 首先从PaimonStorageHandler设置的内部属性中读取
        String location = conf.get(INTERNAL_LOCATION);
        if (location != null) {
            return location;
        }

        // 其次从表属性中读取位置
        // 如果用户通过HiveCatalogOptions#LOCATION_IN_PROPERTIES设置了位置
        location = conf.get(TBPROPERTIES_LOCATION_KEY);
        if (location != null) {
            return location;
        }

        // 对于某些Hive兼容系统，尝试从特殊配置中读取
        location = conf.get("table.original.path");
        if (location != null) {
            return location;
        }

        // 从这个Hive任务的输入目录推导表位置
        //
        // 需要注意的是，输入目录可能是某个分区的目录。
        // 因此我们需要通过检查每个父目录中是否存在"schema"目录来找到表的根目录。
        location = conf.get(FileInputFormat.INPUT_DIR);
        if (location != null) {
            Path path = new Path(location);
            try {
                FileSystem fs = path.getFileSystem(conf);
                // 从输入目录向上遍历，寻找包含schema目录的表根路径
                while (path != null) {
                    if (fs.exists(new Path(path, "schema"))) {
                        break;
                    }
                    path = path.getParent();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (path != null) {
                try {
                    return getDnsPath(path, conf).toString();
                } catch (MetaException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return null;
    }

    /**
     * 通过分区位置推导出Paimon表的根目录.
     *
     * <p>对于分区表，文件系统中的路径结构通常为： {@code
     * /table_root/partition_col1=val1/partition_col2=val2/.../data_files}
     *
     * <p>该方法接收一个分区位置路径，通过以下步骤推导表的根目录：
     *
     * <ol>
     *   <li>获取表的所有分区列信息
     *   <li>如果表没有分区列，直接返回原始位置
     *   <li>使用第一个分区列名作为分隔符，截取路径的前缀部分作为表根目录
     * </ol>
     *
     * <p><b>工作示例</b>：
     *
     * <pre>
     * 输入位置：/warehouse/db/table/year=2024/month=01/day=15/
     * 分区列：year,month,day
     *
     * 1. 获取第一个分区列名：year
     * 2. 使用"/year="作为分隔符进行分割
     * 3. 返回分隔符前的部分：/warehouse/db/table/
     * </pre>
     *
     * <p><b>设计说明</b>：该方法只使用第一个分区列进行分割，因为所有分区列共享相同的 目录结构前缀，使用第一个分区列已足以确定表的根目录。
     *
     * @param location 输入的分区或表位置路径。
     * @param properties 包含表元数据的Properties对象，包含分区列信息。
     * @return 推导出的表根目录路径。如果表无分区列，返回原始location。
     * @see #TBPROPERTIES_LOCATION_KEY
     */
    private static String tableLocation(String location, Properties properties) {
        // 获取表的分区列信息，格式为：col1,col2,col3,...
        String partitionProperty =
                properties.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
        if (StringUtils.isEmpty(partitionProperty)) {
            // 表无分区列，直接返回原始位置
            return location;
        }

        // 解析分区列名列表
        List<String> partitionKeys =
                Arrays.asList(partitionProperty.split(org.apache.paimon.fs.Path.SEPARATOR));
        // 使用第一个分区列作为分隔符，截取表根目录
        // 例如：/table/year=2024/month=01/ → /table/
        return location.split(org.apache.paimon.fs.Path.SEPARATOR + partitionKeys.get(0) + "=")[0];
    }
}
