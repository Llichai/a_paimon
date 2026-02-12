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

package org.apache.paimon.compression;

import java.io.Serializable;
import java.util.Objects;

/**
 * 压缩选项配置。
 *
 * <p>该类封装了数据压缩的配置选项,包括压缩算法类型和特定算法的参数。
 * Paimon 支持多种压缩算法,用于减少存储空间和网络传输开销。
 *
 * <h2>支持的压缩算法</h2>
 * <ul>
 *   <li><b>zstd</b>: Zstandard 压缩,默认算法,提供良好的压缩比和速度平衡
 *   <li><b>lz4</b>: LZ4 压缩,速度快,压缩比较低
 *   <li><b>snappy</b>: Snappy 压缩,速度快,适合实时场景
 *   <li><b>gzip</b>: Gzip 压缩,压缩比高,速度较慢
 *   <li><b>none</b>: 不压缩
 * </ul>
 *
 * <h2>配置选项</h2>
 * <ul>
 *   <li><b>compress</b>: 压缩算法名称
 *   <li><b>zstdLevel</b>: Zstd 压缩级别(1-22,默认为 1)
 *       <ul>
 *         <li>级别 1: 最快速度,较低压缩比
 *         <li>级别 3-5: 平衡速度和压缩比(推荐)
 *         <li>级别 9+: 更高压缩比,较慢速度
 *         <li>级别 22: 最高压缩比,最慢速度
 *       </ul>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 使用默认配置(zstd, level 1)
 * CompressOptions defaultOpts = CompressOptions.defaultOptions();
 *
 * // 使用 zstd 压缩,级别 3
 * CompressOptions zstdOpts = new CompressOptions("zstd", 3);
 *
 * // 使用 lz4 压缩(zstdLevel 对 lz4 无效)
 * CompressOptions lz4Opts = new CompressOptions("lz4", 0);
 *
 * // 使用 snappy 压缩
 * CompressOptions snappyOpts = new CompressOptions("snappy", 0);
 *
 * // 不压缩
 * CompressOptions noCompress = new CompressOptions("none", 0);
 *
 * // 在表配置中使用
 * Map<String, String> tableOptions = new HashMap<>();
 * tableOptions.put("file.compression", zstdOpts.compress());
 * tableOptions.put("file.compression.zstd-level",
 *                  String.valueOf(zstdOpts.zstdLevel()));
 * }</pre>
 *
 * <h2>压缩算法特性对比</h2>
 * <table border="1">
 *   <tr>
 *     <th>算法</th>
 *     <th>压缩比</th>
 *     <th>压缩速度</th>
 *     <th>解压速度</th>
 *     <th>适用场景</th>
 *   </tr>
 *   <tr>
 *     <td>zstd</td>
 *     <td>高</td>
 *     <td>快</td>
 *     <td>很快</td>
 *     <td>通用,默认选择</td>
 *   </tr>
 *   <tr>
 *     <td>lz4</td>
 *     <td>中等</td>
 *     <td>很快</td>
 *     <td>极快</td>
 *     <td>低延迟实时场景</td>
 *   </tr>
 *   <tr>
 *     <td>snappy</td>
 *     <td>中等</td>
 *     <td>很快</td>
 *     <td>很快</td>
 *     <td>实时流处理</td>
 *   </tr>
 *   <tr>
 *     <td>gzip</td>
 *     <td>很高</td>
 *     <td>慢</td>
 *     <td>中等</td>
 *     <td>归档存储</td>
 *   </tr>
 *   <tr>
 *     <td>none</td>
 *     <td>无</td>
 *     <td>极快</td>
 *     <td>极快</td>
 *     <td>调试或已压缩数据</td>
 *   </tr>
 * </table>
 *
 * <h2>选择建议</h2>
 * <ul>
 *   <li><b>通用场景</b>: 使用 zstd,level 1-3
 *   <li><b>低延迟要求</b>: 使用 lz4 或 snappy
 *   <li><b>存储优先</b>: 使用 zstd,level 5-9
 *   <li><b>归档场景</b>: 使用 gzip
 *   <li><b>调试测试</b>: 使用 none
 * </ul>
 *
 * <h2>设计理念</h2>
 * <p>该类采用不可变值对象模式:
 * <ul>
 *   <li>所有字段都是 final 的,创建后不可修改
 *   <li>实现 Serializable 接口,支持序列化
 *   <li>提供默认配置工厂方法
 *   <li>简洁的 API,易于使用
 * </ul>
 *
 * @since 1.0
 */
public class CompressOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 压缩算法名称。 */
    private final String compress;

    /** Zstd 压缩级别(1-22)。 */
    private final int zstdLevel;

    /**
     * 构造压缩选项。
     *
     * @param compress 压缩算法名称(zstd, lz4, snappy, gzip, none)
     * @param zstdLevel Zstd 压缩级别(1-22),仅对 zstd 算法有效
     */
    public CompressOptions(String compress, int zstdLevel) {
        this.compress = compress;
        this.zstdLevel = zstdLevel;
    }

    /**
     * 获取压缩算法名称。
     *
     * @return 压缩算法名称
     */
    public String compress() {
        return compress;
    }

    /**
     * 获取 Zstd 压缩级别。
     *
     * <p>取值范围: 1-22
     * <ul>
     *   <li>1: 最快速度,较低压缩比
     *   <li>3-5: 平衡速度和压缩比(推荐)
     *   <li>9+: 更高压缩比,较慢速度
     *   <li>22: 最高压缩比,最慢速度
     * </ul>
     *
     * @return Zstd 压缩级别
     */
    public int zstdLevel() {
        return zstdLevel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompressOptions that = (CompressOptions) o;
        return zstdLevel == that.zstdLevel && Objects.equals(compress, that.compress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compress, zstdLevel);
    }

    @Override
    public String toString() {
        return "CompressOptions{"
                + "compress='"
                + compress
                + '\''
                + ", zstdLevel="
                + zstdLevel
                + '}';
    }

    /**
     * 返回默认的压缩选项。
     *
     * <p>默认配置:
     * <ul>
     *   <li>压缩算法: zstd
     *   <li>压缩级别: 1
     * </ul>
     *
     * <p>该配置提供了良好的压缩比和速度平衡,适用于大多数场景。
     *
     * @return 默认的压缩选项
     */
    public static CompressOptions defaultOptions() {
        return new CompressOptions("zstd", 1);
    }
}
