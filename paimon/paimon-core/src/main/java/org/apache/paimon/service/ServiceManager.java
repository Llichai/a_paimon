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

package org.apache.paimon.service;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * 服务管理器，用于管理各种服务的注册和发现。
 *
 * <p><b>主要功能：</b>
 * <ul>
 *   <li>服务注册：将服务地址信息写入文件系统
 *   <li>服务发现：读取服务地址信息供客户端使用
 *   <li>服务更新：支持服务地址的动态更新
 *   <li>服务删除：清理不再使用的服务信息
 * </ul>
 *
 * <p><b>服务类型：</b>
 * 目前支持的服务类型：
 * <ul>
 *   <li>{@link #PRIMARY_KEY_LOOKUP}：主键查找服务，用于根据主键快速查询数据
 * </ul>
 *
 * <p><b>存储机制：</b>
 * 服务信息以 JSON 格式存储在文件系统中：
 * <ul>
 *   <li>路径：{@code <tablePath>/service/service-<id>}
 *   <li>格式：InetSocketAddress 数组的 JSON 序列化
 *   <li>更新策略：覆盖写入，支持原子更新
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>分布式查询：在集群环境中定位查询服务节点
 *   <li>负载均衡：客户端通过服务地址列表实现负载均衡
 *   <li>服务发现：动态获取可用的服务实例
 * </ul>
 *
 * <p><b>线程安全：</b>
 * 实现了 Serializable 接口，可以在分布式环境中序列化传输。
 *
 * @see QueryLocationImpl
 * @see QueryServer
 */
public class ServiceManager implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 服务文件名前缀。 */
    public static final String SERVICE_PREFIX = "service-";

    /** 主键查找服务的标识符。 */
    public static final String PRIMARY_KEY_LOOKUP = "primary-key-lookup";

    /** 文件 IO 操作接口。 */
    private final FileIO fileIO;

    /** 表路径，服务信息存储在表目录下。 */
    private final Path tablePath;

    /**
     * 构造函数。
     *
     * @param fileIO 文件 IO 操作接口
     * @param tablePath 表路径
     */
    public ServiceManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    /**
     * 获取表路径。
     *
     * @return 表路径
     */
    public Path tablePath() {
        return tablePath;
    }

    /**
     * 获取指定服务的地址列表。
     *
     * @param id 服务标识符
     * @return 服务地址数组的 Optional，如果服务不存在则返回 empty
     */
    public Optional<InetSocketAddress[]> service(String id) {
        try {
            return fileIO.readOverwrittenFileUtf8(servicePath(id))
                    .map(s -> JsonSerdeUtil.fromJson(s, InetSocketAddress[].class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 重置服务地址列表。
     *
     * <p>覆盖写入新的服务地址，支持服务节点的动态更新。
     *
     * @param id 服务标识符
     * @param addresses 服务地址数组
     */
    public void resetService(String id, InetSocketAddress[] addresses) {
        try {
            fileIO.overwriteFileUtf8(servicePath(id), JsonSerdeUtil.toJson(addresses));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 删除服务信息。
     *
     * @param id 服务标识符
     */
    public void deleteService(String id) {
        fileIO.deleteQuietly(servicePath(id));
    }

    /**
     * 生成服务文件路径。
     *
     * @param id 服务标识符
     * @return 服务文件的完整路径
     */
    private Path servicePath(String id) {
        return new Path(tablePath + "/service/" + SERVICE_PREFIX + id);
    }
}
