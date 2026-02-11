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

package org.apache.paimon.query;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.ServiceManager;

import java.net.InetSocketAddress;
import java.util.Optional;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;
import static org.apache.paimon.table.sink.ChannelComputer.select;

/**
 * {@link QueryLocation} 的实现类，从 {@link ServiceManager} 获取查询服务位置。
 *
 * <p>通过 {@link ServiceManager} 管理的服务发现机制，获取主键查找服务的地址列表，
 * 并根据分区和桶号计算出具体的服务节点。
 *
 * <p><b>工作原理：</b>
 * <ol>
 *   <li>从 ServiceManager 获取服务地址数组
 *   <li>使用 ChannelComputer.select 根据分区和桶计算目标节点索引
 *   <li>返回对应的服务地址
 *   <li>支持地址缓存和强制更新
 * </ol>
 *
 * @see ServiceManager
 */
public class QueryLocationImpl implements QueryLocation {

    /** 服务管理器，用于获取服务地址。 */
    private final ServiceManager manager;

    /** 地址缓存，避免频繁读取文件系统。 */
    private InetSocketAddress[] addressesCache;

    /**
     * 构造函数。
     *
     * @param manager 服务管理器
     */
    public QueryLocationImpl(ServiceManager manager) {
        this.manager = manager;
    }

    @Override
    public InetSocketAddress getLocation(BinaryRow partition, int bucket, boolean forceUpdate) {
        if (addressesCache == null || forceUpdate) {
            Optional<InetSocketAddress[]> addresses = manager.service(PRIMARY_KEY_LOOKUP);
            if (!addresses.isPresent()) {
                throw new RuntimeException(
                        "Cannot find address for table path: " + manager.tablePath());
            }
            addressesCache = addresses.get();
        }

        return addressesCache[select(partition, bucket, addressesCache.length)];
    }
}
