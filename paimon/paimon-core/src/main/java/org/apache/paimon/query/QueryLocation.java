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

import java.net.InetSocketAddress;

/**
 * 查询位置接口。
 *
 * <p>用于获取查询服务的网络位置，支持主键查找等查询操作的路由。
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>主键查找：根据分区和桶定位到具体的查询服务节点
 *   <li>负载均衡：将查询请求路由到不同的服务实例
 *   <li>分布式查询：在集群环境中定位数据所在节点
 * </ul>
 *
 * @see QueryLocationImpl
 * @see QueryServer
 */
public interface QueryLocation {

    /**
     * 根据分区和桶获取查询服务的位置。
     *
     * @param partition 分区键
     * @param bucket 桶编号
     * @param forceUpdate 是否强制刷新位置缓存
     * @return 查询服务的网络地址
     */
    InetSocketAddress getLocation(BinaryRow partition, int bucket, boolean forceUpdate);
}
