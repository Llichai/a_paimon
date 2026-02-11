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

import java.net.InetSocketAddress;

/**
 * 查询服务器接口。
 *
 * <p>在集群的每个节点上运行的服务器，负责处理来自客户端的查询请求。
 *
 * <p><b>主要功能：</b>
 * <ul>
 *   <li>提供主键查找服务：根据主键快速查询数据
 *   <li>处理网络请求：接收和响应客户端查询
 *   <li>数据本地化：利用节点本地数据减少网络传输
 * </ul>
 *
 * <p><b>生命周期：</b>
 * <ol>
 *   <li>创建服务器实例
 *   <li>调用 {@link #start()} 启动服务
 *   <li>监听指定端口，处理请求
 *   <li>调用 {@link #shutdown()} 优雅关闭
 * </ol>
 *
 * @see QueryLocation
 */
public interface QueryServer {

    /**
     * 返回服务器监听的地址。
     *
     * @return 服务器地址（包含IP和端口）
     */
    InetSocketAddress getServerAddress();

    /**
     * 启动服务器。
     *
     * @throws Throwable 如果启动失败
     */
    void start() throws Throwable;

    /**
     * 关闭服务器并释放所有相关的线程池资源。
     */
    void shutdown();
}
