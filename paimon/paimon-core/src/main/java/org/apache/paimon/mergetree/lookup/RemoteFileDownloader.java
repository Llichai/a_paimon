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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.io.DataFileMeta;

import java.io.File;

/**
 * 远程文件下载器接口
 *
 * <p>尝试下载远程 Lookup 文件到本地。
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：下载远程 SST 文件到本地
 *   <li>本地缓存：减少网络请求
 *   <li>快速查询：本地查询比远程查询快
 * </ul>
 *
 * <p>实现：
 * <ul>
 *   <li>{@link RemoteLookupFileManager}：默认实现
 * </ul>
 */
public interface RemoteFileDownloader {

    /**
     * 尝试下载远程文件到本地
     *
     * @param dataFile 数据文件元数据
     * @param remoteSstFile 远程 SST 文件路径
     * @param localFile 本地文件路径
     * @return true 表示下载成功，false 表示下载失败
     */
    boolean tryToDownload(DataFileMeta dataFile, String remoteSstFile, File localFile);
}
