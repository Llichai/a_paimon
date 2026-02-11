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

package org.apache.paimon.sort;

import org.apache.paimon.disk.FileIOChannel;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * 通道管理器,用于管理溢出通道的生命周期。
 */
public class SpillChannelManager {

    /** 通道ID集合 */
    private final HashSet<FileIOChannel.ID> channels;
    /** 打开的通道集合 */
    private final HashSet<FileIOChannel> openChannels;

    /**
     * 构造溢出通道管理器。
     */
    public SpillChannelManager() {
        this.channels = new HashSet<>(64);
        this.openChannels = new HashSet<>(64);
    }

    /**
     * 添加新的文件通道。
     *
     * @param id 通道ID
     */
    public synchronized void addChannel(FileIOChannel.ID id) {
        channels.add(id);
    }

    /**
     * 打开文件通道。
     *
     * @param toOpen 要打开的通道列表
     */
    public synchronized void addOpenChannels(List<FileIOChannel> toOpen) {
        for (FileIOChannel channel : toOpen) {
            openChannels.add(channel);
            channels.remove(channel.getChannelID());
        }
    }

    /**
     * 移除通道。
     *
     * @param id 通道ID
     */
    public synchronized void removeChannel(FileIOChannel.ID id) {
        channels.remove(id);
    }

    /**
     * 重置管理器,关闭并删除所有通道。
     */
    public synchronized void reset() {
        for (Iterator<FileIOChannel> channels = this.openChannels.iterator();
                channels.hasNext(); ) {
            final FileIOChannel channel = channels.next();
            channels.remove();
            try {
                channel.closeAndDelete();
            } catch (Throwable ignored) {
            }
        }

        for (Iterator<FileIOChannel.ID> channels = this.channels.iterator(); channels.hasNext(); ) {
            final FileIOChannel.ID channel = channels.next();
            channels.remove();
            try {
                final File f = new File(channel.getPath());
                if (f.exists()) {
                    f.delete();
                }
            } catch (Throwable ignored) {
            }
        }
    }
}
