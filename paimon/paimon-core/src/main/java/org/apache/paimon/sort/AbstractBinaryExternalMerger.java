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

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.disk.ChannelReaderInputView;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.utils.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * {@link BinaryExternalSortBuffer} 的溢出文件合并器。
 *
 * <p>一次最多合并 {@link #maxFanIn} 个溢出文件。
 *
 * @param <Entry> 要合并排序的条目类型
 */
public abstract class AbstractBinaryExternalMerger<Entry> implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryExternalMerger.class);

    /** 是否已关闭标志 */
    private volatile boolean closed;

    /** 最大合并扇入数 */
    private final int maxFanIn;
    /** 溢出通道管理器 */
    private final SpillChannelManager channelManager;
    /** 压缩编解码工厂 */
    private final BlockCompressionFactory compressionCodecFactory;
    /** 压缩块大小 */
    private final int compressionBlockSize;

    /** 页大小 */
    protected final int pageSize;
    /** IO管理器 */
    protected final IOManager ioManager;

    /**
     * 构造二进制外部合并器。
     *
     * @param ioManager IO管理器
     * @param pageSize 页大小
     * @param maxFanIn 最大合并扇入数
     * @param channelManager 溢出通道管理器
     * @param compressionCodecFactory 压缩编解码工厂
     * @param compressionBlockSize 压缩块大小
     */
    public AbstractBinaryExternalMerger(
            IOManager ioManager,
            int pageSize,
            int maxFanIn,
            SpillChannelManager channelManager,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize) {
        this.ioManager = ioManager;
        this.pageSize = pageSize;
        this.maxFanIn = maxFanIn;
        this.channelManager = channelManager;
        this.compressionCodecFactory = compressionCodecFactory;
        this.compressionBlockSize = compressionBlockSize;
    }

    @Override
    public void close() {
        this.closed = true;
    }

    /**
     * 返回遍历所有给定通道合并结果的迭代器。
     *
     * @param channelIDs 要合并的通道ID列表
     * @param openChannels 打开的通道列表
     * @return 遍历合并记录的迭代器
     * @throws IOException 如果读取器遇到IO问题
     */
    public BinaryMergeIterator<Entry> getMergingIterator(
            List<ChannelWithMeta> channelIDs, List<FileIOChannel> openChannels) throws IOException {
        // create one iterator per channel id
        if (LOG.isDebugEnabled()) {
            LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
        }

        final List<MutableObjectIterator<Entry>> iterators = new ArrayList<>(channelIDs.size() + 1);

        for (ChannelWithMeta channel : channelIDs) {
            ChannelReaderInputView view =
                    FileChannelUtil.createInputView(
                            ioManager,
                            channel,
                            openChannels,
                            compressionCodecFactory,
                            compressionBlockSize);
            iterators.add(channelReaderInputViewIterator(view));
        }

        return new BinaryMergeIterator<>(
                iterators, mergeReusedEntries(channelIDs.size()), mergeComparator());
    }

    /**
     * 将给定的已排序运行合并为更少数量的已排序运行。
     *
     * @param channelIDs 需要合并的已排序运行的ID列表
     * @return 合并后通道的ID列表
     * @throws IOException 如果读取器或写入器遇到IO问题
     */
    public List<ChannelWithMeta> mergeChannelList(List<ChannelWithMeta> channelIDs)
            throws IOException {
        // A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1
        // rounds where every merge
        // is a full merge with maxFanIn input channels. A partial round includes merges with fewer
        // than maxFanIn
        // inputs. It is most efficient to perform the partial round first.
        final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(maxFanIn)) - 1;

        final int numStart = channelIDs.size();
        final int numEnd = (int) Math.pow(maxFanIn, scale);

        final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (maxFanIn - 1));

        final int numNotMerged = numEnd - numMerges;
        final int numToMerge = numStart - numNotMerged;

        // unmerged channel IDs are copied directly to the result list
        final List<ChannelWithMeta> mergedChannelIDs = new ArrayList<>(numEnd);
        mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged));

        final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

        final List<ChannelWithMeta> channelsToMergeThisStep =
                new ArrayList<>(channelsToMergePerStep);
        int channelNum = numNotMerged;
        while (!closed && channelNum < channelIDs.size()) {
            channelsToMergeThisStep.clear();

            for (int i = 0;
                    i < channelsToMergePerStep && channelNum < channelIDs.size();
                    i++, channelNum++) {
                channelsToMergeThisStep.add(channelIDs.get(channelNum));
            }

            mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep));
        }

        return mergedChannelIDs;
    }

    /**
     * 将由给定通道ID描述的已排序运行合并为单个已排序运行。
     *
     * @param channelIDs 运行通道的ID列表
     * @return 描述合并运行的通道ID和块数
     * @throws IOException 如果遇到IO问题
     */
    private ChannelWithMeta mergeChannels(List<ChannelWithMeta> channelIDs) throws IOException {
        // the list with the target iterators
        List<FileIOChannel> openChannels = new ArrayList<>(channelIDs.size());
        final BinaryMergeIterator<Entry> mergeIterator =
                getMergingIterator(channelIDs, openChannels);

        // create a new channel writer
        final FileIOChannel.ID mergedChannelID = ioManager.createChannel();
        channelManager.addChannel(mergedChannelID);
        ChannelWriterOutputView output = null;

        int numBlocksWritten;
        try {
            output =
                    FileChannelUtil.createOutputView(
                            ioManager,
                            mergedChannelID,
                            compressionCodecFactory,
                            compressionBlockSize);
            writeMergingOutput(mergeIterator, output);
            output.close();
            numBlocksWritten = output.getBlockCount();
        } catch (IOException e) {
            if (output != null) {
                output.close();
                output.getChannel().deleteChannel();
            }
            throw e;
        } finally {
            // remove, close and delete channels
            for (FileIOChannel channel : openChannels) {
                channelManager.removeChannel(channel.getChannelID());
                try {
                    channel.closeAndDelete();
                } catch (Throwable ignored) {
                }
            }
        }

        return new ChannelWithMeta(mergedChannelID, numBlocksWritten, output.getWriteBytes());
    }

    // -------------------------------------------------------------------------------------------

    /**
     * 创建从输入视图读取的条目迭代器。
     *
     * @param inView 通道读取器输入视图
     * @return 条目迭代器
     */
    protected abstract MutableObjectIterator<Entry> channelReaderInputViewIterator(
            ChannelReaderInputView inView);

    /**
     * 获取合并时使用的比较器。
     *
     * @return 合并比较器
     */
    protected abstract Comparator<Entry> mergeComparator();

    /**
     * 获取合并时使用的可重用条目对象列表。
     *
     * @param size 条目数量
     * @return 可重用条目对象列表
     */
    protected abstract List<Entry> mergeReusedEntries(int size);

    /**
     * 读取合并流并将数据写回。
     *
     * @param mergeIterator 合并迭代器
     * @param output 输出视图
     * @throws IOException 如果遇到IO问题
     */
    protected abstract void writeMergingOutput(
            MutableObjectIterator<Entry> mergeIterator, AbstractPagedOutputView output)
            throws IOException;
}
