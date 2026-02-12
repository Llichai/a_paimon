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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.SimpleFileEntry;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 需要重试的提交结果
 *
 * <p>表示提交操作失败，需要重试的结果。
 *
 * <h2>重试类型</h2>
 * <p>提供两种重试结果：
 * <ul>
 *   <li><b>提交失败重试</b>：{@link CommitFailRetryResult}
 *       <ul>
 *         <li>提交过程中失败
 *         <li>保留最新快照和基线数据
 *         <li>可以重新检测冲突并重试
 *       </ul>
 *   <li><b>回滚重试</b>：{@link RollbackRetryResult}
 *       <ul>
 *         <li>回滚操作失败
 *         <li>不保留快照数据
 *         <li>通常直接重新提交
 *       </ul>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p>在以下情况返回重试结果：
 * <ul>
 *   <li>快照提交时发生冲突
 *   <li>文件写入失败
 *   <li>Manifest操作失败
 *   <li>回滚COMPACT快照失败
 * </ul>
 *
 * <h2>异常处理</h2>
 * <p>每个重试结果都携带原始异常：
 * <ul>
 *   <li>用于日志记录和调试
 *   <li>帮助判断是否应该继续重试
 *   <li>提供失败原因的详细信息
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * CommitResult result = tryCommit();
 * if (!result.isSuccess()) {
 *     RetryCommitResult retry = (RetryCommitResult) result;
 *     if (retry instanceof CommitFailRetryResult) {
 *         // 重新检测冲突并重试
 *         CommitFailRetryResult fail = (CommitFailRetryResult) retry;
 *         retryWithConflictDetection(fail.latestSnapshot, fail.baseDataFiles);
 *     } else {
 *         // 直接重试
 *         retryCommit();
 *     }
 * }
 * }</pre>
 *
 * @see CommitResult 提交结果接口
 * @see SuccessCommitResult 成功结果
 */
public abstract class RetryCommitResult implements CommitResult {

    /** 导致重试的异常 */
    public final Exception exception;

    /**
     * 构造重试提交结果
     *
     * @param exception 导致重试的异常
     */
    private RetryCommitResult(Exception exception) {
        this.exception = exception;
    }

    /**
     * 创建提交失败的重试结果
     *
     * <p>当快照提交失败时使用，保留快照和数据信息用于重试。
     *
     * @param snapshot 最新快照
     * @param baseDataFiles 基线数据文件
     * @param exception 导致失败的异常
     * @return 提交失败重试结果
     */
    public static RetryCommitResult forCommitFail(
            Snapshot snapshot, List<SimpleFileEntry> baseDataFiles, Exception exception) {
        return new CommitFailRetryResult(snapshot, baseDataFiles, exception);
    }

    /**
     * 创建回滚失败的重试结果
     *
     * <p>当回滚COMPACT快照失败时使用。
     *
     * @param exception 导致失败的异常
     * @return 回滚重试结果
     */
    public static RetryCommitResult forRollback(Exception exception) {
        return new RollbackRetryResult(exception);
    }

    /**
     * 判断提交是否成功
     *
     * @return 始终返回false，表示提交失败需要重试
     */
    @Override
    public boolean isSuccess() {
        return false;
    }

    /**
     * 提交失败的重试结果
     *
     * <p>当提交操作失败时返回该结果，保留快照和数据信息用于重新检测冲突。
     *
     * <h3>保留信息</h3>
     * <ul>
     *   <li><b>latestSnapshot</b>：失败时的最新快照
     *   <li><b>baseDataFiles</b>：基线数据文件
     * </ul>
     *
     * <h3>重试策略</h3>
     * <p>使用这些信息可以：
     * <ul>
     *   <li>重新检测与最新快照的冲突
     *   <li>合并基线数据和新变更
     *   <li>决定是否继续重试或放弃
     * </ul>
     */
    public static class CommitFailRetryResult extends RetryCommitResult {

        /** 失败时的最新快照，可能为null */
        public final @Nullable Snapshot latestSnapshot;

        /** 基线数据文件，可能为null */
        public final @Nullable List<SimpleFileEntry> baseDataFiles;

        /**
         * 构造提交失败重试结果
         *
         * @param latestSnapshot 失败时的最新快照
         * @param baseDataFiles 基线数据文件
         * @param exception 导致失败的异常
         */
        private CommitFailRetryResult(
                @Nullable Snapshot latestSnapshot,
                @Nullable List<SimpleFileEntry> baseDataFiles,
                Exception exception) {
            super(exception);
            this.latestSnapshot = latestSnapshot;
            this.baseDataFiles = baseDataFiles;
        }
    }

    /**
     * 回滚失败的重试结果
     *
     * <p>当回滚COMPACT快照失败时返回该结果。
     *
     * <h3>使用场景</h3>
     * <p>在以下情况返回：
     * <ul>
     *   <li>尝试回滚COMPACT快照以解决冲突
     *   <li>回滚操作本身失败
     *   <li>需要直接重试提交而不依赖回滚
     * </ul>
     *
     * <h3>重试策略</h3>
     * <p>通常直接重新执行提交操作，不再尝试回滚。
     */
    public static class RollbackRetryResult extends RetryCommitResult {

        /**
         * 构造回滚重试结果
         *
         * @param exception 导致失败的异常
         */
        private RollbackRetryResult(Exception exception) {
            super(exception);
        }
    }
}
