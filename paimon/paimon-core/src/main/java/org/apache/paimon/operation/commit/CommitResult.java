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

/**
 * 提交结果接口
 *
 * <p>表示提交操作的结果。
 *
 * <h2>结果类型</h2>
 * <p>提交操作有三种可能的结果：
 * <ul>
 *   <li><b>成功</b>：{@link SuccessCommitResult} - 提交成功完成
 *   <li><b>需要重试</b>：{@link RetryCommitResult} - 提交失败，需要重试
 *       <ul>
 *         <li>提交失败重试：{@link RetryCommitResult.CommitFailRetryResult}
 *         <li>回滚重试：{@link RetryCommitResult.RollbackRetryResult}
 *       </ul>
 * </ul>
 *
 * <h2>使用模式</h2>
 * <p>通过 {@link #isSuccess()} 判断提交结果：
 * <pre>{@code
 * CommitResult result = commitOperation();
 * if (result.isSuccess()) {
 *     // 提交成功
 * } else {
 *     RetryCommitResult retry = (RetryCommitResult) result;
 *     // 处理重试逻辑
 * }
 * }</pre>
 *
 * @see SuccessCommitResult 成功结果
 * @see RetryCommitResult 重试结果
 */
public interface CommitResult {

    /**
     * 判断提交是否成功
     *
     * @return true表示提交成功，false表示需要重试
     */
    boolean isSuccess();
}
