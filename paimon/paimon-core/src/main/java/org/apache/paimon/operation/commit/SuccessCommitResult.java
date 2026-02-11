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
 * 成功提交结果
 *
 * <p>表示提交操作成功完成。
 *
 * <h2>使用场景</h2>
 * <p>当提交操作成功时返回该结果：
 * <ul>
 *   <li>快照已成功写入
 *   <li>Manifest文件已持久化
 *   <li>所有文件变更已记录
 * </ul>
 *
 * @see CommitResult 提交结果接口
 * @see RetryCommitResult 重试提交结果
 */
public class SuccessCommitResult implements CommitResult {

    /**
     * 判断提交是否成功
     *
     * @return 始终返回true，表示提交成功
     */
    @Override
    public boolean isSuccess() {
        return true;
    }
}
