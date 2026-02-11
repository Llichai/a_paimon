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

package org.apache.paimon.table.sink;

/**
 * 标签回调接口。
 *
 * <p>在标签操作（创建、删除）完成后调用的回调接口。
 * 允许用户在标签操作后执行自定义的后处理逻辑。
 *
 * <p>典型使用场景：
 * <ul>
 *   <li><b>元数据同步</b>：
 *       <ul>
 *         <li>将标签信息同步到外部系统
 *         <li>更新外部 Catalog 的标签列表
 *         <li>同步到 Iceberg、Hudi 等其他表格式
 *       </ul>
 *   <li><b>监控告警</b>：
 *       <ul>
 *         <li>记录标签创建事件
 *         <li>发送标签变更通知
 *         <li>触发数据发布流程
 *       </ul>
 *   <li><b>生命周期管理</b>：
 *       <ul>
 *         <li>基于标签创建外部快照
 *         <li>触发数据备份任务
 *         <li>更新数据版本管理系统
 *       </ul>
 * </ul>
 *
 * <p>与 {@link CommitCallback} 的区别：
 * <ul>
 *   <li>{@link CommitCallback}：在数据提交后调用，保证被调用
 *   <li>{@link TagCallback}：在标签操作后调用，<b>不保证一定被调用</b>
 * </ul>
 *
 * <p>重要说明：
 * <ul>
 *   <li><b>无调用保证</b>：
 *       <ul>
 *         <li>标签回调不保证一定会被调用
 *         <li>如果标签操作后立即发生故障，回调可能不会执行
 *         <li>实现时需要考虑这种不确定性
 *       </ul>
 *   <li><b>幂等性建议</b>：
 *       <ul>
 *         <li>虽然不保证调用，但建议实现幂等性
 *         <li>避免在重试或恢复后产生副作用
 *       </ul>
 * </ul>
 *
 * <p>配置方式：
 * <pre>
 * CREATE TABLE t (...) WITH (
 *   'tag.callbacks' = 'com.example.MyTagCallback1,com.example.MyTagCallback2',
 *   'tag.callback.param' = 'param1,param2'
 * );
 * </pre>
 *
 * <p>实现示例：
 * <pre>
 * public class IcebergTagCallback implements TagCallback {
 *     private IcebergCatalog catalog;
 *
 *     {@literal @}Override
 *     public void notifyCreation(String tagName, long snapshotId) {
 *         // 在 Iceberg 中创建对应的标签
 *         catalog.createTag(tagName, snapshotId);
 *     }
 *
 *     {@literal @}Override
 *     public void notifyDeletion(String tagName) {
 *         // 在 Iceberg 中删除对应的标签
 *         catalog.deleteTag(tagName);
 *     }
 *
 *     {@literal @}Override
 *     public void close() {
 *         catalog.close();
 *     }
 * }
 * </pre>
 *
 * <p>与 Iceberg 的集成：
 * <ul>
 *   <li>Iceberg 的标签回调需要 snapshotId 参数
 *   <li>提供了重载方法 {@link #notifyCreation(String, long)}
 *   <li>默认实现委托给不带 snapshotId 的方法
 * </ul>
 *
 * @see CallbackUtils 回调加载工具
 * @see CommitCallback 提交回调接口
 */
public interface TagCallback extends AutoCloseable {

    /**
     * 标签创建通知（基础版本）。
     *
     * <p>当创建标签时调用此方法。默认实现，不包含快照ID参数。
     *
     * @param tagName 标签名称
     */
    void notifyCreation(String tagName);

    /**
     * 标签创建通知（扩展版本，支持 Iceberg）。
     *
     * <p>扩展方法，包含快照ID参数，用于与 Iceberg 等需要快照ID的系统集成。
     * 默认实现委托给基础版本的 {@link #notifyCreation(String)}。
     *
     * <p>使用场景：
     * <ul>
     *   <li>Iceberg 标签回调：需要快照ID来创建 Iceberg 标签
     *   <li>版本管理系统：需要关联快照ID
     *   <li>外部系统同步：需要精确的快照信息
     * </ul>
     *
     * @param tagName 标签名称
     * @param snapshotId 关联的快照ID
     */
    default void notifyCreation(String tagName, long snapshotId) {
        // 默认委托给不带 snapshotId 的方法，保持向后兼容
        notifyCreation(tagName);
    }

    /**
     * 标签删除通知。
     *
     * <p>当删除标签时调用此方法。
     *
     * @param tagName 被删除的标签名称
     */
    void notifyDeletion(String tagName);
}
