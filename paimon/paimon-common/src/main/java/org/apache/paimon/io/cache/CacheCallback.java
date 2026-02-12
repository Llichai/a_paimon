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

package org.apache.paimon.io.cache;

/**
 * 缓存移除回调接口,在缓存项被淘汰时触发。
 *
 * <p>该接口用于在缓存项被移除时执行清理或通知操作,例如:
 * <ul>
 *   <li>释放相关资源</li>
 *   <li>更新统计信息</li>
 *   <li>记录日志</li>
 *   <li>触发其他连锁清理</li>
 * </ul>
 *
 * <h3>触发时机</h3>
 * <p>回调在以下情况触发:
 * <ul>
 *   <li><b>容量淘汰:</b> 缓存达到大小限制,LRU 淘汰旧项</li>
 *   <li><b>手动失效:</b> 调用 invalidate() 或 invalidateAll()</li>
 *   <li><b>覆盖更新:</b> 使用相同 key 放入新值</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 简单的日志回调
 * CacheCallback loggingCallback = key -> {
 *     System.out.println("Evicted: " + key);
 * };
 *
 * // 资源清理回调
 * CacheCallback cleanupCallback = key -> {
 *     // 关闭文件句柄
 *     if (key instanceof PositionCacheKey) {
 *         closeFile(((PositionCacheKey) key).filePath);
 *     }
 * };
 *
 * // 统计回调
 * AtomicLong evictionCount = new AtomicLong();
 * CacheCallback statsCallback = key -> {
 *     evictionCount.incrementAndGet();
 * };
 *
 * // 使用回调创建缓存值
 * CacheValue value = new CacheValue(segment, statsCallback);
 * }</pre>
 *
 * <h3>实现注意事项</h3>
 * <ul>
 *   <li><b>快速执行:</b> 回调应该快速完成,避免阻塞缓存操作</li>
 *   <li><b>异常处理:</b> 不应抛出未捕获异常,会影响缓存稳定性</li>
 *   <li><b>线程安全:</b> 回调可能在任意线程中执行</li>
 *   <li><b>避免递归:</b> 不要在回调中再次操作同一缓存</li>
 * </ul>
 *
 * @see Cache
 * @see Cache.CacheValue
 */
public interface CacheCallback {

    /**
     * 当缓存项被移除时调用。
     *
     * <p>该方法在缓存项被淘汰时同步调用。实现应该快速完成并处理所有异常。
     *
     * <p><b>警告:</b> 不要在此方法中执行耗时操作或阻塞调用,
     * 否则会严重影响缓存性能。
     *
     * @param key 被移除的缓存键
     */
    void onRemoval(CacheKey key);
}
