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

package org.apache.paimon.options;

/**
 * 回退键类,表示配置选项的备用键。
 *
 * <p>当配置选项本身未被配置时,会回退到 FallbackKeys。这对于支持配置键的重命名或废弃非常有用。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>配置键重命名:支持新旧两个键名
 *   <li>配置键废弃:标记旧键为废弃,但仍然支持
 *   <li>配置键迁移:平滑过渡到新的配置键体系
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建配置选项,支持旧键名
 * ConfigOption<Integer> option = ConfigOptions
 *     .key("new.parallelism")
 *     .intType()
 *     .defaultValue(1)
 *     .withFallbackKeys("old.parallelism");  // 会创建 FallbackKey
 * }</pre>
 */
public class FallbackKey {

    // -------------------------
    //  工厂方法
    // -------------------------

    /**
     * 创建一个回退键(非废弃)。
     *
     * @param key 键名
     * @return 回退键实例
     */
    static FallbackKey createFallbackKey(String key) {
        return new FallbackKey(key, false);
    }

    /**
     * 创建一个废弃的键。
     *
     * @param key 键名
     * @return 废弃的回退键实例
     */
    static FallbackKey createDeprecatedKey(String key) {
        return new FallbackKey(key, true);
    }

    // ------------------------------------------------------------------------

    /** 回退键的键名 */
    private final String key;

    /** 是否为废弃的键 */
    private final boolean isDeprecated;

    /**
     * 获取键名。
     *
     * @return 键名
     */
    public String getKey() {
        return key;
    }

    /**
     * 判断是否为废弃的键。
     *
     * @return 如果是废弃的键返回 true,否则返回 false
     */
    public boolean isDeprecated() {
        return isDeprecated;
    }

    /**
     * 私有构造函数。
     *
     * @param key 键名
     * @param isDeprecated 是否为废弃的键
     */
    private FallbackKey(String key, boolean isDeprecated) {
        this.key = key;
        this.isDeprecated = isDeprecated;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == FallbackKey.class) {
            FallbackKey that = (FallbackKey) o;
            return this.key.equals(that.key) && (this.isDeprecated == that.isDeprecated);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + (isDeprecated ? 1 : 0);
    }

    @Override
    public String toString() {
        return String.format("{key=%s, isDeprecated=%s}", key, isDeprecated);
    }
}
