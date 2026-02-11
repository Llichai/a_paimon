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

package org.apache.paimon.mergetree;

import java.util.Objects;

/**
 * 带层级的 Sorted Run
 *
 * <p>为 {@link SortedRun} 添加层级信息，用于 LSM Tree 的分层存储。
 *
 * <p>使用场景：
 * <ul>
 *   <li>压缩策略：根据层级选择压缩单元
 *   <li>读取优化：根据层级顺序读取（Level-0 最新，高层级更旧）
 *   <li>状态管理：跟踪每层的文件分布
 * </ul>
 */
public class LevelSortedRun {

    /** 层级（0表示Level-0，越大越旧） */
    private final int level;

    /** Sorted Run */
    private final SortedRun run;

    /**
     * 构造带层级的 Sorted Run
     *
     * @param level 层级
     * @param run Sorted Run
     */
    public LevelSortedRun(int level, SortedRun run) {
        this.level = level;
        this.run = run;
    }

    /**
     * 获取层级
     *
     * @return 层级
     */
    public int level() {
        return level;
    }

    /**
     * 获取 Sorted Run
     *
     * @return Sorted Run
     */
    public SortedRun run() {
        return run;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LevelSortedRun that = (LevelSortedRun) o;
        return level == that.level && Objects.equals(run, that.run);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, run);
    }

    @Override
    public String toString() {
        return "LevelSortedRun{" + "level=" + level + ", run=" + run + '}';
    }
}
