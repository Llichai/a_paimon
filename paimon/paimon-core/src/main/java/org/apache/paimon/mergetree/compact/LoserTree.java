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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.ExceptionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 败者树（Loser Tree）的变种实现
 *
 * <p>在 LSM Tree 架构中，多个 {@link RecordReader} 会包含重复的键，这些键需要合并。
 * 在败者树中，我们按键的顺序返回，但由于返回的对象可能在 {@link RecordReader} 或
 * {@link MergeFunction} 中被复用，对于单个 {@link RecordReader}，我们不能在返回
 * 一个键后立即获取下一个键，而需要等到所有 {@link RecordReader} 中相同的键都返回后，
 * 才能继续处理下一个键。
 *
 * <p>核心思想：
 * <ul>
 *   <li>构建败者树的过程与常规败者树相同
 *   <li>在调整树的过程中，需要记录相同键的索引和胜者/败者的状态
 *   <li>通过状态机制快速调整胜者位置，避免重复比较
 * </ul>
 *
 * <p>与常规败者树的区别：
 * <ul>
 *   <li>常规败者树：每次返回最小值后立即从对应 reader 取下一个值
 *   <li>变种败者树：等所有相同键都返回后，才从对应 readers 取下一个值（避免对象复用问题）
 * </ul>
 *
 * <p>详细设计参考：https://cwiki.apache.org/confluence/x/9Ak0Dw
 *
 * @param <T> 记录类型（通常是 KeyValue）
 */
public class LoserTree<T> implements Closeable {
    /** 败者树数组（tree[0] 存储全局胜者，tree[parent] 存储父节点的败者） */
    private final int[] tree;
    /** 叶子节点数量（输入 reader 数量） */
    private final int size;
    /** 叶子节点列表（包装 RecordReader 的迭代器） */
    private final List<LeafIterator<T>> leaves;

    /**
     * 第一级比较器（比较用户键）
     *
     * <p>如果 comparator.compare('a', 'b') > 0，则 'a' 是胜者。
     * 在以下实现中，我们始终让 'a' 表示父节点。
     */
    private final Comparator<T> firstComparator;

    /** 第二级比较器（比较序列号） */
    private final Comparator<T> secondComparator;

    /** 是否已初始化 */
    private boolean initialized;

    /**
     * 构造败者树
     *
     * @param nextBatchReaders RecordReader 列表
     * @param firstComparator 第一级比较器（用户键）
     * @param secondComparator 第二级比较器（序列号）
     */
    public LoserTree(
            List<RecordReader<T>> nextBatchReaders,
            Comparator<T> firstComparator,
            Comparator<T> secondComparator) {
        this.size = nextBatchReaders.size();
        this.leaves = new ArrayList<>(size);
        this.tree = new int[size];
        // null 值处理：如果 e1 和 e2 都为 null，谁成为胜者无关紧要
        // 如果 firstComparator 返回0，表示必须使用 secondComparator 再次比较
        this.firstComparator =
                (e1, e2) -> e1 == null ? -1 : (e2 == null ? 1 : firstComparator.compare(e1, e2));
        this.secondComparator =
                (e1, e2) -> e1 == null ? -1 : (e2 == null ? 1 : secondComparator.compare(e1, e2));
        this.initialized = false;

        // 创建叶子节点
        for (RecordReader<T> reader : nextBatchReaders) {
            LeafIterator<T> iterator = new LeafIterator<>(reader);
            this.leaves.add(iterator);
        }
    }

    /**
     * 初始化败者树（与常规败者树相同）
     *
     * <p>构建过程：
     * <ol>
     *   <li>将所有 tree 节点初始化为 -1
     *   <li>从最后一个叶子节点开始，逆序读取第一个值
     *   <li>对每个叶子节点调用 adjust，构建败者树
     * </ol>
     *
     * @throws IOException IO 异常
     */
    public void initializeIfNeeded() throws IOException {
        if (!initialized) {
            Arrays.fill(tree, -1);
            for (int i = size - 1; i >= 0; i--) {
                leaves.get(i).advanceIfAvailable(); // 读取第一个值
                adjust(i); // 调整树
            }
            initialized = true;
        }
    }

    /**
     * 调整到下一轮（处理下一个不同的键）
     *
     * <p>当所有相同键的记录都被 pop 后，需要调用此方法：
     * <ol>
     *   <li>检查当前胜者的状态
     *   <li>如果是 WINNER_POPPED，从对应 reader 读取下一个值
     *   <li>调整树，找到新的全局胜者
     * </ol>
     *
     * @throws IOException IO 异常
     */
    public void adjustForNextLoop() throws IOException {
        LeafIterator<T> winner = leaves.get(tree[0]);
        // 循环处理所有已 pop 的胜者
        while (winner.state == State.WINNER_POPPED) {
            winner.advanceIfAvailable(); // 读取下一个值
            adjust(tree[0]); // 调整树
            winner = leaves.get(tree[0]); // 更新胜者
        }
    }

    /**
     * 弹出当前胜者并更新其状态为 {@link State#WINNER_POPPED}
     *
     * <p>工作流程：
     * <ol>
     *   <li>检查胜者状态
     *   <li>如果已 popped，返回 null（所有相同键已处理）
     *   <li>否则，pop 胜者并调整树（找到下一个相同键的胜者）
     * </ol>
     *
     * @return 当前胜者或 null（已全部处理）
     */
    public T popWinner() {
        LeafIterator<T> winner = leaves.get(tree[0]);
        if (winner.state == State.WINNER_POPPED) {
            // 胜者已被 pop，说明所有相同键已处理
            return null;
        }
        T result = winner.pop(); // pop 并标记为 WINNER_POPPED
        adjust(tree[0]); // 调整树
        return result;
    }

    /**
     * 查看当前胜者（不弹出）
     *
     * <p>主要用于键比较
     *
     * @return 当前胜者或 null（已全部处理）
     */
    public T peekWinner() {
        return leaves.get(tree[0]).state != State.WINNER_POPPED ? leaves.get(tree[0]).peek() : null;
    }

    /**
     * 从底向上调整胜者位置
     *
     * <p>使用不同的 {@link State}，可以快速判断当前所有相同键是否已全部处理。
     *
     * <p>调整过程：
     * <ol>
     *   <li>从叶子节点（winner）开始向上遍历到根节点
     *   <li>在每一层比较胜者和父节点的败者
     *   <li>根据状态决定是否交换位置
     *   <li>更新节点状态
     * </ol>
     *
     * @param winner 待调整的胜者索引
     */
    private void adjust(int winner) {
        // 从底向上遍历树（parent = (winner + size) / 2）
        for (int parent = (winner + this.size) / 2; parent > 0 && winner >= 0; parent /= 2) {
            LeafIterator<T> winnerNode = leaves.get(winner);
            LeafIterator<T> parentNode;
            if (this.tree[parent] == -1) {
                // 初始化树：父节点为空，标记胜者为败者（新键）
                winnerNode.state = State.LOSER_WITH_NEW_KEY;
            } else {
                parentNode = leaves.get(this.tree[parent]);
                // 根据胜者状态分别处理
                switch (winnerNode.state) {
                    case WINNER_WITH_NEW_KEY:
                        // 胜者是新键，需要与父节点比较
                        adjustWithNewWinnerKey(parent, parentNode, winnerNode);
                        break;
                    case WINNER_WITH_SAME_KEY:
                        // 胜者与全局胜者的键相同，只需比较序列号
                        adjustWithSameWinnerKey(parent, parentNode, winnerNode);
                        break;
                    case WINNER_POPPED:
                        // 胜者已被 pop，需要快速找到下一个相同键
                        if (winnerNode.firstSameKeyIndex < 0) {
                            // 快速路径：树中没有相同键，停止调整
                            parent = -1;
                        } else {
                            // 快速路径：直接与记录的相同键位置交换，无需逐层比较
                            parent = winnerNode.firstSameKeyIndex;
                            parentNode = leaves.get(this.tree[parent]);
                            winnerNode.state = State.LOSER_POPPED;
                            parentNode.state = State.WINNER_WITH_SAME_KEY;
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "unknown state for " + winnerNode.state.name());
                }
            }

            // 如果胜者输了，交换节点
            if (!winnerNode.state.isWinner()) {
                int tmp = winner;
                winner = this.tree[parent];
                this.tree[parent] = tmp;
            }
        }
        this.tree[0] = winner; // 更新全局胜者
    }

    /**
     * 胜者节点与全局胜者的用户键相同
     *
     * <p>只需比较序列号，不需要比较用户键
     *
     * @param index 父节点索引
     * @param parentNode 父节点（败者）
     * @param winnerNode 胜者节点
     */
    private void adjustWithSameWinnerKey(
            int index, LeafIterator<T> parentNode, LeafIterator<T> winnerNode) {
        switch (parentNode.state) {
            case LOSER_WITH_SAME_KEY:
                // 父节点败者的键与当前胜者的键相同，只需比较序列号
                T parentKey = parentNode.peek();
                T childKey = winnerNode.peek();
                int secondResult = secondComparator.compare(parentKey, childKey);
                if (secondResult > 0) {
                    // 父节点序列号更大，成为新胜者
                    parentNode.state = State.WINNER_WITH_SAME_KEY;
                    winnerNode.state = State.LOSER_WITH_SAME_KEY;
                    parentNode.setFirstSameKeyIndex(index);
                } else {
                    // 当前胜者序列号更大，保持胜者
                    winnerNode.setFirstSameKeyIndex(index);
                }
                return;
            case LOSER_WITH_NEW_KEY:
            case LOSER_POPPED:
                // 父节点不是相同键或已 pop，无需比较
                return;
            default:
                throw new UnsupportedOperationException(
                        "unknown state for " + parentNode.state.name());
        }
    }

    /**
     * 新局部胜者节点的用户键与前一个全局胜者不同
     *
     * <p>需要完整比较用户键和序列号
     *
     * @param index 父节点索引
     * @param parentNode 父节点（败者）
     * @param winnerNode 胜者节点
     */
    private void adjustWithNewWinnerKey(
            int index, LeafIterator<T> parentNode, LeafIterator<T> winnerNode) {
        switch (parentNode.state) {
            case LOSER_WITH_NEW_KEY:
                // 新胜者也是新键，需要比较
                T parentKey = parentNode.peek();
                T childKey = winnerNode.peek();
                int firstResult = firstComparator.compare(parentKey, childKey);
                if (firstResult == 0) {
                    // 键相同，需要更新节点状态并记录相同键的索引
                    int secondResult = secondComparator.compare(parentKey, childKey);
                    if (secondResult < 0) {
                        // 当前胜者序列号更大
                        parentNode.state = State.LOSER_WITH_SAME_KEY;
                        winnerNode.setFirstSameKeyIndex(index);
                    } else {
                        // 父节点序列号更大，成为新胜者
                        winnerNode.state = State.LOSER_WITH_SAME_KEY;
                        parentNode.state = State.WINNER_WITH_NEW_KEY;
                        parentNode.setFirstSameKeyIndex(index);
                    }
                } else if (firstResult > 0) {
                    // 两个键完全不同，只需更新状态
                    parentNode.state = State.WINNER_WITH_NEW_KEY;
                    winnerNode.state = State.LOSER_WITH_NEW_KEY;
                }
                return;
            case LOSER_WITH_SAME_KEY:
                // WINNER_WITH_NEW_KEY 状态的节点不能遇到 LOSER_WITH_SAME_KEY 状态的节点
                throw new RuntimeException(
                        "This is a bug. Please file an issue. A node in the WINNER_WITH_NEW_KEY "
                                + "state cannot encounter a node in the LOSER_WITH_SAME_KEY state.");
            case LOSER_POPPED:
                // 这种情况只会在 adjustForNextLoop 中发生
                parentNode.state = State.WINNER_POPPED;
                parentNode.firstSameKeyIndex = -1;
                winnerNode.state = State.LOSER_WITH_NEW_KEY;
                return;
            default:
                throw new UnsupportedOperationException(
                        "unknown state for " + parentNode.state.name());
        }
    }

    /**
     * 关闭败者树，释放所有资源
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (LeafIterator<T> iterator : leaves) {
            try {
                iterator.close();
            } catch (IOException e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * 叶子节点迭代器
     *
     * <p>包装 {@link RecordReader}，管理批次读取和状态
     */
    private static class LeafIterator<T> implements Closeable {
        /** 批次记录读取器 */
        private final RecordReader<T> reader;

        /** 当前批次使用的迭代器 */
        private RecordReader.RecordIterator<T> iterator;

        /** 当前最小的 KeyValue */
        private T kv;

        /** 标记是否访问完成 */
        private boolean endOfInput;

        /** 第一个相同键胜者的索引（用于快速调整） */
        private int firstSameKeyIndex;

        /** 当前节点的状态 */
        private State state;

        /**
         * 构造叶子迭代器
         *
         * @param reader 记录读取器
         */
        private LeafIterator(RecordReader<T> reader) {
            this.reader = reader;
            this.endOfInput = false;
            this.firstSameKeyIndex = -1;
            this.state = State.WINNER_WITH_NEW_KEY;
        }

        /**
         * 查看当前值（不弹出）
         *
         * @return 当前值
         */
        public T peek() {
            return kv;
        }

        /**
         * 弹出当前值并标记为已弹出
         *
         * @return 当前值
         */
        public T pop() {
            this.state = State.WINNER_POPPED;
            return kv;
        }

        /**
         * 设置第一个相同键的索引（仅设置一次）
         *
         * @param index 索引
         */
        public void setFirstSameKeyIndex(int index) {
            if (firstSameKeyIndex == -1) {
                firstSameKeyIndex = index;
            }
        }

        /**
         * 读取下一个值（如果可用）
         *
         * <p>逻辑：
         * <ol>
         *   <li>重置状态
         *   <li>尝试从当前批次读取
         *   <li>如果当前批次为空，读取下一批次
         *   <li>如果所有批次都为空，标记为结束
         * </ol>
         *
         * @throws IOException IO 异常
         */
        public void advanceIfAvailable() throws IOException {
            this.firstSameKeyIndex = -1; // 重置相同键索引
            this.state = State.WINNER_WITH_NEW_KEY; // 重置状态
            if (iterator == null || (kv = iterator.next()) == null) {
                while (!endOfInput) {
                    if (iterator != null) {
                        iterator.releaseBatch(); // 释放当前批次
                        iterator = null;
                    }

                    iterator = reader.readBatch(); // 读取下一批次
                    if (iterator == null) {
                        // 所有批次读完
                        endOfInput = true;
                        kv = null;
                        reader.close();
                    } else if ((kv = iterator.next()) != null) {
                        // 成功读取到值
                        break;
                    }
                }
            }
        }

        /**
         * 关闭迭代器
         *
         * @throws IOException IO 异常
         */
        @Override
        public void close() throws IOException {
            if (this.iterator != null) {
                this.iterator.releaseBatch();
                this.iterator = null;
            }
            this.reader.close();
        }
    }

    /**
     * 败者树节点状态枚举
     *
     * <p>六种状态：
     * <ul>
     *   <li>LOSER_WITH_NEW_KEY：败者，新键
     *   <li>LOSER_WITH_SAME_KEY：败者，与全局胜者键相同
     *   <li>LOSER_POPPED：败者，已被弹出
     *   <li>WINNER_WITH_NEW_KEY：胜者，新键
     *   <li>WINNER_WITH_SAME_KEY：胜者，与全局胜者键相同
     *   <li>WINNER_POPPED：胜者，已被弹出
     * </ul>
     */
    private enum State {
        LOSER_WITH_NEW_KEY(false),   // 败者：新键
        LOSER_WITH_SAME_KEY(false),  // 败者：相同键
        LOSER_POPPED(false),         // 败者：已弹出
        WINNER_WITH_NEW_KEY(true),   // 胜者：新键
        WINNER_WITH_SAME_KEY(true),  // 胜者：相同键
        WINNER_POPPED(true);         // 胜者：已弹出

        /** 是否为胜者 */
        private final boolean winner;

        /**
         * 构造状态
         *
         * @param winner 是否为胜者
         */
        State(boolean winner) {
            this.winner = winner;
        }

        /**
         * 判断是否为胜者
         *
         * @return 是否为胜者
         */
        public boolean isWinner() {
            return winner;
        }
    }
}
