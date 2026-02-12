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

package org.apache.paimon.utils;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 函数式编程实用工具类,表示两种可能类型之一的值。
 *
 * <p>这是一个可辨识联合类型,可以持有 Left 值(通常表示错误或失败情况)
 * 或 Right 值(通常表示成功或正确结果)。
 *
 * <p>Either 类型在函数式编程中常用于处理可能成功或失败的操作,
 * 提供了一种类型安全的替代方案来处理异常或 null 值。
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 创建 Left (错误情况)
 * Either<String, Integer> error = Either.left("Invalid input");
 *
 * // 创建 Right (成功情况)
 * Either<String, Integer> success = Either.right(42);
 *
 * // 检查和处理结果
 * if (result.isRight()) {
 *     Integer value = result.getRight();
 *     // 处理成功结果
 * } else {
 *     String error = result.getLeft();
 *     // 处理错误情况
 * }
 *
 * // 使用函数式风格处理
 * result.ifRight(value -> System.out.println("Success: " + value));
 * result.ifLeft(error -> System.err.println("Error: " + error));
 * }</pre>
 *
 * @param <L> Left 值的类型(通常表示错误/失败)
 * @param <R> Right 值的类型(通常表示成功/结果)
 */
public abstract class Either<L, R> {

    /** 私有构造函数,防止直接实例化 */
    private Either() {}

    /**
     * 创建 Left 侧的 Either 实例的静态工厂方法。
     *
     * <p>通常表示失败或错误情况。
     *
     * @param value Left 侧的值
     * @param <L> Left 值的类型
     * @param <R> Right 值的类型
     * @return 新的 Left 实例
     * @throws NullPointerException 如果 value 为 null
     */
    public static <L, R> Either<L, R> left(L value) {
        return new Left<>(value);
    }

    /**
     * 创建 Right 侧的 Either 实例的静态工厂方法。
     *
     * <p>通常表示成功或正确的结果。
     *
     * @param value Right 侧的值
     * @param <L> Left 值的类型
     * @param <R> Right 值的类型
     * @return 新的 Right 实例
     * @throws NullPointerException 如果 value 为 null
     */
    public static <L, R> Either<L, R> right(R value) {
        return new Right<>(value);
    }

    /**
     * 检查此实例是否为 Left。
     *
     * @return 如果是 Left 则返回 true,否则返回 false
     */
    public abstract boolean isLeft();

    /**
     * 检查此实例是否为 Right。
     *
     * @return 如果是 Right 则返回 true,否则返回 false
     */
    public abstract boolean isRight();

    /**
     * 如果这是 Left 实例,则返回 left 值。
     *
     * @return Left 值
     * @throws NoSuchElementException 如果这是 Right 实例
     */
    public abstract L getLeft();

    /**
     * 如果这是 Right 实例,则返回 right 值。
     *
     * @return Right 值
     * @throws NoSuchElementException 如果这是 Left 实例
     */
    public abstract R getRight();

    /**
     * 如果这是 Left,则对其值执行给定的操作。
     *
     * @param action 要执行的消费者函数
     */
    public abstract void ifLeft(Consumer<L> action);

    /**
     * 如果这是 Right,则对其值执行给定的操作。
     *
     * @param action 要执行的消费者函数
     */
    public abstract void ifRight(Consumer<R> action);

    /**
     * 表示 Either 的 Left 状态的私有静态内部类。
     *
     * <p>通常用于表示错误或失败情况。
     */
    private static final class Left<L, R> extends Either<L, R> {
        private final L value;

        private Left(L value) {
            this.value = Objects.requireNonNull(value, "Left value cannot be null");
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public boolean isRight() {
            return false;
        }

        @Override
        public L getLeft() {
            return value;
        }

        @Override
        public R getRight() {
            throw new NoSuchElementException("Cannot call getRight() on a Left instance");
        }

        @Override
        public void ifLeft(Consumer<L> action) {
            action.accept(value);
        }

        @Override
        public void ifRight(Consumer<R> action) {
            // 不执行任何操作
        }

        @Override
        public String toString() {
            return "Left(" + value + ")";
        }
    }

    /**
     * 表示 Either 的 Right 状态的私有静态内部类。
     *
     * <p>通常用于表示成功或正确的结果。
     */
    private static final class Right<L, R> extends Either<L, R> {
        private final R value;

        private Right(R value) {
            this.value = Objects.requireNonNull(value, "Right value cannot be null");
        }

        @Override
        public boolean isLeft() {
            return false;
        }

        @Override
        public boolean isRight() {
            return true;
        }

        @Override
        public L getLeft() {
            throw new NoSuchElementException("Cannot call getLeft() on a Right instance");
        }

        @Override
        public R getRight() {
            return value;
        }

        @Override
        public void ifLeft(Consumer<L> action) {
            // 不执行任何操作
        }

        @Override
        public void ifRight(Consumer<R> action) {
            action.accept(value);
        }

        @Override
        public String toString() {
            return "Right(" + value + ")";
        }
    }
}
