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

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * CompletableFuture 工具类。
 *
 * <p>提供了扩展 {@link CompletableFuture} 使用的工具方法集合，包括异常完成、值转发等操作。
 *
 * <p>主要功能：
 * <ul>
 *   <li>创建异常完成的 Future - 用于快速创建失败的 Future
 *   <li>Future 值转发 - 将一个 Future 的结果转发到另一个 Future
 *   <li>条件完成 - 根据值或异常来完成 Future
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>异步编程 - 处理异步操作的结果和异常
 *   <li>Future 链式调用 - 在多个 Future 之间传递结果
 *   <li>错误处理 - 统一处理异步操作中的异常
 * </ul>
 *
 * @see CompletableFuture
 */
public class FutureUtils {

    /**
     * 返回一个异常完成的 {@link CompletableFuture}。
     *
     * <p>创建一个已经以异常状态完成的 Future，常用于快速返回失败结果。
     *
     * @param cause 用于完成 Future 的异常原因
     * @param <T> Future 的值类型
     * @return 异常完成的 CompletableFuture
     */
    public static <T> CompletableFuture<T> completedExceptionally(Throwable cause) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(cause);

        return result;
    }

    /**
     * 将源 Future 的值转发到目标 Future。
     *
     * <p>当源 Future 完成时（无论正常完成还是异常完成），将其结果转发给目标 Future。
     *
     * @param source 要转发值的源 Future
     * @param target 要转发值到的目标 Future
     * @param <T> 值的类型
     */
    public static <T> void forward(CompletableFuture<T> source, CompletableFuture<T> target) {
        source.whenComplete(forwardTo(target));
    }

    private static <T> BiConsumer<T, Throwable> forwardTo(CompletableFuture<T> target) {
        return (value, throwable) -> doForward(value, throwable, target);
    }

    /**
     * 使用给定的值或异常完成给定的 Future，具体取决于哪个参数不为 null。
     *
     * <p>如果 throwable 不为 null，则 Future 将异常完成；否则使用 value 正常完成。
     *
     * @param value 用于正常完成 Future 的值
     * @param throwable 用于异常完成 Future 的异常
     * @param target 要完成的目标 Future
     * @param <T> Future 的值类型
     */
    public static <T> void doForward(
            @Nullable T value, @Nullable Throwable throwable, CompletableFuture<T> target) {
        if (throwable != null) {
            target.completeExceptionally(throwable);
        } else {
            target.complete(value);
        }
    }
}
