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

package org.apache.paimon.memory;

import org.apache.paimon.utils.Preconditions;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 内存操作工具类。
 *
 * <p>该类提供了访问和使用 sun.misc.Unsafe 的能力,支持底层内存操作。
 *
 * <h2>核心功能</h2>
 *
 * <ul>
 *   <li>获取 Unsafe 实例,用于直接内存访问
 *   <li>获取平台的本地字节序
 *   <li>获取 DirectByteBuffer 的内存地址
 *   <li>使用反射获取类字段偏移量
 * </ul>
 *
 * <h2>安全性警告</h2>
 *
 * <p>使用 Unsafe 进行内存操作绕过了 JVM 的安全检查,需要特别小心:
 *
 * <ul>
 *   <li>错误的偏移量会导致内存越界和 JVM 崩溃
 *   <li>直接内存访问不受 GC 保护,可能导致悬空指针
 *   <li>仅在性能关键路径且充分测试的情况下使用
 * </ul>
 */
public class MemoryUtils {

    /** Unsafe 实例,用于执行本地内存访问。 */
    @SuppressWarnings({"restriction", "UseOfSunClasses"})
    public static final sun.misc.Unsafe UNSAFE = getUnsafe();

    /** 平台的本地字节序。 */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    /** ByteBuffer 中 address 字段的偏移量,用于获取堆外内存地址。 */
    private static final long BUFFER_ADDRESS_FIELD_OFFSET =
            getClassFieldOffset(Buffer.class, "address");

    /**
     * 获取 Unsafe 实例。
     *
     * <p>通过反射访问 sun.misc.Unsafe 的私有静态字段 "theUnsafe"。
     *
     * @return Unsafe 实例
     * @throws Error 如果无法访问 Unsafe(权限不足、字段不存在等)
     */
    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        try {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (SecurityException e) {
            throw new Error(
                    "Could not access the sun.misc.Unsafe handle, permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error("The static handle field in sun.misc.Unsafe was not found.", e);
        } catch (IllegalArgumentException e) {
            throw new Error("Bug: Illegal argument reflection access for static field.", e);
        } catch (IllegalAccessException e) {
            throw new Error("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
        } catch (Throwable t) {
            throw new Error(
                    "Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
        }
    }

    /**
     * 获取类中字段的内存偏移量。
     *
     * <p>该偏移量可用于 Unsafe 的 get/put 方法直接访问对象字段。
     *
     * @param cl 目标类
     * @param fieldName 字段名称
     * @return 字段的内存偏移量
     * @throws Error 如果无法获取字段偏移量(字段不存在、权限不足等)
     */
    private static long getClassFieldOffset(
            @SuppressWarnings("SameParameterValue") Class<?> cl, String fieldName) {
        try {
            return UNSAFE.objectFieldOffset(cl.getDeclaredField(fieldName));
        } catch (SecurityException e) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName)
                            + ", permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error(getClassFieldOffsetErrorMessage(cl, fieldName), e);
        } catch (Throwable t) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName) + ", unclassified error", t);
        }
    }

    private static String getClassFieldOffsetErrorMessage(Class<?> cl, String fieldName) {
        return "Could not get field '"
                + fieldName
                + "' offset in class '"
                + cl
                + "' for unsafe operations";
    }

    private static Class<?> getClassByName(
            @SuppressWarnings("SameParameterValue") String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new Error("Could not find class '" + className + "' for unsafe operations.", e);
        }
    }

    /**
     * 获取 ByteBuffer 包装的本地内存地址。
     *
     * <p>该方法仅适用于 DirectByteBuffer,返回其底层的堆外内存地址。
     *
     * @param buffer 必须是 DirectByteBuffer
     * @return 堆外内存的起始地址
     * @throws IllegalArgumentException 如果 buffer 不是 DirectByteBuffer
     * @throws IllegalStateException 如果地址无效(为负或超出范围)
     * @throws Error 如果无法访问 address 字段
     */
    static long getByteBufferAddress(ByteBuffer buffer) {
        Preconditions.checkNotNull(buffer, "buffer is null");
        Preconditions.checkArgument(
                buffer.isDirect(), "Can't get address of a non-direct ByteBuffer.");

        long offHeapAddress;
        try {
            offHeapAddress = UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
        } catch (Throwable t) {
            throw new Error("Could not access direct byte buffer address field.", t);
        }

        Preconditions.checkState(offHeapAddress > 0, "negative pointer or size");
        Preconditions.checkState(
                offHeapAddress < Long.MAX_VALUE - Integer.MAX_VALUE,
                "Segment initialized with too large address: "
                        + offHeapAddress
                        + " ; Max allowed address is "
                        + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));

        return offHeapAddress;
    }

    /**
     * 私有构造函数,防止实例化。
     *
     * <p>该类是工具类,所有方法都是静态的。
     */
    private MemoryUtils() {}
}
