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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Objects;

import static org.apache.paimon.memory.MemoryUtils.getByteBufferAddress;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * 内存段类,代表一块连续的内存区域。
 *
 * <p>该类是 Paimon 内存管理的核心组件,提供对堆内存(byte[])和堆外内存(DirectByteBuffer)的统一抽象。
 * 通过使用 Unsafe 直接操作内存,可以实现高性能的内存访问,避免 JVM 的边界检查和对象创建开销。
 *
 * <h2>内存类型</h2>
 *
 * <ul>
 *   <li>堆内存:基于 byte[] 数组,受 JVM 垃圾回收器管理
 *   <li>堆外内存:基于 DirectByteBuffer,不受 GC 影响,需要手动释放
 * </ul>
 *
 * <h2>性能考虑</h2>
 *
 * <ul>
 *   <li>使用 sun.misc.Unsafe 进行直接内存访问,避免数组边界检查
 *   <li>提供批量读写方法,减少方法调用开销
 *   <li>支持字节序转换(大端/小端),优化跨平台兼容性
 *   <li>提供内存比较和交换的高效实现,每次处理 8 字节
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的。多线程访问同一个 MemorySegment 需要外部同步。
 *
 * @since 0.4.0
 */
@Public
public final class MemorySegment {

    /** Unsafe 实例,用于直接内存访问操作。 */
    public static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    /** byte 数组的基础偏移量,用于 Unsafe 访问数组元素。 */
    public static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    /** 当前平台是否使用小端字节序。小端字节序下,低位字节存储在低地址。 */
    public static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

    /** 堆内存数组。如果该字段非空,则表示这是堆内存段。 */
    @Nullable private byte[] heapMemory;

    /** 堆外内存缓冲区。如果该字段非空,则表示这是堆外内存段。 */
    @Nullable private ByteBuffer offHeapBuffer;

    /** 内存地址。对于堆内存,这是相对于 heapMemory 数组的偏移量;对于堆外内存,这是绝对地址。 */
    private long address;

    /** 内存段的大小(字节数)。 */
    private final int size;

    /**
     * 私有构造函数,用于创建内存段实例。
     *
     * @param heapMemory 堆内存数组,如果是堆外内存则为 null
     * @param offHeapBuffer 堆外内存缓冲区,如果是堆内存则为 null
     * @param address 内存地址
     * @param size 内存段大小
     */
    private MemorySegment(
            @Nullable byte[] heapMemory,
            @Nullable ByteBuffer offHeapBuffer,
            long address,
            int size) {
        this.heapMemory = heapMemory;
        this.offHeapBuffer = offHeapBuffer;
        this.address = address;
        this.size = size;
    }

    /**
     * 包装一个 byte 数组为内存段。
     *
     * @param buffer 要包装的 byte 数组
     * @return 新的堆内存段
     */
    public static MemorySegment wrap(byte[] buffer) {
        return new MemorySegment(buffer, null, BYTE_ARRAY_BASE_OFFSET, buffer.length);
    }

    /**
     * 包装一个堆外 ByteBuffer 为内存段。
     *
     * @param buffer 要包装的 DirectByteBuffer
     * @return 新的堆外内存段
     */
    public static MemorySegment wrapOffHeapMemory(ByteBuffer buffer) {
        return new MemorySegment(null, buffer, getByteBufferAddress(buffer), buffer.capacity());
    }

    /**
     * 分配指定大小的堆内存段。
     *
     * @param size 内存段大小(字节)
     * @return 新分配的堆内存段
     */
    public static MemorySegment allocateHeapMemory(int size) {
        return wrap(new byte[size]);
    }

    /**
     * 分配指定大小的堆外内存段。
     *
     * <p>注意:堆外内存不受 GC 管理,需要手动释放,否则会导致内存泄漏。
     *
     * @param size 内存段大小(字节)
     * @return 新分配的堆外内存段
     */
    public static MemorySegment allocateOffHeapMemory(int size) {
        return wrapOffHeapMemory(ByteBuffer.allocateDirect(size));
    }

    /**
     * 获取内存段的大小。
     *
     * @return 内存段大小(字节)
     */
    public int size() {
        return size;
    }

    /**
     * 检查该内存段是否为堆外内存。
     *
     * @return 如果是堆外内存返回 true,堆内存返回 false
     */
    public boolean isOffHeap() {
        return heapMemory == null;
    }

    /**
     * 获取底层的堆内存数组。
     *
     * <p>只有堆内存段才能调用此方法,堆外内存段会抛出异常。
     *
     * @return 底层的 byte 数组
     * @throws IllegalStateException 如果该内存段不是堆内存
     */
    public byte[] getArray() {
        if (heapMemory != null) {
            return heapMemory;
        } else {
            throw new IllegalStateException("Memory segment does not represent heap memory");
        }
    }

    /**
     * 将内存段的指定区域包装为 ByteBuffer。
     *
     * @param offset 起始偏移量
     * @param length 长度
     * @return 包装后的 ByteBuffer
     */
    public ByteBuffer wrap(int offset, int length) {
        return wrapInternal(offset, length);
    }

    /**
     * 内部方法:将内存段的指定区域包装为 ByteBuffer。
     *
     * @param offset 起始偏移量
     * @param length 长度
     * @return 包装后的 ByteBuffer
     * @throws IndexOutOfBoundsException 如果偏移量或长度无效
     */
    private ByteBuffer wrapInternal(int offset, int length) {
        if (heapMemory != null) {
            return ByteBuffer.wrap(heapMemory, offset, length);
        } else {
            try {
                ByteBuffer wrapper = Objects.requireNonNull(offHeapBuffer).duplicate();
                wrapper.limit(offset + length);
                wrapper.position(offset);
                return wrapper;
            } catch (IllegalArgumentException e) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    /**
     * 读取指定位置的 byte 值。
     *
     * <p>使用 Unsafe 进行直接内存访问,无边界检查,性能优于数组访问。
     *
     * @param index 读取位置的索引
     * @return byte 值
     */
    public byte get(int index) {
        return UNSAFE.getByte(heapMemory, address + index);
    }

    /**
     * 写入 byte 值到指定位置。
     *
     * @param index 写入位置的索引
     * @param b 要写入的 byte 值
     */
    public void put(int index, byte b) {
        UNSAFE.putByte(heapMemory, address + index, b);
    }

    /**
     * 从指定位置读取字节数组。
     *
     * @param index 起始位置索引
     * @param dst 目标字节数组
     */
    public void get(int index, byte[] dst) {
        get(index, dst, 0, dst.length);
    }

    /**
     * 将字节数组写入指定位置。
     *
     * @param index 起始位置索引
     * @param src 源字节数组
     */
    public void put(int index, byte[] src) {
        put(index, src, 0, src.length);
    }

    /**
     * 从指定位置读取字节到目标数组的指定位置。
     *
     * <p>使用 Unsafe.copyMemory 进行批量拷贝,性能优于逐字节复制。
     *
     * @param index 内存段起始位置索引
     * @param dst 目标字节数组
     * @param offset 目标数组起始偏移量
     * @param length 要读取的字节数
     * @throws IndexOutOfBoundsException 如果索引越界
     */
    public void get(int index, byte[] dst, int offset, int length) {
        // check the byte array offset and length and the status
        if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        UNSAFE.copyMemory(
                heapMemory, address + index, dst, BYTE_ARRAY_BASE_OFFSET + offset, length);
    }

    /**
     * 将源数组的指定区域写入内存段的指定位置。
     *
     * <p>使用 Unsafe.copyMemory 进行批量拷贝,性能优于逐字节复制。
     *
     * @param index 内存段起始位置索引
     * @param src 源字节数组
     * @param offset 源数组起始偏移量
     * @param length 要写入的字节数
     * @throws IndexOutOfBoundsException 如果索引越界
     */
    public void put(int index, byte[] src, int offset, int length) {
        // check the byte array offset and length
        if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        UNSAFE.copyMemory(
                src, BYTE_ARRAY_BASE_OFFSET + offset, heapMemory, address + index, length);
    }

    /**
     * 读取指定位置的 boolean 值。
     *
     * @param index 读取位置的索引
     * @return boolean 值(非零为 true)
     */
    public boolean getBoolean(int index) {
        return get(index) != 0;
    }

    /**
     * 写入 boolean 值到指定位置。
     *
     * @param index 写入位置的索引
     * @param value 要写入的 boolean 值
     */
    public void putBoolean(int index, boolean value) {
        put(index, (byte) (value ? 1 : 0));
    }

    /**
     * 读取指定位置的 char 值(使用本地字节序)。
     *
     * @param index 读取位置的索引
     * @return char 值
     */
    public char getChar(int index) {
        return UNSAFE.getChar(heapMemory, address + index);
    }

    /**
     * 读取指定位置的 char 值(小端字节序)。
     *
     * <p>如果当前平台是小端序,直接读取;否则需要反转字节序。
     *
     * @param index 读取位置的索引
     * @return char 值
     */
    public char getCharLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getChar(index);
        } else {
            return Character.reverseBytes(getChar(index));
        }
    }

    /**
     * 读取指定位置的 char 值(大端字节序)。
     *
     * <p>如果当前平台是大端序,直接读取;否则需要反转字节序。
     *
     * @param index 读取位置的索引
     * @return char 值
     */
    public char getCharBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Character.reverseBytes(getChar(index));
        } else {
            return getChar(index);
        }
    }

    /**
     * 写入 char 值到指定位置(使用本地字节序)。
     *
     * @param index 写入位置的索引
     * @param value 要写入的 char 值
     */
    public void putChar(int index, char value) {
        UNSAFE.putChar(heapMemory, address + index, value);
    }

    /**
     * 写入 char 值到指定位置(小端字节序)。
     *
     * @param index 写入位置的索引
     * @param value 要写入的 char 值
     */
    public void putCharLittleEndian(int index, char value) {
        if (LITTLE_ENDIAN) {
            putChar(index, value);
        } else {
            putChar(index, Character.reverseBytes(value));
        }
    }

    /**
     * 写入 char 值到指定位置(大端字节序)。
     *
     * @param index 写入位置的索引
     * @param value 要写入的 char 值
     */
    public void putCharBigEndian(int index, char value) {
        if (LITTLE_ENDIAN) {
            putChar(index, Character.reverseBytes(value));
        } else {
            putChar(index, value);
        }
    }

    public short getShort(int index) {
        return UNSAFE.getShort(heapMemory, address + index);
    }

    public short getShortLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getShort(index);
        } else {
            return Short.reverseBytes(getShort(index));
        }
    }

    public short getShortBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Short.reverseBytes(getShort(index));
        } else {
            return getShort(index);
        }
    }

    public void putShort(int index, short value) {
        UNSAFE.putShort(heapMemory, address + index, value);
    }

    public void putShortLittleEndian(int index, short value) {
        if (LITTLE_ENDIAN) {
            putShort(index, value);
        } else {
            putShort(index, Short.reverseBytes(value));
        }
    }

    public void putShortBigEndian(int index, short value) {
        if (LITTLE_ENDIAN) {
            putShort(index, Short.reverseBytes(value));
        } else {
            putShort(index, value);
        }
    }

    public int getInt(int index) {
        return UNSAFE.getInt(heapMemory, address + index);
    }

    public int getIntLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getInt(index);
        } else {
            return Integer.reverseBytes(getInt(index));
        }
    }

    public int getIntBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Integer.reverseBytes(getInt(index));
        } else {
            return getInt(index);
        }
    }

    public void putInt(int index, int value) {
        UNSAFE.putInt(heapMemory, address + index, value);
    }

    public void putIntLittleEndian(int index, int value) {
        if (LITTLE_ENDIAN) {
            putInt(index, value);
        } else {
            putInt(index, Integer.reverseBytes(value));
        }
    }

    public void putIntBigEndian(int index, int value) {
        if (LITTLE_ENDIAN) {
            putInt(index, Integer.reverseBytes(value));
        } else {
            putInt(index, value);
        }
    }

    public long getLong(int index) {
        return UNSAFE.getLong(heapMemory, address + index);
    }

    public long getLongLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getLong(index);
        } else {
            return Long.reverseBytes(getLong(index));
        }
    }

    public long getLongBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Long.reverseBytes(getLong(index));
        } else {
            return getLong(index);
        }
    }

    public void putLong(int index, long value) {
        UNSAFE.putLong(heapMemory, address + index, value);
    }

    public void putLongLittleEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, value);
        } else {
            putLong(index, Long.reverseBytes(value));
        }
    }

    public void putLongBigEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, Long.reverseBytes(value));
        } else {
            putLong(index, value);
        }
    }

    public float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    public float getFloatLittleEndian(int index) {
        return Float.intBitsToFloat(getIntLittleEndian(index));
    }

    public float getFloatBigEndian(int index) {
        return Float.intBitsToFloat(getIntBigEndian(index));
    }

    public void putFloat(int index, float value) {
        putInt(index, Float.floatToRawIntBits(value));
    }

    public void putFloatLittleEndian(int index, float value) {
        putIntLittleEndian(index, Float.floatToRawIntBits(value));
    }

    public void putFloatBigEndian(int index, float value) {
        putIntBigEndian(index, Float.floatToRawIntBits(value));
    }

    public double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    public double getDoubleLittleEndian(int index) {
        return Double.longBitsToDouble(getLongLittleEndian(index));
    }

    public double getDoubleBigEndian(int index) {
        return Double.longBitsToDouble(getLongBigEndian(index));
    }

    public void putDouble(int index, double value) {
        putLong(index, Double.doubleToRawLongBits(value));
    }

    public void putDoubleLittleEndian(int index, double value) {
        putLongLittleEndian(index, Double.doubleToRawLongBits(value));
    }

    public void putDoubleBigEndian(int index, double value) {
        putLongBigEndian(index, Double.doubleToRawLongBits(value));
    }

    // -------------------------------------------------------------------------
    //                     批量读写方法
    // -------------------------------------------------------------------------

    /**
     * 将内存段的数据写入到 DataOutput。
     *
     * <p>对于堆内存,直接调用 out.write();对于堆外内存,逐 8 字节或逐字节写入。
     *
     * @param out 目标输出流
     * @param offset 起始偏移量
     * @param length 要写入的字节数
     * @throws IOException 如果发生 I/O 错误
     */
    public void get(DataOutput out, int offset, int length) throws IOException {
        if (heapMemory != null) {
            out.write(heapMemory, offset, length);
        } else {
            while (length >= 8) {
                out.writeLong(getLongBigEndian(offset));
                offset += 8;
                length -= 8;
            }

            while (length > 0) {
                out.writeByte(get(offset));
                offset++;
                length--;
            }
        }
    }

    /**
     * 从 DataInput 读取数据到内存段。
     *
     * <p>对于堆内存,直接调用 in.readFully();对于堆外内存,逐 8 字节或逐字节读取。
     *
     * @param in 输入流
     * @param offset 起始偏移量
     * @param length 要读取的字节数
     * @throws IOException 如果发生 I/O 错误
     */
    public void put(DataInput in, int offset, int length) throws IOException {
        if (heapMemory != null) {
            in.readFully(heapMemory, offset, length);
        } else {
            while (length >= 8) {
                putLongBigEndian(offset, in.readLong());
                offset += 8;
                length -= 8;
            }
            while (length > 0) {
                put(offset, in.readByte());
                offset++;
                length--;
            }
        }
    }

    /**
     * 将内存段的数据复制到目标 ByteBuffer。
     *
     * <p>支持 DirectByteBuffer 和基于数组的 ByteBuffer。
     *
     * @param offset 内存段起始偏移量
     * @param target 目标 ByteBuffer
     * @param numBytes 要复制的字节数
     * @throws IndexOutOfBoundsException 如果索引越界
     * @throws ReadOnlyBufferException 如果目标 ByteBuffer 是只读的
     * @throws BufferOverflowException 如果目标 ByteBuffer 空间不足
     */
    public void get(int offset, ByteBuffer target, int numBytes) {
        // check the byte array offset and length
        if ((offset | numBytes | (offset + numBytes)) < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (target.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }

        final int targetOffset = target.position();
        final int remaining = target.remaining();

        if (remaining < numBytes) {
            throw new BufferOverflowException();
        }

        if (target.isDirect()) {
            // copy to the target memory directly
            final long targetPointer = getByteBufferAddress(target) + targetOffset;
            final long sourcePointer = address + offset;

            UNSAFE.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
            target.position(targetOffset + numBytes);
        } else if (target.hasArray()) {
            // move directly into the byte array
            get(offset, target.array(), targetOffset + target.arrayOffset(), numBytes);

            // this must be after the get() call to ensue that the byte buffer is not
            // modified in case the call fails
            target.position(targetOffset + numBytes);
        } else {
            // other types of byte buffers
            throw new IllegalArgumentException(
                    "The target buffer is not direct, and has no array.");
        }
    }

    /**
     * 从源 ByteBuffer 复制数据到内存段。
     *
     * <p>支持 DirectByteBuffer 和基于数组的 ByteBuffer。
     *
     * @param offset 内存段起始偏移量
     * @param source 源 ByteBuffer
     * @param numBytes 要复制的字节数
     * @throws IndexOutOfBoundsException 如果索引越界
     * @throws BufferUnderflowException 如果源 ByteBuffer 数据不足
     */
    public void put(int offset, ByteBuffer source, int numBytes) {
        // check the byte array offset and length
        if ((offset | numBytes | (offset + numBytes)) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final int sourceOffset = source.position();
        final int remaining = source.remaining();

        if (remaining < numBytes) {
            throw new BufferUnderflowException();
        }

        if (source.isDirect()) {
            // copy to the target memory directly
            final long sourcePointer = getByteBufferAddress(source) + sourceOffset;
            final long targetPointer = address + offset;

            UNSAFE.copyMemory(null, sourcePointer, heapMemory, targetPointer, numBytes);
            source.position(sourceOffset + numBytes);
        } else if (source.hasArray()) {
            // move directly into the byte array
            put(offset, source.array(), sourceOffset + source.arrayOffset(), numBytes);

            // this must be after the get() call to ensue that the byte buffer is not
            // modified in case the call fails
            source.position(sourceOffset + numBytes);
        } else {
            // other types of byte buffers
            for (int i = 0; i < numBytes; i++) {
                put(offset++, source.get());
            }
        }
    }

    /**
     * 将当前内存段的数据复制到目标内存段。
     *
     * <p>使用 Unsafe.copyMemory 进行高效的内存复制,支持堆内和堆外内存之间的任意组合。
     *
     * @param offset 当前内存段起始偏移量
     * @param target 目标内存段
     * @param targetOffset 目标内存段起始偏移量
     * @param numBytes 要复制的字节数
     */
    public void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
        final byte[] thisHeapRef = this.heapMemory;
        final byte[] otherHeapRef = target.heapMemory;
        final long thisPointer = this.address + offset;
        final long otherPointer = target.address + targetOffset;

        UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
    }

    /**
     * 使用 Unsafe 复制数据到任意对象。
     *
     * <p>高级方法,用于优化的内存操作。需要调用者确保参数正确性。
     *
     * @param offset 起始偏移量
     * @param target 目标对象(可以是 byte[] 或其他对象)
     * @param targetPointer 目标对象中的偏移量
     * @param numBytes 要复制的字节数
     */
    public void copyToUnsafe(int offset, Object target, int targetPointer, int numBytes) {
        UNSAFE.copyMemory(this.heapMemory, this.address + offset, target, targetPointer, numBytes);
    }

    /**
     * 使用 Unsafe 从任意对象复制数据到当前内存段。
     *
     * <p>高级方法,用于优化的内存操作。需要调用者确保参数正确性。
     *
     * @param offset 起始偏移量
     * @param source 源对象(可以是 byte[] 或其他对象)
     * @param sourcePointer 源对象中的偏移量
     * @param numBytes 要复制的字节数
     */
    public void copyFromUnsafe(int offset, Object source, int sourcePointer, int numBytes) {
        UNSAFE.copyMemory(source, sourcePointer, this.heapMemory, this.address + offset, numBytes);
    }

    /**
     * 比较两个内存段的内容。
     *
     * <p>采用每次比较 8 字节的优化策略,提高比较性能。使用大端字节序进行比较。
     *
     * @param seg2 要比较的另一个内存段
     * @param offset1 当前内存段的起始偏移量
     * @param offset2 另一个内存段的起始偏移量
     * @param len 要比较的长度
     * @return 如果当前段小于 seg2 返回负数,相等返回 0,大于返回正数
     */
    public int compare(MemorySegment seg2, int offset1, int offset2, int len) {
        while (len >= 8) {
            long l1 = this.getLongBigEndian(offset1);
            long l2 = seg2.getLongBigEndian(offset2);

            if (l1 != l2) {
                return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
            }

            offset1 += 8;
            offset2 += 8;
            len -= 8;
        }
        while (len > 0) {
            int b1 = this.get(offset1) & 0xff;
            int b2 = seg2.get(offset2) & 0xff;
            int cmp = b1 - b2;
            if (cmp != 0) {
                return cmp;
            }
            offset1++;
            offset2++;
            len--;
        }
        return 0;
    }

    /**
     * 比较两个内存段的内容,支持不同长度。
     *
     * <p>首先比较公共长度部分,如果相等则比较长度差异。
     *
     * @param seg2 要比较的另一个内存段
     * @param offset1 当前内存段的起始偏移量
     * @param offset2 另一个内存段的起始偏移量
     * @param len1 当前内存段要比较的长度
     * @param len2 另一个内存段要比较的长度
     * @return 比较结果:负数、0 或正数
     */
    public int compare(MemorySegment seg2, int offset1, int offset2, int len1, int len2) {
        final int minLength = Math.min(len1, len2);
        int c = compare(seg2, offset1, offset2, minLength);
        return c == 0 ? (len1 - len2) : c;
    }

    /**
     * 交换两个内存段的字节内容。
     *
     * <p>使用临时缓冲区进行三次内存复制完成交换。
     *
     * @param tempBuffer 临时缓冲区,必须足够大以容纳要交换的数据
     * @param seg2 要交换的另一个内存段
     * @param offset1 当前内存段的起始偏移量
     * @param offset2 另一个内存段的起始偏移量
     * @param len 要交换的字节数
     * @throws IndexOutOfBoundsException 如果索引越界或临时缓冲区太小
     */
    public void swapBytes(
            byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
        if ((offset1 | offset2 | len | (tempBuffer.length - len)) >= 0) {
            final long thisPos = this.address + offset1;
            final long otherPos = seg2.address + offset2;

            // this -> temp buffer
            UNSAFE.copyMemory(this.heapMemory, thisPos, tempBuffer, BYTE_ARRAY_BASE_OFFSET, len);

            // other -> this
            UNSAFE.copyMemory(seg2.heapMemory, otherPos, this.heapMemory, thisPos, len);

            // temp buffer -> other
            UNSAFE.copyMemory(tempBuffer, BYTE_ARRAY_BASE_OFFSET, seg2.heapMemory, otherPos, len);
            return;
        }

        // index is in fact invalid
        throw new IndexOutOfBoundsException(
                String.format(
                        "offset1=%d, offset2=%d, len=%d, bufferSize=%d, address1=%d, address2=%d",
                        offset1, offset2, len, tempBuffer.length, this.address, seg2.address));
    }

    /**
     * 比较两个内存段区域是否相等。
     *
     * <p>采用每次比较 8 字节的优化策略,提高比较性能。
     *
     * @param seg2 要比较的另一个内存段
     * @param offset1 当前内存段的起始偏移量
     * @param offset2 另一个内存段的起始偏移量
     * @param length 要比较的内存区域长度
     * @return 如果相等返回 true,否则返回 false
     */
    public boolean equalTo(MemorySegment seg2, int offset1, int offset2, int length) {
        int i = 0;

        // we assume unaligned accesses are supported.
        // Compare 8 bytes at a time.
        while (i <= length - 8) {
            if (getLong(offset1 + i) != seg2.getLong(offset2 + i)) {
                return false;
            }
            i += 8;
        }

        // cover the last (length % 8) elements.
        while (i < length) {
            if (get(offset1 + i) != seg2.get(offset2 + i)) {
                return false;
            }
            i += 1;
        }

        return true;
    }

    /**
     * 获取堆内存数组对象。
     *
     * <p>该方法用于需要直接访问底层 byte 数组的场景。
     *
     * @return 如果是堆内存返回非 null 的 byte 数组,如果是堆外内存返回 null
     */
    public byte[] getHeapMemory() {
        return heapMemory;
    }
}
