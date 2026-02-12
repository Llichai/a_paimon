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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 可空序列化器包装器 - 为不支持 {@code null} 值的序列化器添加 null 支持。
 *
 * <p>这是一个装饰器模式的实现,它包装原始序列化器并添加 null 值处理能力。
 * 许多基本类型的序列化器(如 IntSerializer)不支持序列化 null,
 * 这个包装器通过添加一个额外的布尔标记来解决这个问题。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>null 标记: 1 字节布尔值
 *     <ul>
 *       <li>true: 表示值为 null,后面没有更多数据
 *       <li>false: 表示值不为 null,后面跟随实际的序列化数据
 *     </ul>
 *   <li>实际数据: 仅在值不为 null 时存在,使用原始序列化器序列化
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>包装基本类型序列化器,使其支持 null
 *   <li>为不支持 null 的自定义序列化器添加 null 支持
 *   <li>统一 null 值处理逻辑
 * </ul>
 *
 * <p>性能影响:
 * <ul>
 *   <li>额外开销: 每个值增加 1 字节的 null 标记
 *   <li>分支预测: null 检查可能影响 CPU 分支预测
 *   <li>建议: 对于大多数值都为 null 或都不为 null 的场景,考虑使用专门的序列化器
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 为 IntSerializer 添加 null 支持
 * Serializer<Integer> originalSerializer = IntSerializer.INSTANCE;
 * Serializer<Integer> nullableSerializer =
 *     NullableSerializer.wrap(originalSerializer);
 *
 * // 序列化 null 值
 * nullableSerializer.serialize(null, output);
 *
 * // 序列化非 null 值
 * nullableSerializer.serialize(42, output);
 *
 * // 自动检测是否需要包装
 * Serializer<Integer> serializer =
 *     NullableSerializer.wrapIfNullIsNotSupported(originalSerializer);
 * }</pre>
 *
 * <p>设计特点:
 * <ul>
 *   <li>透明包装: 对外接口与普通序列化器一致
 *   <li>智能检测: 提供 {@link #checkIfNullSupported} 方法检测是否需要包装
 *   <li>嵌套检测: 避免重复包装已经是 NullableSerializer 的序列化器
 * </ul>
 *
 * @param <T> 要序列化的对象类型
 * @see Serializer
 */
public class NullableSerializer<T> implements Serializer<T> {

    private static final long serialVersionUID = 1L;

    /** 被包装的原始序列化器。 */
    private final Serializer<T> originalSerializer;

    /**
     * 私有构造函数,使用 {@link #wrap} 或 {@link #wrapIfNullIsNotSupported} 创建实例。
     *
     * @param originalSerializer 要包装的原始序列化器
     */
    private NullableSerializer(Serializer<T> originalSerializer) {
        this.originalSerializer = originalSerializer;
    }

    /**
     * 如果原始序列化器不支持 null,则包装它;否则返回原始序列化器。
     *
     * <p>这个方法会尝试序列化和反序列化 null 值来检测是否支持 null。
     * 如果检测失败(抛出异常),则自动包装序列化器。
     *
     * @param originalSerializer 要检测和可能包装的序列化器
     * @param <T> 序列化对象的类型
     * @return 支持 null 的序列化器(可能是原始的,也可能是包装的)
     */
    public static <T> Serializer<T> wrapIfNullIsNotSupported(
            @Nonnull Serializer<T> originalSerializer) {
        return checkIfNullSupported(originalSerializer)
                ? originalSerializer
                : wrap(originalSerializer);
    }

    /**
     * 检查序列化器是否支持 null 值。
     *
     * <p>检测方法:
     * <ol>
     *   <li>尝试序列化 null 值到临时输出
     *   <li>尝试从临时输入反序列化,验证结果为 null
     *   <li>验证 copy(null) 返回 null
     * </ol>
     *
     * <p>如果任何步骤失败(抛出 IOException 或 RuntimeException),
     * 则认为不支持 null。
     *
     * @param serializer 要检测的序列化器
     * @param <T> 序列化对象的类型
     * @return 如果支持 null 返回 true,否则返回 false
     */
    public static <T> boolean checkIfNullSupported(Serializer<T> serializer) {
        DataOutputSerializer dos = new DataOutputSerializer(10);
        try {
            serializer.serialize(null, dos);
        } catch (IOException | RuntimeException e) {
            return false;
        }
        DataInputDeserializer dis = new DataInputDeserializer(dos.getSharedBuffer());
        try {
            checkArgument(serializer.deserialize(dis) == null);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Unexpected failure to deserialize just serialized null value with %s",
                            serializer.getClass().getName()),
                    e);
        }
        checkArgument(
                serializer.copy(null) == null,
                "Serializer %s has to be able properly copy null value if it can serialize it",
                serializer.getClass().getName());
        return true;
    }

    /**
     * 获取原始序列化器。
     *
     * @return 被包装的原始序列化器
     */
    private Serializer<T> originalSerializer() {
        return originalSerializer;
    }

    /**
     * 包装序列化器以添加 null 支持。
     *
     * <p>如果序列化器已经是 NullableSerializer,直接返回;
     * 否则创建新的 NullableSerializer 包装它。
     *
     * @param originalSerializer 要包装的原始序列化器
     * @param <T> 序列化对象的类型
     * @return 支持 null 的包装序列化器
     */
    public static <T> Serializer<T> wrap(Serializer<T> originalSerializer) {
        return originalSerializer instanceof NullableSerializer
                ? originalSerializer
                : new NullableSerializer<>(originalSerializer);
    }

    /**
     * 复制序列化器实例。
     *
     * <p>如果原始序列化器的副本与原始序列化器相同(无状态),
     * 则返回当前实例;否则创建新的包装器。
     *
     * @return 序列化器的副本
     */
    @Override
    public Serializer<T> duplicate() {
        Serializer<T> duplicateOriginalSerializer = originalSerializer.duplicate();
        return duplicateOriginalSerializer == originalSerializer
                ? this
                : new NullableSerializer<>(originalSerializer.duplicate());
    }

    /**
     * 深拷贝对象。
     *
     * <p>如果对象为 null,返回 null;否则委托给原始序列化器。
     *
     * @param from 要拷贝的对象
     * @return 对象的深拷贝,或 null
     */
    @Override
    public T copy(T from) {
        return from == null ? null : originalSerializer.copy(from);
    }

    /**
     * 序列化对象到输出视图。
     *
     * <p>序列化格式:
     * <ol>
     *   <li>如果对象为 null: 写入 true(1字节)
     *   <li>如果对象不为 null: 写入 false(1字节),然后写入实际数据
     * </ol>
     *
     * @param record 要序列化的对象(可能为 null)
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeBoolean(true);
        } else {
            target.writeBoolean(false);
            originalSerializer.serialize(record, target);
        }
    }

    /**
     * 从输入视图反序列化对象。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>读取 null 标记(1字节布尔值)
     *   <li>如果为 true,返回 null
     *   <li>如果为 false,委托给原始序列化器反序列化
     * </ol>
     *
     * @param source 源输入视图
     * @return 反序列化的对象,或 null
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public T deserialize(DataInputView source) throws IOException {
        boolean isNull = deserializeNull(source);
        return isNull ? null : originalSerializer.deserialize(source);
    }

    /**
     * 读取 null 标记。
     *
     * @param source 源输入视图
     * @return 如果值为 null 返回 true,否则返回 false
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    private boolean deserializeNull(DataInputView source) throws IOException {
        return source.readBoolean();
    }

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>当且仅当原始序列化器相等时,两个 NullableSerializer 才相等。
     *
     * @param obj 要比较的对象
     * @return 如果相等返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && originalSerializer.equals(
                                ((NullableSerializer) obj).originalSerializer));
    }

    /**
     * 计算序列化器的哈希码。
     *
     * <p>基于原始序列化器的哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return originalSerializer.hashCode();
    }
}
