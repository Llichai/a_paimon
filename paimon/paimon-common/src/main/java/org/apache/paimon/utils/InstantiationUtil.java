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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.HashMap;

/**
 * 实例化工具类。
 *
 * <p>提供对象序列化、反序列化和克隆的核心功能。该工具类是 Paimon 项目中处理对象持久化和传输的基础组件,
 * 支持自定义 ClassLoader 来解决不同运行时环境下的类加载问题。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>对象序列化</b> - 将 Java 对象转换为字节数组
 *   <li><b>对象反序列化</b> - 从字节数组恢复 Java 对象
 *   <li><b>对象深度克隆</b> - 通过序列化实现对象的完整复制
 *   <li><b>自定义 ClassLoader</b> - 支持指定类加载器解决类加载问题
 *   <li><b>原始类型支持</b> - 正确处理基本类型的序列化
 *   <li><b>代理类支持</b> - 支持动态代理类的序列化和反序列化
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>分布式计算</b> - 在不同节点间传输对象状态
 *   <li><b>状态持久化</b> - 将运行时对象持久化到存储系统
 *   <li><b>对象克隆</b> - 创建对象的深度副本
 *   <li><b>任务序列化</b> - 序列化用户定义的函数和任务
 *   <li><b>配置传输</b> - 在客户端和服务端间传输配置对象
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 序列化对象
 * MySerializableObject obj = new MySerializableObject();
 * byte[] bytes = InstantiationUtil.serializeObject(obj);
 *
 * // 2. 反序列化对象
 * ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
 * MySerializableObject restored = InstantiationUtil.deserializeObject(bytes, classLoader);
 *
 * // 3. 克隆对象
 * MySerializableObject clone = InstantiationUtil.clone(obj);
 *
 * // 4. 使用自定义 ClassLoader 克隆
 * ClassLoader customLoader = new URLClassLoader(...);
 * MySerializableObject cloneWithLoader = InstantiationUtil.clone(obj, customLoader);
 *
 * // 5. 从输入流反序列化
 * try (FileInputStream fis = new FileInputStream("object.ser")) {
 *     MySerializableObject fromFile = InstantiationUtil.deserializeObject(fis, classLoader);
 * }
 * }</pre>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>ClassLoader 切换</b> - 反序列化时临时切换线程上下文类加载器
 *   <li><b>原始类型映射</b> - 维护基本类型名称到 Class 对象的映射表
 *   <li><b>代理类处理</b> - 特殊处理动态代理类和非公开接口
 *   <li><b>线程安全</b> - 使用 ThreadLocal 机制保证多线程安全
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>被序列化的对象必须实现 {@link Serializable} 接口
 *   <li>对象的所有字段也必须是可序列化的
 *   <li>序列化开销较大,不适合高频率调用
 *   <li>克隆操作会创建完全独立的对象副本,包括所有引用的对象
 *   <li>使用正确的 ClassLoader 避免 ClassNotFoundException
 * </ul>
 *
 * @see java.io.Serializable
 * @see java.io.ObjectInputStream
 * @see java.io.ObjectOutputStream
 */
public final class InstantiationUtil {

    /**
     * 从字节数组反序列化对象。
     *
     * <p>使用指定的 ClassLoader 来加载对象所需的类。该方法适用于已经将对象序列化为字节数组的场景。
     *
     * @param bytes 包含序列化对象的字节数组
     * @param cl 用于加载类的 ClassLoader
     * @param <T> 反序列化后的对象类型
     * @return 反序列化后的对象
     * @throws IOException 如果读取或反序列化过程中发生 I/O 错误
     * @throws ClassNotFoundException 如果找不到对象引用的某个类
     */
    public static <T> T deserializeObject(byte[] bytes, ClassLoader cl)
            throws IOException, ClassNotFoundException {

        return deserializeObject(new ByteArrayInputStream(bytes), cl);
    }

    /**
     * 从输入流反序列化对象。
     *
     * <p>使用指定的 ClassLoader 来加载对象所需的类。该方法会临时切换线程上下文类加载器,
     * 以确保反序列化过程中使用正确的类加载器。
     *
     * <p><b>注意</b>: 该方法不会关闭输入流,调用者需要负责流的关闭。
     *
     * @param in 包含序列化对象的输入流
     * @param cl 用于加载类的 ClassLoader
     * @param <T> 反序列化后的对象类型
     * @return 反序列化后的对象
     * @throws IOException 如果读取或反序列化过程中发生 I/O 错误
     * @throws ClassNotFoundException 如果找不到对象引用的某个类
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(InputStream in, ClassLoader cl)
            throws IOException, ClassNotFoundException {

        final ClassLoader old = Thread.currentThread().getContextClassLoader();
        // not using resource try to avoid AutoClosable's close() on the given stream
        try {
            ObjectInputStream oois = new InstantiationUtil.ClassLoaderObjectInputStream(in, cl);
            Thread.currentThread().setContextClassLoader(cl);
            return (T) oois.readObject();
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
    }

    /**
     * 将对象序列化为字节数组。
     *
     * <p>使用 Java 标准序列化机制将对象转换为字节数组。该方法会自动管理流的关闭。
     *
     * @param o 要序列化的对象,必须实现 {@link Serializable} 接口
     * @return 包含序列化对象的字节数组
     * @throws IOException 如果序列化过程中发生 I/O 错误
     */
    public static byte[] serializeObject(Object o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        }
    }

    /**
     * 将对象序列化到输出流。
     *
     * <p>如果提供的输出流已经是 {@link ObjectOutputStream},则直接使用;
     * 否则创建一个新的 ObjectOutputStream 包装输出流。
     *
     * <p><b>注意</b>: 该方法不会关闭输出流,调用者需要负责流的关闭。
     *
     * @param out 目标输出流
     * @param o 要序列化的对象,必须实现 {@link Serializable} 接口
     * @throws IOException 如果序列化过程中发生 I/O 错误
     */
    public static void serializeObject(OutputStream out, Object o) throws IOException {
        ObjectOutputStream oos =
                out instanceof ObjectOutputStream
                        ? (ObjectOutputStream) out
                        : new ObjectOutputStream(out);
        oos.writeObject(o);
    }

    /**
     * 克隆给定的可序列化对象。
     *
     * <p>使用 Java 序列化机制创建对象的深度副本。该方法通过序列化和反序列化对象来实现克隆,
     * 因此会创建完全独立的对象副本,包括所有引用的对象。
     *
     * <p>使用对象自身的 ClassLoader 进行反序列化。
     *
     * @param obj 要克隆的对象
     * @param <T> 对象类型
     * @return 克隆后的对象,如果输入为 null 则返回 null
     * @throws IOException 如果序列化或反序列化过程失败
     * @throws ClassNotFoundException 如果反序列化时无法解析对象引用的某个类
     */
    public static <T extends Serializable> T clone(T obj)
            throws IOException, ClassNotFoundException {
        if (obj == null) {
            return null;
        } else {
            return clone(obj, obj.getClass().getClassLoader());
        }
    }

    /**
     * 使用指定的 ClassLoader 克隆对象。
     *
     * <p>使用 Java 序列化机制创建对象的深度副本,并使用指定的 ClassLoader 来解析克隆对象的类。
     * 该方法适用于需要在不同类加载器上下文中克隆对象的场景,如分布式计算环境。
     *
     * @param obj 要克隆的对象
     * @param classLoader 用于解析克隆对象类的 ClassLoader
     * @param <T> 对象类型
     * @return 克隆后的对象,如果输入为 null 则返回 null
     * @throws IOException 如果序列化或反序列化过程失败
     * @throws ClassNotFoundException 如果使用指定 ClassLoader 无法解析对象引用的某个类
     */
    public static <T extends Serializable> T clone(T obj, ClassLoader classLoader)
            throws IOException, ClassNotFoundException {
        if (obj == null) {
            return null;
        } else {
            final byte[] serializedObject = serializeObject(obj);
            return deserializeObject(serializedObject, classLoader);
        }
    }

    /**
     * 自定义 ObjectInputStream,支持使用指定的 ClassLoader 加载类。
     *
     * <p>该类扩展了标准的 {@link ObjectInputStream},允许使用自定义的 ClassLoader 来加载类。
     * 这在分布式环境中尤其有用,因为不同节点可能使用不同的类加载器。
     *
     * <h3>特性</h3>
     * <ul>
     *   <li>支持使用自定义 ClassLoader 加载类
     *   <li>支持加载基本类型(primitive types)
     *   <li>支持动态代理类的加载
     *   <li>正确处理非公开接口的代理类
     * </ul>
     */
    public static class ClassLoaderObjectInputStream extends ObjectInputStream {

        /** 用于加载类的 ClassLoader。 */
        protected final ClassLoader classLoader;

        /**
         * 创建 ClassLoaderObjectInputStream 实例。
         *
         * @param in 底层输入流
         * @param classLoader 用于加载类的 ClassLoader
         * @throws IOException 如果创建输入流时发生 I/O 错误
         */
        public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader)
                throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        /**
         * 解析类。
         *
         * <p>使用指定的 ClassLoader 加载类。如果指定的 ClassLoader 无法加载该类,
         * 会尝试从基本类型映射表中查找,以支持基本类型的反序列化。
         *
         * @param desc 类的描述符
         * @return 解析后的类对象
         * @throws IOException 如果发生 I/O 错误
         * @throws ClassNotFoundException 如果找不到类
         */
        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            if (classLoader != null) {
                String name = desc.getName();
                try {
                    return Class.forName(name, false, classLoader);
                } catch (ClassNotFoundException ex) {
                    // check if class is a primitive class
                    Class<?> cl = primitiveClasses.get(name);
                    if (cl != null) {
                        // return primitive class
                        return cl;
                    } else {
                        // throw ClassNotFoundException
                        throw ex;
                    }
                }
            }

            return super.resolveClass(desc);
        }

        /**
         * 解析代理类。
         *
         * <p>使用指定的 ClassLoader 加载动态代理类。该方法会特别处理非公开接口,
         * 确保代理类在正确的类加载器上下文中创建。
         *
         * <p>如果代理类实现了非公开接口,则使用该非公开接口的类加载器;
         * 否则使用指定的 classLoader。
         *
         * @param interfaces 代理类实现的接口名称数组
         * @return 解析后的代理类
         * @throws IOException 如果发生 I/O 错误
         * @throws ClassNotFoundException 如果找不到某个接口类
         */
        @Override
        protected Class<?> resolveProxyClass(String[] interfaces)
                throws IOException, ClassNotFoundException {
            if (classLoader != null) {
                ClassLoader nonPublicLoader = null;
                boolean hasNonPublicInterface = false;

                // define proxy in class loader of non-public interface(s), if any
                Class<?>[] classObjs = new Class<?>[interfaces.length];
                for (int i = 0; i < interfaces.length; i++) {
                    Class<?> cl = Class.forName(interfaces[i], false, classLoader);
                    if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
                        if (hasNonPublicInterface) {
                            if (nonPublicLoader != cl.getClassLoader()) {
                                throw new IllegalAccessError(
                                        "conflicting non-public interface class loaders");
                            }
                        } else {
                            nonPublicLoader = cl.getClassLoader();
                            hasNonPublicInterface = true;
                        }
                    }
                    classObjs[i] = cl;
                }
                try {
                    return Proxy.getProxyClass(
                            hasNonPublicInterface ? nonPublicLoader : classLoader, classObjs);
                } catch (IllegalArgumentException e) {
                    throw new ClassNotFoundException(null, e);
                }
            }

            return super.resolveProxyClass(interfaces);
        }

        // ------------------------------------------------

        /**
         * 基本类型名称到 Class 对象的映射表。
         *
         * <p>用于在反序列化时快速查找基本类型的 Class 对象,支持以下基本类型:
         * <ul>
         *   <li>boolean - 布尔类型
         *   <li>byte - 字节类型
         *   <li>char - 字符类型
         *   <li>short - 短整型
         *   <li>int - 整型
         *   <li>long - 长整型
         *   <li>float - 单精度浮点型
         *   <li>double - 双精度浮点型
         *   <li>void - 空类型
         * </ul>
         */
        private static final HashMap<String, Class<?>> primitiveClasses = new HashMap<>(9);

        static {
            primitiveClasses.put("boolean", boolean.class);
            primitiveClasses.put("byte", byte.class);
            primitiveClasses.put("char", char.class);
            primitiveClasses.put("short", short.class);
            primitiveClasses.put("int", int.class);
            primitiveClasses.put("long", long.class);
            primitiveClasses.put("float", float.class);
            primitiveClasses.put("double", double.class);
            primitiveClasses.put("void", void.class);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 私有构造方法,防止实例化。
     *
     * <p>该类是纯工具类,所有方法都是静态的,不应该被实例化。
     */
    private InstantiationUtil() {
        throw new RuntimeException();
    }
}
