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

package org.apache.paimon.codegen;

import org.apache.paimon.codegen.codesplit.JavaCodeSplitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 生成类的包装器。
 *
 * <p>该类封装了代码生成的结果,包括类名、生成的代码和引用对象。它提供了一个 {@link #newInstance(ClassLoader)}
 * 方法,可以通过引用对象轻松获取实例。
 *
 * <p>主要功能:
 * <ul>
 *   <li>封装生成的代码和类名</li>
 *   <li>支持代码分割以避免 Java 方法大小限制</li>
 *   <li>缓存编译结果以提高性能</li>
 *   <li>支持序列化和反序列化</li>
 * </ul>
 *
 * @param <T> 生成类的类型
 */
public final class GeneratedClass<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GeneratedClass.class);

    /** 生成类的完全限定名 */
    private final String className;

    /** 原始生成的代码 */
    private final String code;

    /** 分割后的代码（用于避免方法大小限制） */
    private final String splitCode;

    /** 传递给生成类的引用对象数组 */
    private final Object[] references;

    /** 缓存的编译后的类（不序列化） */
    private transient Class<T> compiledClass;

    /**
     * 创建一个生成类包装器（无引用对象）。
     *
     * @param className 类名
     * @param code 生成的代码
     */
    public GeneratedClass(String className, String code) {
        this(className, code, new Object[0]);
    }

    /**
     * 创建一个生成类包装器。
     *
     * @param className 类名
     * @param code 生成的代码
     * @param references 传递给生成类的引用对象数组
     */
    public GeneratedClass(String className, String code, Object[] references) {
        checkNotNull(className, "name must not be null");
        checkNotNull(code, "code must not be null");
        checkNotNull(references, "references must not be null");
        this.className = className;
        this.code = code;
        this.splitCode = code.isEmpty() ? code : JavaCodeSplitter.split(code, 4000, 10000);
        this.references = references;
    }

    /**
     * 创建此生成类的新实例。
     *
     * @param classLoader 用于编译和加载类的类加载器
     * @return 新创建的实例
     */
    public T newInstance(ClassLoader classLoader) {
        try {
            return compile(classLoader)
                    .getConstructor(Object[].class)
                    // 因为 Constructor.newInstance(Object... initargs),
                    // 我们需要将 references 加载到一个新的 Object[] 中,否则无法编译
                    .newInstance(new Object[] {references});
        } catch (Throwable e) {
            throw new RuntimeException(
                    "Could not instantiate generated class '" + className + "'", e);
        }
    }

    /**
     * 创建指定类的新实例。
     *
     * <p>这是一个静态工具方法,用于从已编译的类创建实例。
     *
     * @param clazz 要实例化的类
     * @param references 传递给构造函数的引用对象
     * @param <T> 类的类型
     * @return 新创建的实例
     */
    public static <T> T newInstance(Class<T> clazz, Object[] references) {
        try {
            return clazz.getConstructor(Object[].class)
                    // 因为 Constructor.newInstance(Object... initargs),
                    // 我们需要将 references 加载到一个新的 Object[] 中,否则无法编译
                    .newInstance(new Object[] {references});
        } catch (Throwable e) {
            throw new RuntimeException("Could not instantiate generated class '" + clazz + "'", e);
        }
    }

    /**
     * 编译生成的代码。
     *
     * <p>编译后的类会被缓存在 {@link GeneratedClass} 中,避免重复编译。
     *
     * <p>编译过程:
     * <ol>
     *   <li>首先尝试编译分割后的代码</li>
     *   <li>如果分割代码编译失败,则回退到原始代码</li>
     * </ol>
     *
     * @param classLoader 用于编译的类加载器
     * @return 编译后的 Class
     */
    public Class<T> compile(ClassLoader classLoader) {
        if (compiledClass == null) {
            // 缓存编译后的类
            try {
                // 首先尝试编译分割后的代码
                compiledClass = CompileUtils.compile(classLoader, className, splitCode);
            } catch (Throwable t) {
                // 如果失败,回退到原始代码
                LOG.warn("Failed to compile split code, falling back to original code", t);
                compiledClass = CompileUtils.compile(classLoader, className, code);
            }
        }
        return compiledClass;
    }

    /**
     * 获取类名。
     *
     * @return 类的完全限定名
     */
    public String getClassName() {
        return className;
    }

    /**
     * 获取原始生成的代码。
     *
     * @return 生成的 Java 源代码
     */
    public String getCode() {
        return code;
    }

    /**
     * 获取引用对象数组。
     *
     * @return 传递给生成类的引用对象
     */
    public Object[] getReferences() {
        return references;
    }

    /**
     * 获取编译后的 Class。
     *
     * <p>这是 {@link #compile(ClassLoader)} 的别名方法。
     *
     * @param classLoader 用于编译的类加载器
     * @return 编译后的 Class
     */
    public Class<T> getClass(ClassLoader classLoader) {
        return compile(classLoader);
    }
}
