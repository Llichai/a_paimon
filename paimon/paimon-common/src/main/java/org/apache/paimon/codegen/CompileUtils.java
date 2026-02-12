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

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 编译工具类。
 *
 * <p>用于将生成的 Java 代码编译成 Class 类。该类提供了代码编译和缓存功能,避免重复编译相同的代码。
 *
 * <p>主要特性:
 * <ul>
 *   <li>使用 Janino 进行快速编译</li>
 *   <li>提供编译结果缓存,避免重复编译</li>
 *   <li>错误时输出带行号的代码便于调试</li>
 * </ul>
 */
public final class CompileUtils {

    // 用于将生成的代码记录到同一位置
    private static final Logger CODE_LOG = LoggerFactory.getLogger(CompileUtils.class);

    /**
     * 编译缓存。
     *
     * <p>Janino 每次编译都会生成新的 ClassLoader 和 Class 文件（确保类名不会重复）。
     * 这导致同一进程的多个任务生成大量重复的类,从而导致大量的元空间 GC（类卸载）,造成性能瓶颈。
     * 因此我们添加缓存来避免这个问题。
     *
     * <p>缓存特性:
     * <ul>
     *   <li>使用软引用,允许在内存不足时回收</li>
     *   <li>5分钟访问过期时间</li>
     *   <li>最大缓存 300 个类</li>
     * </ul>
     */
    static final Cache<ClassKey, Class<?>> COMPILED_CLASS_CACHE =
            Caffeine.newBuilder()
                    .softValues()
                    // estimated maximum planning/startup time
                    .expireAfterAccess(Duration.ofMinutes(5))
                    // estimated cache size
                    .maximumSize(300)
                    .executor(Runnable::run)
                    .build();

    /**
     * 将生成的代码编译成 Class。
     *
     * <p>该方法使用缓存机制,如果相同的代码已经编译过,则直接返回缓存的结果。
     *
     * @param cl 用于加载类的 ClassLoader
     * @param name 类名
     * @param code 生成的代码
     * @param <T> 类的类型
     * @return 编译后的 Class
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> compile(ClassLoader cl, String name, String code) {
        try {
            // 类名是 "code" 的一部分并使字符串唯一,
            // 为了防止类泄漏,我们不直接缓存类加载器,
            // 而是只缓存其哈希码
            final ClassKey classKey = new ClassKey(cl.hashCode(), code);
            return (Class<T>) COMPILED_CLASS_CACHE.get(classKey, key -> doCompile(cl, name, code));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 执行实际的编译操作。
     *
     * @param cl 类加载器
     * @param name 类名
     * @param code 源代码
     * @param <T> 类的类型
     * @return 编译后的 Class
     */
    private static <T> Class<T> doCompile(ClassLoader cl, String name, String code) {
        checkNotNull(cl, "Classloader must not be null.");
        CODE_LOG.debug("Compiling: {} \n\n Code:\n{}", name, code);
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(cl);
        try {
            compiler.cook(code);
        } catch (Throwable t) {
            System.out.println(addLineNumber(code));
            throw new RuntimeException(
                    "Table program cannot be compiled. This is a bug. Please file an issue.", t);
        }
        try {
            //noinspection unchecked
            return (Class<T>) compiler.getClassLoader().loadClass(name);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Can not load class " + name, e);
        }
    }

    /**
     * 为代码添加行号。
     *
     * <p>在编译失败时输出更多信息。通常当 cook 失败时,会显示哪一行出错。这个行号从 1 开始。
     *
     * @param code 源代码
     * @return 带行号的代码
     */
    private static String addLineNumber(String code) {
        String[] lines = code.split("\n");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            builder.append("/* ").append(i + 1).append(" */").append(lines[i]).append("\n");
        }
        return builder.toString();
    }

    /**
     * 用作 {@link #COMPILED_CLASS_CACHE} 的键的类。
     *
     * <p>组合类加载器 ID 和代码内容作为缓存键,确保相同的代码在相同的类加载器环境中只编译一次。
     */
    private static class ClassKey {
        /** 类加载器的哈希码 */
        private final int classLoaderId;
        /** 源代码内容 */
        private final String code;

        private ClassKey(int classLoaderId, String code) {
            this.classLoaderId = classLoaderId;
            this.code = code;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassKey classKey = (ClassKey) o;
            return classLoaderId == classKey.classLoaderId && code.equals(classKey.code);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classLoaderId, code);
        }
    }
}
