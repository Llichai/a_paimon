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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Java 反射工具类，提供简化的反射操作方法。
 *
 * <p>ReflectionUtils 提供了一组静态方法，用于执行常见的反射操作，包括：
 * <ul>
 *   <li>调用静态方法
 *   <li>根据方法名和参数个数查找方法
 *   <li>访问私有字段
 *   <li>跨类层次结构查找字段
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 调用静态方法
 * Integer result = ReflectionUtils.invokeStaticMethod(
 *     Integer.class,
 *     "parseInt",
 *     "123"
 * );
 *
 * // 获取方法对象
 * Method method = ReflectionUtils.getMethod(String.class, "substring", 2);
 *
 * // 访问私有字段
 * Object obj = new MyClass();
 * String privateValue = ReflectionUtils.getPrivateFieldValue(obj, "privateField");
 * }</pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li>访问和调用第三方库的私有 API
 *   <li>单元测试中访问私有成员进行验证
 *   <li>框架开发中的动态方法调用
 *   <li>兼容性适配层的实现
 * </ul>
 *
 * <p>性能考虑：
 * <ul>
 *   <li>反射调用比直接调用慢 10-100 倍
 *   <li>Method 对象可以被缓存以提高性能
 *   <li>频繁调用的反射操作应该考虑缓存或代码生成
 * </ul>
 *
 * <p>安全性注意事项：
 * <ul>
 *   <li>反射可以绕过访问控制检查（通过 setAccessible）
 *   <li>在安全管理器启用的环境中可能被禁止
 *   <li>访问私有成员可能违反封装原则
 *   <li>应仅在必要时使用反射
 * </ul>
 *
 * <p>异常处理：
 * <ul>
 *   <li>{@link NoSuchMethodException}：找不到指定的方法
 *   <li>{@link NoSuchFieldException}：找不到指定的字段
 *   <li>{@link InvocationTargetException}：被调用的方法抛出异常
 *   <li>{@link IllegalAccessException}：访问权限不足
 * </ul>
 */
public class ReflectionUtils {

    /**
     * 调用类的静态方法。
     *
     * <p>此方法会在指定类中查找第一个名称匹配的方法，然后调用它。
     * 注意：如果有多个同名方法（重载），此方法会返回第一个找到的方法。
     *
     * <p>示例：
     * <pre>{@code
     * // 调用 Integer.parseInt(String)
     * Integer result = ReflectionUtils.invokeStaticMethod(
     *     Integer.class,
     *     "parseInt",
     *     "123"
     * );
     * }</pre>
     *
     * @param clz 包含静态方法的类
     * @param methodName 方法名称
     * @param args 方法参数
     * @param <T> 返回值类型
     * @return 方法的返回值
     * @throws NoSuchMethodException 如果找不到指定名称的方法
     * @throws InvocationTargetException 如果被调用的方法抛出异常
     * @throws IllegalAccessException 如果方法不可访问
     */
    @SuppressWarnings("rawtypes")
    public static <T> T invokeStaticMethod(Class clz, String methodName, Object... args)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = null;
        Method[] methods = clz.getDeclaredMethods();
        for (Method m : methods) {
            if (methodName.equals(m.getName())) {
                method = m;
                break;
            }
        }

        if (method == null) {
            throw new NoSuchMethodException(methodName);
        }
        return invokeStaticMethod(method, args);
    }

    /**
     * 使用指定的 Method 对象调用静态方法。
     *
     * <p>此方法假设传入的 Method 对象是静态方法。
     *
     * @param method 要调用的方法对象
     * @param args 方法参数
     * @param <T> 返回值类型
     * @return 方法的返回值
     * @throws InvocationTargetException 如果被调用的方法抛出异常
     * @throws IllegalAccessException 如果方法不可访问
     */
    @SuppressWarnings("unchecked")
    public static <T> T invokeStaticMethod(Method method, Object... args)
            throws InvocationTargetException, IllegalAccessException {
        return (T) method.invoke(null, args);
    }

    /**
     * 根据方法名和参数个数获取方法对象。
     *
     * <p>此方法在类的所有公共方法（包括继承的方法）中查找匹配的方法。
     * 如果有多个同名且参数个数相同的方法（不同参数类型），此方法会返回第一个找到的方法。
     *
     * <p>示例：
     * <pre>{@code
     * // 获取 String.substring(int, int)
     * Method method = ReflectionUtils.getMethod(String.class, "substring", 2);
     * }</pre>
     *
     * @param clz 要查找方法的类
     * @param methodName 方法名称
     * @param argSize 参数个数
     * @return 匹配的方法对象
     * @throws NoSuchMethodException 如果找不到匹配的方法
     */
    public static Method getMethod(Class<?> clz, String methodName, int argSize)
            throws NoSuchMethodException {
        Method method = null;
        Method[] methods = clz.getMethods();
        for (Method m : methods) {
            if (methodName.equals(m.getName()) && m.getParameterTypes().length == argSize) {
                method = m;
                break;
            }
        }

        if (method == null) {
            throw new NoSuchMethodException(methodName);
        }
        return method;
    }

    /**
     * 获取对象的私有字段值。
     *
     * <p>此方法会在对象的类层次结构中向上搜索，直到找到指定的字段或到达顶层类。
     * 它会自动设置 accessible 标志以访问私有字段。
     *
     * <p>示例：
     * <pre>{@code
     * class MyClass {
     *     private String secret = "password";
     * }
     *
     * MyClass obj = new MyClass();
     * String value = ReflectionUtils.getPrivateFieldValue(obj, "secret");
     * // value = "password"
     * }</pre>
     *
     * <p>注意事项：
     * <ul>
     *   <li>此方法会搜索整个类层次结构，包括父类的私有字段
     *   <li>字段访问权限会被临时修改（setAccessible）
     *   <li>在安全管理器启用的环境中可能失败
     * </ul>
     *
     * @param obj 要访问字段的对象
     * @param fieldName 字段名称
     * @param <T> 字段值类型
     * @return 字段的值
     * @throws NoSuchFieldException 如果在整个类层次结构中都找不到该字段
     * @throws IllegalAccessException 如果字段不可访问（即使设置了 accessible）
     */
    @SuppressWarnings("unchecked")
    public static <T> T getPrivateFieldValue(Object obj, String fieldName)
            throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(obj);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
