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

package org.apache.paimon.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 文档生成相关注解集合。
 *
 * <p>该类提供了一组注解,用于修改文档生成器的行为,控制配置选项在文档中的展示方式。
 * 这些注解主要用于配置文档的自动生成,支持覆盖默认值、排除选项、分组显示等功能。
 *
 * <h2>包含的注解</h2>
 * <ul>
 *   <li><b>OverrideDefault</b>: 覆盖文档中显示的默认值
 *   <li><b>Immutable</b>: 标记配置为不可变,排除在 schema 变更之外
 *   <li><b>ExcludeFromDocumentation</b>: 从文档中排除特定选项
 *   <li><b>Section</b>: 将选项包含在特定的文档章节中
 *   <li><b>SuffixOption</b>: 标记后缀配置选项
 * </ul>
 *
 * <h2>设计理念</h2>
 * <p>该类采用内部注解类的方式组织文档注解:
 * <ul>
 *   <li>将相关的文档注解集中在一个工具类中
 *   <li>通过注解元数据控制文档生成行为
 *   <li>支持灵活的文档定制和结构化展示
 *   <li>简化文档维护和生成流程
 * </ul>
 *
 * @since 1.0
 */
public final class Documentation {

    /**
     * 覆盖默认值注解。
     *
     * <p>用于配置选项字段上,覆盖文档中显示的默认值。当配置选项的实际默认值
     * 在文档中不够清晰或需要特殊说明时使用。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * @Documentation.OverrideDefault("无限制")
     * public static final ConfigOption<Integer> MAX_FILE_SIZE =
     *     ConfigOption.key("max.file.size")
     *         .intType()
     *         .defaultValue(-1);
     * }</pre>
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface OverrideDefault {
        /**
         * 文档中显示的默认值。
         *
         * @return 覆盖的默认值字符串
         */
        String value();
    }

    /**
     * 不可变配置注解。
     *
     * <p>用于 {@code ConfigOption} 字段上,标记该配置选项为不可变的,
     * 即在 schema 变更时不能修改。不可变配置通常是在表创建时设定的关键属性。
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>表的核心属性(如主键、分区键等)
     *   <li>创建后不能修改的配置
     *   <li>影响底层存储结构的配置
     * </ul>
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * @Documentation.Immutable
     * public static final ConfigOption<String> BUCKET_KEY =
     *     ConfigOption.key("bucket-key")
     *         .stringType()
     *         .noDefaultValue();
     * }</pre>
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Immutable {}

    /**
     * 从文档中排除注解。
     *
     * <p>用于配置选项字段或 REST API 消息头上,将其从生成的文档中排除。
     * 适用于内部配置、实验性功能或已弃用的配置。
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>内部使用的配置选项
     *   <li>实验性或不稳定的配置
     *   <li>已弃用但为了兼容性保留的配置
     *   <li>不希望在公开文档中暴露的配置
     * </ul>
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * @Documentation.ExcludeFromDocumentation("内部调试选项")
     * public static final ConfigOption<Boolean> DEBUG_MODE =
     *     ConfigOption.key("internal.debug.mode")
     *         .booleanType()
     *         .defaultValue(false);
     * }</pre>
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ExcludeFromDocumentation {
        /**
         * 排除原因说明(可选)。
         *
         * <p>提供排除该选项的原因,便于理解和维护。
         *
         * @return 排除原因,默认为空字符串
         */
        String value() default "";
    }

    /**
     * 文档章节注解。
     *
     * <p>用于配置选项字段上,将其包含在特定的文档章节中。章节是跨配置类
     * 聚合的选项组,每个组会被放置在独立的文档文件中。
     *
     * <h3>位置控制</h3>
     * <p>{@link Documentation.Section#position()} 参数控制选项在生成表格中的位置:
     * <ul>
     *   <li>数值越小,位置越靠前
     *   <li>相同位置的字段按配置键字母顺序排序
     *   <li>默认位置为 {@link Integer#MAX_VALUE}(最后)
     * </ul>
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * @Documentation.Section(value = {"性能调优"}, position = 10)
     * public static final ConfigOption<Integer> WRITE_BUFFER_SIZE =
     *     ConfigOption.key("write.buffer.size")
     *         .intType()
     *         .defaultValue(256);
     *
     * @Documentation.Section(value = {"性能调优", "内存管理"}, position = 20)
     * public static final ConfigOption<MemorySize> MEMORY_POOL =
     *     ConfigOption.key("memory.pool.size")
     *         .memoryType()
     *         .defaultValue(MemorySize.parse("256mb"));
     * }</pre>
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Section {

        /**
         * 配置文档中应包含此选项的章节。
         *
         * <p>一个配置选项可以被包含在多个章节中,以便在不同的文档结构中展示。
         *
         * @return 章节名称数组,默认为空数组
         */
        String[] value() default {};

        /**
         * 选项在其章节中的相对位置。
         *
         * <p>控制选项在文档表格中的显示顺序:
         * <ul>
         *   <li>较小的值出现在顶部
         *   <li>相同位置的选项按键名字母顺序排序
         *   <li>默认值为 {@link Integer#MAX_VALUE}
         * </ul>
         *
         * @return 相对位置值
         */
        int position() default Integer.MAX_VALUE;
    }

    /**
     * 后缀配置选项注解。
     *
     * <p>用于配置选项字段或选项类上,标记它们为后缀配置选项。
     * 后缀配置选项的键只是一个后缀,前缀在运行时动态提供。
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>动态配置键(前缀在运行时确定)
     *   <li>参数化配置(如特定列的配置)
     *   <li>模板配置(可复用的配置模式)
     * </ul>
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * // 字段级别的后缀选项
     * @Documentation.SuffixOption("<column-name>")
     * public static final ConfigOption<String> COLUMN_CODEC =
     *     ConfigOption.key("fields.<column-name>.codec")
     *         .stringType()
     *         .noDefaultValue();
     *
     * // 实际使用: fields.user_id.codec = "zstd"
     *
     * // 类级别的后缀选项
     * @Documentation.SuffixOption("<connector-type>")
     * public class ConnectorOptions {
     *     // 所有配置都是后缀配置
     * }
     * }</pre>
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface SuffixOption {
        /**
         * 后缀模式说明。
         *
         * <p>描述动态前缀的格式或占位符,便于用户理解如何使用该配置。
         *
         * @return 后缀模式字符串
         */
        String value();
    }

    /** 私有构造函数,防止实例化。 */
    private Documentation() {}
}
