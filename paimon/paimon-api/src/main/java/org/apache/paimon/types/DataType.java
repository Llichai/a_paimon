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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * 描述 Paimon 生态系统中的数据类型。
 *
 * <p>DataType 是 Paimon 类型系统的核心抽象类,用于表示表中字段的数据类型。
 * 每个数据类型都包含以下核心信息:
 * <ul>
 *     <li>类型根(typeRoot): 类型的基本分类,如 INTEGER、VARCHAR、ROW 等</li>
 *     <li>可空性(isNullable): 该类型的值是否允许为 null</li>
 *     <li>类型参数: 特定类型可能包含的额外参数,如 DECIMAL(10,2) 的精度和小数位数</li>
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *     <li>不可变性: 所有类型实例都是不可变的,修改操作返回新实例</li>
 *     <li>可序列化: 支持序列化,可在分布式环境中传输</li>
 *     <li>访问者模式: 通过 {@link DataTypeVisitor} 支持类型遍历和处理</li>
 *     <li>SQL 兼容: 可转换为 SQL 标准的类型字符串表示</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建一个可空的整数类型
 * DataType intType = DataTypes.INT();
 *
 * // 创建一个不可空的字符串类型
 * DataType stringType = DataTypes.STRING().notNull();
 *
 * // 创建复杂类型(数组)
 * DataType arrayType = DataTypes.ARRAY(DataTypes.INT());
 * }</pre>
 *
 * @see DataTypes 用于创建各种数据类型的工厂类
 * @see DataTypeRoot 数据类型的根枚举
 * @see DataTypeVisitor 数据类型访问者接口
 * @since 0.4.0
 */
@Public
public abstract class DataType implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 标识该类型的值是否可以为 null */
    private final boolean isNullable;

    /** 该类型的根分类,是对类型的基本描述 */
    private final DataTypeRoot typeRoot;

    /**
     * 构造一个数据类型实例。
     *
     * @param isNullable 该类型的值是否允许为 null
     * @param typeRoot 该类型的根分类
     */
    public DataType(boolean isNullable, DataTypeRoot typeRoot) {
        this.isNullable = isNullable;
        this.typeRoot = Preconditions.checkNotNull(typeRoot);
    }

    /**
     * 返回该类型的值是否可以为 {@code null}。
     *
     * <p>可空性是类型系统的重要属性,影响:
     * <ul>
     *     <li>数据存储: 是否需要额外的 null 标记位</li>
     *     <li>查询优化: 优化器可以基于可空性进行优化</li>
     *     <li>数据验证: 写入时的 null 值检查</li>
     * </ul>
     *
     * @return 如果该类型允许 null 值则返回 true,否则返回 false
     */
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * 返回该类型的根分类。
     *
     * <p>类型根是对数据类型的基本描述,不包含额外参数。例如,参数化的数据类型
     * {@code DECIMAL(12,3)} 拥有其根类型 {@code DECIMAL} 的所有特征。
     * 类型根能够在类型评估期间进行高效的类型比较。
     *
     * @return 该类型的根分类枚举值
     * @see DataTypeRoot
     */
    public DataTypeRoot getTypeRoot() {
        return typeRoot;
    }

    /**
     * 判断该类型的根是否等于指定的 {@code typeRoot}。
     *
     * <p>这是类型判断的最常用方法,提供高效的类型检查。
     *
     * @param typeRoot 要检查的目标类型根
     * @return 如果类型根相等则返回 true,否则返回 false
     */
    public boolean is(DataTypeRoot typeRoot) {
        return this.typeRoot == typeRoot;
    }

    /**
     * 判断该类型的根是否等于给定的任意一个 {@code typeRoots}。
     *
     * <p>用于同时检查多个可能的类型,常用于类型转换和兼容性判断。
     *
     * <p>示例:
     * <pre>{@code
     * if (dataType.isAnyOf(DataTypeRoot.TINYINT, DataTypeRoot.SMALLINT, DataTypeRoot.INTEGER)) {
     *     // 处理各种整数类型
     * }
     * }</pre>
     *
     * @param typeRoots 要检查的目标类型根数组
     * @return 如果类型根匹配任意一个则返回 true,否则返回 false
     */
    public boolean isAnyOf(DataTypeRoot... typeRoots) {
        return Arrays.stream(typeRoots).anyMatch(tr -> this.typeRoot == tr);
    }

    /**
     * 判断该类型的根是否属于给定的任意一个类型族 {@code typeFamilies}。
     *
     * <p>类型族是类型的更高层次分组,例如所有整数类型(TINYINT、SMALLINT、INTEGER、BIGINT)
     * 都属于 INTEGER_NUMERIC 族。这种检查方式更灵活,适合泛型的类型处理。
     *
     * <p>示例:
     * <pre>{@code
     * if (dataType.isAnyOf(DataTypeFamily.NUMERIC)) {
     *     // 处理所有数值类型
     * }
     * }</pre>
     *
     * @param typeFamilies 要检查的目标类型族数组
     * @return 如果类型根属于任意一个类型族则返回 true,否则返回 false
     * @see DataTypeFamily
     */
    public boolean isAnyOf(DataTypeFamily... typeFamilies) {
        return Arrays.stream(typeFamilies).anyMatch(tf -> this.typeRoot.getFamilies().contains(tf));
    }

    /**
     * 判断该类型是否属于指定的类型族 {@code family}。
     *
     * <p>单个类型族的检查方法,比 {@link #isAnyOf(DataTypeFamily...)} 更简洁。
     *
     * @param family 要检查的目标类型族
     * @return 如果类型属于该族则返回 true,否则返回 false
     */
    public boolean is(DataTypeFamily family) {
        return typeRoot.getFamilies().contains(family);
    }

    /**
     * 返回该数据类型值的默认大小(字节数)。
     *
     * <p>该值在内部用于大小估算,主要应用场景:
     * <ul>
     *     <li>内存缓冲区大小计算</li>
     *     <li>批量读写的批次大小估算</li>
     *     <li>查询优化器的成本估算</li>
     * </ul>
     *
     * <p>注意:这是一个估算值,实际大小可能因具体值而异(如变长类型)。
     *
     * @return 该类型值的默认大小(字节)
     */
    public abstract int defaultSize();

    /**
     * 返回该类型的深拷贝,可以指定不同的可空性。
     *
     * <p>由于类型对象是不可变的,修改类型属性(如可空性)需要创建新实例。
     * 深拷贝确保嵌套类型(如 ARRAY、MAP、ROW)中的元素类型也被正确复制。
     *
     * <p>实现说明:子类必须实现此方法,并确保:
     * <ul>
     *     <li>返回新的类型实例</li>
     *     <li>递归复制所有嵌套类型</li>
     *     <li>保留所有类型参数(如精度、长度等)</li>
     * </ul>
     *
     * @param isNullable 复制后类型的目标可空性
     * @return 具有指定可空性的新类型实例
     */
    public abstract DataType copy(boolean isNullable);

    /**
     * 返回该类型的深拷贝,保持原有的可空性。
     *
     * <p>这是 {@link #copy(boolean)} 的便捷方法,等价于 {@code copy(isNullable)}。
     *
     * @return 保持原有可空性的深拷贝
     */
    public final DataType copy() {
        return copy(isNullable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataType that = (DataType) o;
        return isNullable == that.isNullable && typeRoot == that.typeRoot;
    }

    /**
     * 比较两个数据类型是否相等,忽略可空性属性。
     *
     * <p>在某些场景下(如 Schema 演化、类型兼容性检查),我们需要判断两个类型
     * 在忽略可空性的情况下是否相同。此方法通过将两个类型都设为可空后再比较来实现。
     *
     * <p>示例:
     * <pre>{@code
     * DataType t1 = DataTypes.INT(); // 可空
     * DataType t2 = DataTypes.INT().notNull(); // 不可空
     * t1.equalsIgnoreNullable(t2); // 返回 true
     * }</pre>
     *
     * @param o 要比较的目标数据类型
     * @return 如果忽略可空性后两个类型相等则返回 true,否则返回 false
     */
    public boolean equalsIgnoreNullable(DataType o) {
        return Objects.equals(this.copy(true), o.copy(true));
    }

    /**
     * 比较两个数据类型是否相等,忽略字段 ID。
     *
     * <p>对于基本类型,此方法等同于 {@link #equals(Object)}。
     * 对于复杂类型(如 ROW),子类会重写此方法以忽略嵌套字段的 ID 进行比较。
     *
     * <p>字段 ID 是 Paimon 内部使用的标识符,在 Schema 演化时可能发生变化。
     * 此方法用于判断两个 Schema 在结构上是否相同,而不关心字段 ID 的具体值。
     *
     * @param o 要比较的目标数据类型
     * @return 如果忽略字段 ID 后两个类型相等则返回 true,否则返回 false
     */
    public boolean equalsIgnoreFieldId(DataType o) {
        return equals(o);
    }

    /**
     * 判断当前类型是否是目标类型经过裁剪(pruning)后的结果,或者两者完全相同。
     *
     * <p>裁剪是指从复杂类型中选择部分字段的操作,常见于列裁剪优化(Column Pruning)。
     * 例如,从一个包含 10 个字段的 ROW 类型中只选择 3 个字段,结果类型就是原类型的裁剪版本。
     *
     * <p>对于基本类型,此方法等同于 {@link #equals(Object)}。
     * 对于复杂类型(如 ROW、ARRAY、MAP),子类会重写此方法以支持结构性的裁剪判断。
     *
     * <p>使用场景:
     * <ul>
     *     <li>查询优化: 验证投影下推(Projection Pushdown)的正确性</li>
     *     <li>Schema 验证: 确保读取的数据Schema与期望的Schema兼容</li>
     *     <li>列式存储: 验证读取的列子集是否合法</li>
     * </ul>
     *
     * @param o 目标数据类型(可能是被裁剪前的类型)
     * @return 如果当前类型是目标类型的裁剪版本或两者相同则返回 true,否则返回 false
     */
    public boolean isPrunedFrom(Object o) {
        return equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isNullable, typeRoot);
    }

    /**
     * 返回该类型的 SQL 标准字符串表示形式。
     *
     * <p>返回的字符串遵循 SQL 标准,可用于:
     * <ul>
     *     <li>在控制台中打印类型信息</li>
     *     <li>生成 DDL 语句</li>
     *     <li>日志和错误消息</li>
     *     <li>与外部系统交互(如 SQL 客户端)</li>
     * </ul>
     *
     * <p>实现说明:为了提高可读性,实现可能会缩短过长的名称或省略某些特定属性。
     * 可空性会以 "NOT NULL" 后缀的形式体现。
     *
     * <p>示例输出:
     * <pre>
     * INT NOT NULL
     * VARCHAR(100)
     * ARRAY&lt;INT&gt;
     * ROW&lt;id INT, name VARCHAR(50)&gt;
     * DECIMAL(10, 2)
     * </pre>
     *
     * @return SQL 标准格式的类型字符串
     */
    public abstract String asSQLString();

    /**
     * 将该类型序列化为 JSON 格式。
     *
     * <p>默认实现将类型转换为 SQL 字符串并写入 JSON。子类可以重写此方法以提供
     * 更结构化的 JSON 表示(例如 ROW 类型会将字段信息展开为 JSON 对象)。
     *
     * @param generator JSON 生成器
     * @throws IOException 如果序列化过程中发生 I/O 错误
     */
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeString(asSQLString());
    }

    /**
     * 根据可空性为格式化字符串添加 "NOT NULL" 后缀。
     *
     * <p>这是一个辅助方法,用于子类实现 {@link #asSQLString()} 时格式化输出。
     * 如果类型不可空,会在格式化结果后追加 " NOT NULL"。
     *
     * @param format 格式化字符串模板
     * @param params 格式化参数
     * @return 格式化后的字符串,不可空类型会包含 " NOT NULL" 后缀
     */
    protected String withNullability(String format, Object... params) {
        if (!isNullable) {
            return String.format(format + " NOT NULL", params);
        }
        return String.format(format, params);
    }

    @Override
    public String toString() {
        return asSQLString();
    }

    /**
     * 接受一个数据类型访问者的访问。
     *
     * <p>这是访问者模式(Visitor Pattern)的标准实现。访问者模式允许在不修改类型类的情况下,
     * 为类型系统添加新的操作。常见用途:
     * <ul>
     *     <li>类型转换: 如 Paimon 类型转换为 Arrow、Parquet 等格式</li>
     *     <li>类型遍历: 递归遍历复杂类型的所有嵌套类型</li>
     *     <li>类型分析: 提取类型信息,如精度、长度、字段 ID 等</li>
     *     <li>代码生成: 根据类型生成序列化/反序列化代码</li>
     * </ul>
     *
     * <p>示例:
     * <pre>{@code
     * DataTypeVisitor<String> visitor = new DataTypeVisitor<String>() {
     *     public String visit(IntType intType) { return "int"; }
     *     public String visit(VarCharType varCharType) { return "string"; }
     *     // ... 其他类型
     * };
     * String typeName = dataType.accept(visitor);
     * }</pre>
     *
     * @param visitor 数据类型访问者
     * @param <R> 访问者返回的结果类型
     * @return 访问者处理该类型后的返回值
     * @see DataTypeVisitor
     */
    public abstract <R> R accept(DataTypeVisitor<R> visitor);

    /**
     * 收集该类型及其嵌套类型中的所有字段 ID。
     *
     * <p>字段 ID 是 Paimon 为每个字段分配的唯一标识符,用于:
     * <ul>
     *     <li>Schema 演化: 跟踪字段在不同 Schema 版本中的对应关系</li>
     *     <li>列裁剪: 通过字段 ID 精确定位需要读取的列</li>
     *     <li>数据重组: 在字段顺序变化时正确映射数据</li>
     * </ul>
     *
     * <p>默认实现为空(不收集任何字段 ID),只有复杂类型(如 ROW)需要重写此方法。
     *
     * @param fieldIds 用于收集字段 ID 的集合,方法会将发现的字段 ID 添加到此集合中
     */
    public void collectFieldIds(Set<Integer> fieldIds) {}

    /**
     * 返回该类型的不可空版本。
     *
     * <p>如果该类型已经是不可空的,返回的仍然是一个新实例(深拷贝)。
     * 这是 {@code copy(false)} 的便捷方法。
     *
     * <p>使用场景:
     * <ul>
     *     <li>主键字段: 主键字段必须是 NOT NULL</li>
     *     <li>分区字段: 某些表引擎要求分区字段不可空</li>
     *     <li>聚合键: 聚合表的聚合键通常不可空</li>
     * </ul>
     *
     * @return 不可空的类型副本
     */
    public DataType notNull() {
        return copy(false);
    }

    /**
     * 返回该类型的可空版本。
     *
     * <p>如果该类型已经是可空的,返回的仍然是一个新实例(深拷贝)。
     * 这是 {@code copy(true)} 的便捷方法。
     *
     * <p>使用场景:
     * <ul>
     *     <li>Schema 演化: 将不可空字段改为可空</li>
     *     <li>外连接: JOIN 结果中的字段可能为 null</li>
     *     <li>可选字段: 业务上允许缺失的字段</li>
     * </ul>
     *
     * @return 可空的类型副本
     */
    public DataType nullable() {
        return copy(true);
    }
}
