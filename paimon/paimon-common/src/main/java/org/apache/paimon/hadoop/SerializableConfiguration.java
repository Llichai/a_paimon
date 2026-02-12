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

package org.apache.paimon.hadoop;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * 可序列化的 Hadoop 配置包装器。
 *
 * <p>该类将 Hadoop 的 {@link Configuration} 包装为 {@link Serializable} 类型,
 * 使其能够在分布式计算框架中进行序列化传输。
 *
 * <p>主要应用场景:
 * <ul>
 *   <li>Spark/Flink 作业 - 在序列化算子中传递 Hadoop 配置
 *   <li>远程执行 - 将配置传输到远程节点
 *   <li>持久化 - 将配置保存到检查点或状态
 * </ul>
 */
public class SerializableConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Hadoop 配置对象,使用 transient 避免默认序列化 */
    private transient Configuration hadoopConf;

    /**
     * 构造可序列化的 Hadoop 配置包装器。
     *
     * @param hadoopConf Hadoop 配置对象
     */
    public SerializableConfiguration(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    /**
     * 自定义序列化方法。
     *
     * @param out 对象输出流
     * @throws IOException 如果序列化失败
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        hadoopConf.write(out);
    }

    /**
     * 自定义反序列化方法。
     *
     * @param in 对象输入流
     * @throws ClassNotFoundException 如果类未找到
     * @throws IOException 如果反序列化失败
     */
    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        in.defaultReadObject();
        hadoopConf = new Configuration(false);
        hadoopConf.readFields(in);
    }

    /**
     * 获取 Hadoop 配置对象。
     *
     * @return Hadoop 配置
     */
    public Configuration get() {
        return hadoopConf;
    }
}
