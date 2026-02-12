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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 本地文件工具类。
 *
 * <p>提供本地文件系统的特殊操作,特别是符号链接的处理。该工具类主要用于解析包含符号链接的路径,
 * 将符号链接替换为其指向的真实路径。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>符号链接解析</b> - 检测并解析路径中的符号链接
 *   <li><b>真实路径获取</b> - 获取符号链接指向的实际路径
 *   <li><b>路径规范化</b> - 将包含符号链接的路径转换为真实路径
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>文件定位</b> - 获取文件的真实物理位置
 *   <li><b>权限检查</b> - 检查符号链接目标的实际权限
 *   <li><b>路径比较</b> - 比较两个路径是否指向同一文件
 *   <li><b>日志记录</b> - 记录文件的真实位置而非符号链接
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 解析包含符号链接的路径
 * Path path = Paths.get("/data/logs/app.log");
 * // 假设 /data 是一个符号链接,指向 /mnt/storage/data
 * Path realPath = LocalFileUtils.getTargetPathIfContainsSymbolicPath(path);
 * // 返回: /mnt/storage/data/logs/app.log
 *
 * // 2. 路径不包含符号链接
 * Path normalPath = Paths.get("/home/user/file.txt");
 * Path result = LocalFileUtils.getTargetPathIfContainsSymbolicPath(normalPath);
 * // 返回: /home/user/file.txt (不变)
 *
 * // 3. 多级路径中包含符号链接
 * Path complexPath = Paths.get("/link1/link2/real/file.txt");
 * // 假设 /link1 是符号链接
 * Path resolved = LocalFileUtils.getTargetPathIfContainsSymbolicPath(complexPath);
 * // 符号链接会被解析,但后续路径会保留
 * }</pre>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>自底向上遍历</b> - 从路径的最深层开始,逐级向上检查
 *   <li><b>首次命中</b> - 找到第一个符号链接后即停止
 *   <li><b>路径重建</b> - 将符号链接目标与剩余路径组合
 *   <li><b>保留后缀</b> - 符号链接之后的路径部分会被保留
 * </ul>
 *
 * <h2>工作原理</h2>
 * <p>方法从给定路径开始,逐级向父目录遍历:
 * <ol>
 *   <li>检查当前路径是否为符号链接
 *   <li>如果是符号链接,获取其真实路径
 *   <li>将真实路径与之前收集的后缀路径组合
 *   <li>如果不是符号链接,继续向父目录遍历
 *   <li>如果到达根目录都没有找到符号链接,返回原始路径
 * </ol>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>仅解析首个符号链接</b> - 找到第一个符号链接后停止,不会递归解析
 *   <li><b>权限要求</b> - 需要对路径有读取权限
 *   <li><b>异常处理</b> - I/O 错误会向上抛出
 *   <li><b>相对路径</b> - 建议传入绝对路径,避免歧义
 * </ul>
 *
 * @see java.nio.file.Files#isSymbolicLink(Path)
 * @see java.nio.file.Path#toRealPath()
 */
public class LocalFileUtils {

    /**
     * 获取目标路径 (如果原始路径包含符号链接则返回真实路径,否则返回原始路径)。
     *
     * <p>该方法从路径的最后一个组件开始,向根目录逐级检查是否为符号链接。
     * 一旦发现符号链接,就将其替换为链接目标的真实路径,并保留后续的路径部分。
     *
     * <p><b>算法</b>:
     * <pre>
     * 输入: /link/to/file.txt (假设 /link 是符号链接,指向 /real)
     * 1. 检查 /link/to/file.txt - 不是符号链接
     * 2. suffixPath = file.txt
     * 3. 检查 /link/to - 不是符号链接
     * 4. suffixPath = to/file.txt
     * 5. 检查 /link - 是符号链接!
     * 6. 获取真实路径: /real
     * 7. 返回: /real/to/file.txt
     * </pre>
     *
     * @param path 原始路径
     * @return 如果路径中包含符号链接,返回替换后的真实路径;否则返回原始路径
     * @throws IOException 如果读取路径信息时发生 I/O 错误
     */
    public static Path getTargetPathIfContainsSymbolicPath(Path path) throws IOException {
        Path targetPath = path;
        Path suffixPath = Paths.get("");
        while (path != null && path.getFileName() != null) {
            if (Files.isSymbolicLink(path)) {
                Path linkedPath = path.toRealPath();
                targetPath = Paths.get(linkedPath.toString(), suffixPath.toString());
                break;
            }
            suffixPath = Paths.get(path.getFileName().toString(), suffixPath.toString());
            path = path.getParent();
        }
        return targetPath;
    }
}
