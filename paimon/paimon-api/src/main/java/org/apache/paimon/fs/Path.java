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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.StringUtils;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 文件或目录路径。
 *
 * <p>该类用于在 {@code FileIO} 中命名文件或目录,是 Paimon 文件系统抽象层的核心类。
 * 路径字符串使用斜杠(/)作为目录分隔符,支持跨平台的路径表示和操作。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>路径表示</b>: 统一表示文件和目录路径
 *   <li><b>路径解析</b>: 支持 URI 格式的路径解析
 *   <li><b>路径操作</b>: 支持路径的组合、规范化等操作
 *   <li><b>跨平台</b>: 处理 Windows 和 Unix 系统的路径差异
 * </ul>
 *
 * <h2>路径格式</h2>
 * <p>路径字符串本质上是 URI,但具有以下特点:
 * <ul>
 *   <li>元素不需要转义
 *   <li>使用斜杠(/)作为目录分隔符
 *   <li>支持额外的规范化处理
 *   <li>支持 scheme、authority 和 path 组件
 * </ul>
 *
 * <h2>路径示例</h2>
 * <pre>{@code
 * // 本地文件系统路径
 * Path localPath = new Path("/tmp/data/table");
 *
 * // HDFS 路径
 * Path hdfsPath = new Path("hdfs://namenode:9000/user/paimon/table");
 *
 * // S3 路径
 * Path s3Path = new Path("s3://bucket/path/to/table");
 *
 * // Windows 路径
 * Path windowsPath = new Path("C:/Users/data/table");
 *
 * // 父子路径组合
 * Path parent = new Path("/tmp/data");
 * Path child = new Path(parent, "table");
 * // 结果: /tmp/data/table
 *
 * // 获取父路径和文件名
 * Path path = new Path("/tmp/data/table");
 * Path parent = path.getParent();  // /tmp/data
 * String name = path.getName();     // table
 * }</pre>
 *
 * <h2>路径规范化</h2>
 * <p>路径在创建时会自动进行规范化:
 * <ul>
 *   <li>移除重复的斜杠: "/tmp//data" → "/tmp/data"
 *   <li>移除末尾斜杠: "/tmp/data/" → "/tmp/data"
 *   <li>处理 Windows 驱动器字母: "C:\data" → "/C:/data"
 *   <li>将反斜杠转换为斜杠(Windows): "C:\data\file" → "/C:/data/file"
 * </ul>
 *
 * <h2>跨平台支持</h2>
 * <p>该类自动处理不同操作系统的路径格式:
 * <ul>
 *   <li><b>Windows</b>: 支持驱动器字母(C:)和反斜杠(\)
 *   <li><b>Unix/Linux</b>: 使用标准的斜杠(/)分隔符
 *   <li><b>相对路径</b>: 自动添加 "./" 前缀(Linux)
 * </ul>
 *
 * <h2>URI 组件</h2>
 * <p>路径可以包含以下 URI 组件:
 * <ul>
 *   <li><b>scheme</b>: 协议类型(hdfs、s3、file 等)
 *   <li><b>authority</b>: 主机和端口信息
 *   <li><b>path</b>: 实际的路径字符串
 *   <li><b>fragment</b>: 片段标识符(可选)
 * </ul>
 *
 * <h2>设计理念</h2>
 * <p>该类采用基于 URI 的路径抽象:
 * <ul>
 *   <li>统一不同文件系统的路径表示
 *   <li>支持本地和远程文件系统
 *   <li>提供跨平台的路径操作
 *   <li>保持路径字符串的可读性
 * </ul>
 *
 * <h2>实现特点</h2>
 * <ul>
 *   <li><b>不可变性</b>: 路径对象一旦创建就不可修改
 *   <li><b>可序列化</b>: 实现 Serializable 接口,支持序列化
 *   <li><b>可比较</b>: 实现 Comparable 接口,支持排序
 *   <li><b>线程安全</b>: 不可变对象,天然线程安全
 * </ul>
 *
 * @see org.apache.paimon.fs.FileIO
 * @since 0.4.0
 */
@Public
public class Path implements Comparable<Path>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 目录分隔符,斜杠。 */
    public static final String SEPARATOR = "/";

    /** 目录分隔符,斜杠,字符形式。 */
    public static final char SEPARATOR_CHAR = '/';

    /** 当前目录,"."。 */
    public static final String CUR_DIR = ".";

    /** 当前主机是否为 Windows 机器。 */
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

    /** 预编译的正则表达式,用于匹配 Windows 驱动器字母。 */
    private static final Pattern HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:");

    /** 预编译的正则表达式,用于匹配重复的斜杠。 */
    private static final Pattern SLASHES = Pattern.compile("/+");

    /** 层次化的 URI。 */
    private URI uri;

    /**
     * 基于父路径和子路径创建新路径。
     *
     * <p>将子路径解析为相对于父路径的路径。
     *
     * @param parent 父路径字符串
     * @param child 子路径字符串
     */
    public Path(String parent, String child) {
        this(new Path(parent), new Path(child));
    }

    /**
     * 基于父路径和子路径创建新路径。
     *
     * <p>将子路径解析为相对于父路径的路径。
     *
     * @param parent 父路径对象
     * @param child 子路径字符串
     */
    public Path(Path parent, String child) {
        this(parent, new Path(child));
    }

    /**
     * 基于父路径和子路径创建新路径。
     *
     * <p>将子路径解析为相对于父路径的路径。
     *
     * @param parent 父路径字符串
     * @param child 子路径对象
     */
    public Path(String parent, Path child) {
        this(new Path(parent), child);
    }

    /**
     * 基于父路径和子路径创建新路径。
     *
     * <p>将子路径解析为相对于父路径的路径。这是路径组合的核心方法:
     * <ul>
     *   <li>在父路径的路径部分末尾添加斜杠(如果需要)
     *   <li>使用 URI 解析机制将子路径解析为相对于父路径的路径
     *   <li>保留父路径的 scheme 和 authority
     * </ul>
     *
     * <p>示例:
     * <pre>{@code
     * Path parent = new Path("/tmp/data");
     * Path child = new Path("table");
     * Path result = new Path(parent, child);
     * // result: /tmp/data/table
     * }</pre>
     *
     * @param parent 父路径对象
     * @param child 子路径对象
     */
    public Path(Path parent, Path child) {
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        String parentPath = parentUri.getPath();
        if (!(parentPath.equals(SEPARATOR) || parentPath.isEmpty())) {
            try {
                parentUri =
                        new URI(
                                parentUri.getScheme(),
                                parentUri.getAuthority(),
                                parentUri.getPath() + SEPARATOR,
                                null,
                                parentUri.getFragment());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        URI resolved = parentUri.resolve(child.uri);
        initialize(
                resolved.getScheme(),
                resolved.getAuthority(),
                resolved.getPath(),
                resolved.getFragment());
    }

    /**
     * 检查路径字符串参数。
     *
     * <p>验证路径字符串不为 null 且长度不为零,否则抛出 {@link IllegalArgumentException}。
     *
     * @param path 要检查的路径字符串
     * @throws IllegalArgumentException 如果路径为 null 或空字符串
     */
    private void checkPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
    }

    /**
     * 从字符串构造路径。
     *
     * <p>路径字符串是 URI,但具有未转义的元素和额外的规范化处理。
     * 该构造函数会解析路径字符串的各个组件(scheme、authority、path)并进行规范化。
     *
     * <p>解析过程:
     * <ol>
     *   <li>处理 Windows 驱动器字母(如 "C:")
     *   <li>解析 scheme(如 "hdfs:")
     *   <li>解析 authority(如 "//namenode:9000")
     *   <li>解析 path 部分
     *   <li>规范化路径
     * </ol>
     *
     * <p>示例:
     * <pre>{@code
     * new Path("/tmp/data");
     * new Path("hdfs://namenode:9000/user/data");
     * new Path("C:/Users/data");
     * new Path("s3://bucket/path/to/file");
     * }</pre>
     *
     * @param pathString 路径字符串
     * @throws IllegalArgumentException 如果路径字符串为 null 或空
     */
    public Path(String pathString) {
        checkPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString) && pathString.charAt(0) != SEPARATOR_CHAR) {
            pathString = SEPARATOR + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        int colon = pathString.indexOf(':');
        int slash = pathString.indexOf(SEPARATOR_CHAR);
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start)
                && (pathString.length() - start > 2)) { // has authority
            int nextSlash = pathString.indexOf(SEPARATOR_CHAR, start + 2);
            int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        String path = pathString.substring(start);

        initialize(scheme, authority, path, null);
    }

    /**
     * 从 URI 构造路径。
     *
     * <p>直接使用提供的 URI,并进行规范化处理。
     *
     * @param aUri 源 URI
     */
    public Path(URI aUri) {
        uri = aUri.normalize();
    }

    /**
     * 从 scheme、authority 和路径字符串构造路径。
     *
     * <p>该构造函数用于显式指定 URI 的各个组件。主要用于构造带有特定
     * scheme 和 authority 的路径(如 HDFS、S3 等远程文件系统路径)。
     *
     * <p>示例:
     * <pre>{@code
     * new Path("hdfs", "namenode:9000", "/user/data");
     * new Path("s3", "my-bucket", "/path/to/file");
     * new Path("file", null, "/tmp/data");
     * }</pre>
     *
     * @param scheme scheme 字符串(如 "hdfs", "s3")
     * @param authority authority 字符串(如 "namenode:9000")
     * @param path 路径字符串
     * @throws IllegalArgumentException 如果路径字符串为 null 或空
     */
    public Path(String scheme, String authority, String path) {
        checkPathArg(path);

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(path) && path.charAt(0) != SEPARATOR_CHAR) {
            path = SEPARATOR + path;
        }

        // add "./" in front of Linux relative paths so that a path containing
        // a colon e.q. "a:b" will not be interpreted as scheme "a".
        if (!WINDOWS && path.charAt(0) != SEPARATOR_CHAR) {
            path = CUR_DIR + SEPARATOR + path;
        }

        initialize(scheme, authority, path, null);
    }

    /**
     * 初始化路径对象。
     *
     * <p>根据给定的 scheme、authority、path 和 fragment 字符串创建内部 URI 对象。
     * 在创建过程中会对路径进行规范化处理。
     *
     * @param scheme scheme 字符串
     * @param authority authority 字符串
     * @param path 路径字符串
     * @param fragment fragment 字符串
     * @throws IllegalArgumentException 如果 URI 语法错误
     */
    private void initialize(String scheme, String authority, String path, String fragment) {
        try {
            this.uri =
                    new URI(scheme, authority, normalizePath(scheme, path), null, fragment)
                            .normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 规范化路径字符串。
     *
     * <p>使用非重复的正斜杠作为路径分隔符,并移除末尾的路径分隔符。
     * 规范化规则包括:
     * <ul>
     *   <li>移除重复的斜杠: "/tmp//data" → "/tmp/data"
     *   <li>在 Windows 上将反斜杠替换为斜杠
     *   <li>移除非根路径末尾的斜杠
     * </ul>
     *
     * @param scheme URI scheme,用于判断是否应该替换反斜杠
     * @param path scheme 特定的路径部分
     * @return 规范化后的路径字符串
     */
    private static String normalizePath(String scheme, String path) {
        // Remove duplicated slashes.
        path = SLASHES.matcher(path).replaceAll(SEPARATOR);

        // Remove backslashes if this looks like a Windows path. Avoid
        // the substitution if it looks like a non-local URI.
        if (WINDOWS
                && (hasWindowsDrive(path)
                        || (scheme == null)
                        || (scheme.isEmpty())
                        || (scheme.equals("file")))) {
            path = StringUtils.replace(path, "\\", SEPARATOR);
        }

        // trim trailing slash from non-root path (ignoring windows drive)
        int minLength = startPositionWithoutWindowsDrive(path) + 1;
        if (path.length() > minLength && path.endsWith(SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    /**
     * 检查路径字符串是否包含 Windows 驱动器字母。
     *
     * <p>Windows 驱动器字母格式: "C:", "D:", "/C:", 等。
     *
     * @param path 路径字符串
     * @return 如果路径包含 Windows 驱动器字母返回 true,否则返回 false
     */
    private static boolean hasWindowsDrive(String path) {
        return (WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find());
    }

    /**
     * 获取去除 Windows 驱动器字母后的起始位置。
     *
     * <p>用于跳过 Windows 驱动器字母部分,找到实际路径的起始位置。
     *
     * @param path 路径字符串
     * @return 去除 Windows 驱动器字母后的起始位置
     */
    private static int startPositionWithoutWindowsDrive(String path) {
        if (hasWindowsDrive(path)) {
            return path.charAt(0) == SEPARATOR_CHAR ? 3 : 2;
        } else {
            return 0;
        }
    }

    /**
     * 将此路径转换为 URI。
     *
     * @return 此路径对应的 URI
     */
    public URI toUri() {
        return uri;
    }

    /**
     * 返回路径的最后一个组件。
     *
     * <p>即路径中最后一个分隔符之后的所有内容,通常是文件名或目录名。
     *
     * <p>示例:
     * <pre>{@code
     * new Path("/tmp/data/table").getName();  // returns "table"
     * new Path("s3://bucket/path/file").getName();  // returns "file"
     * }</pre>
     *
     * @return 路径的最后一个组件
     */
    public String getName() {
        String path = uri.getPath();
        int slash = path.lastIndexOf(SEPARATOR);
        return path.substring(slash + 1);
    }

    /**
     * 为此路径创建临时路径。
     *
     * <p>临时路径用于原子性写入操作,先写入临时文件,然后重命名为目标文件。
     * 临时文件名格式: ".{原文件名}.{UUID}.tmp"
     *
     * <p>示例:
     * <pre>{@code
     * Path path = new Path("/tmp/data/table");
     * Path tempPath = path.createTempPath();
     * // tempPath: /tmp/data/.table.uuid.tmp
     * }</pre>
     *
     * @return 临时路径对象
     */
    public Path createTempPath() {
        return new Path(getParent(), String.format(".%s.%s.tmp", getName(), UUID.randomUUID()));
    }

    /**
     * 返回路径的父路径。
     *
     * <p>如果路径已经是根路径,则返回 null。
     *
     * <p>示例:
     * <pre>{@code
     * new Path("/tmp/data/table").getParent();  // returns "/tmp/data"
     * new Path("/tmp").getParent();             // returns "/"
     * new Path("/").getParent();                // returns null
     * }</pre>
     *
     * @return 父路径,如果已经是根路径则返回 null
     */
    public Path getParent() {
        String path = uri.getPath();
        int lastSlash = path.lastIndexOf(SEPARATOR_CHAR);
        int start = startPositionWithoutWindowsDrive(path);
        if ((path.length() == start)
                || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }
        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            parent = path.substring(0, lastSlash == start ? start + 1 : lastSlash);
        }
        return new Path(uri.getScheme(), uri.getAuthority(), parent);
    }

    /**
     * 返回路径的字符串表示形式。
     *
     * <p>注意: 不使用 uri.toString(),因为它会转义所有字符。
     * 我们希望在字符串中保留非法字符的未转义形式,以便进行 glob 处理等操作。
     *
     * <p>返回格式: [scheme:][[//authority]/path][#fragment]
     *
     * @return 路径的字符串表示形式
     */
    @Override
    public String toString() {
        // we can't use uri.toString(), which escapes everything, because we want
        // illegal characters unescaped in the string, for glob processing, etc.
        StringBuilder buffer = new StringBuilder();
        if (uri.getScheme() != null) {
            buffer.append(uri.getScheme()).append(":");
        }
        if (uri.getAuthority() != null) {
            buffer.append("//").append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            if (path.indexOf(SEPARATOR_CHAR) == 0
                    && hasWindowsDrive(path)
                    && // has windows drive
                    uri.getScheme() == null
                    && // but no scheme
                    uri.getAuthority() == null) {
                path = path.substring(1); // remove slash before drive
            }
            buffer.append(path);
        }
        if (uri.getFragment() != null) {
            buffer.append("#").append(uri.getFragment());
        }
        return buffer.toString();
    }

    /**
     * 比较路径对象是否相等。
     *
     * <p>两个路径相等当且仅当它们的 URI 相等。
     *
     * @param o 要比较的对象
     * @return 如果路径相等返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Path)) {
            return false;
        }
        Path that = (Path) o;
        return this.uri.equals(that.uri);
    }

    /**
     * 返回路径的哈希码。
     *
     * @return 路径的哈希码
     */
    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    /**
     * 比较路径的顺序。
     *
     * <p>路径的比较基于其 URI 的字典顺序。
     *
     * @param that 要比较的路径
     * @return 负数、零或正数,分别表示此路径小于、等于或大于指定路径
     */
    @Override
    public int compareTo(Path that) {
        return this.uri.compareTo(that.uri);
    }
}
