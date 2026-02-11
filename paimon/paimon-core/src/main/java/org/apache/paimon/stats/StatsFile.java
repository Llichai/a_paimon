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

package org.apache.paimon.stats;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/** 包含统计信息的统计文件。 */
public class StatsFile {

    private final FileIO fileIO;
    private final PathFactory pathFactory;

    public StatsFile(FileIO fileIO, PathFactory pathFactory) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
    }

    /**
     * 从统计文件名读取统计信息。
     *
     * @return 统计信息
     */
    public Statistics read(String fileName) {
        return Statistics.fromPath(fileIO, pathFactory.toPath(fileName));
    }

    /**
     * 将统计信息写入统计文件。
     *
     * @return 写入的文件名
     */
    public String write(Statistics stats) {
        Path path = pathFactory.newPath();

        try {
            fileIO.writeFile(path, stats.toJson(), false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write stats file: " + path, e);
        }
        return path.getName();
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }

    public boolean exists(String fileName) {
        try {
            return fileIO.exists(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
