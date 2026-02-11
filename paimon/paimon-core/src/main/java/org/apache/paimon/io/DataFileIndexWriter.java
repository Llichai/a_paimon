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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexCommon;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据文件索引写入器,为单个数据文件创建索引。
 *
 * <p>主要功能:
 * <ul>
 *   <li>为指定列创建索引(Bloom Filter、Bitmap等)
 *   <li>支持普通列索引和Map嵌套列索引
 *   <li>自动决定索引存储位置(嵌入或独立文件)
 *   <li>逐记录更新索引内容
 * </ul>
 *
 * <p>索引存储策略:
 * <ul>
 *   <li><b>嵌入存储</b>: 索引大小 ≤ fileIndexInManifestThreshold
 *       <ul>
 *         <li>存储位置: DataFileMeta.embeddedIndex字段
 *         <li>优势: 减少文件数量,加快元数据读取
 *         <li>适用: 小索引(如Bloom Filter)
 *       </ul>
 *   <li><b>独立文件</b>: 索引大小 > fileIndexInManifestThreshold
 *       <ul>
 *         <li>存储位置: 独立的.index文件
 *         <li>优势: 避免元数据过大
 *         <li>适用: 大索引(如Bitmap Index)
 *       </ul>
 * </ul>
 *
 * <p>支持的索引类型:
 * <ul>
 *   <li>bloom: Bloom Filter (布隆过滤器)
 *   <li>bitmap: Bitmap Index (位图索引)
 *   <li>hash: Hash Index (哈希索引)
 * </ul>
 *
 * <p>Map类型索引支持:
 * <ul>
 *   <li>为Map的特定key创建索引
 *   <li>格式: column_name['key1'], column_name['key2']
 *   <li>每个key独立维护索引
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建索引写入器
 * DataFileIndexWriter indexWriter = DataFileIndexWriter.create(
 *     fileIO, indexPath, rowType, fileIndexOptions);
 *
 * // 写入数据时更新索引
 * while (hasNextRow) {
 *     InternalRow row = readNextRow();
 *     dataWriter.write(row);
 *     indexWriter.write(row);  // 同步更新索引
 * }
 *
 * // 关闭索引写入器,序列化索引
 * indexWriter.close();
 *
 * // 获取索引结果
 * FileIndexResult result = indexWriter.result();
 * if (result.independentIndexFile() != null) {
 *     // 独立文件模式
 * } else {
 *     // 嵌入模式
 *     byte[] indexBytes = result.embeddedIndexBytes();
 * }
 * }</pre>
 *
 * @see FileIndexer 索引器接口
 * @see FileIndexWriter 索引写入器接口
 */
public final class DataFileIndexWriter implements Closeable {

    public static final FileIndexResult EMPTY_RESULT = FileIndexResult.of(null, null);

    private final FileIO fileIO;

    private final Path path;

    // if the filter size greater than fileIndexInManifestThreshold, we put it in file
    private final long inManifestThreshold;

    // index type, column name -> index maintainer
    private final Map<String, Map<String, IndexMaintainer>> indexMaintainers = new HashMap<>();

    private String resultFileName;

    private byte[] embeddedIndexBytes;

    public DataFileIndexWriter(
            FileIO fileIO,
            Path path,
            RowType rowType,
            FileIndexOptions fileIndexOptions,
            @Nullable Map<String, String> colNameMapping) {
        this.fileIO = fileIO;
        this.path = path;
        List<DataField> fields = rowType.getFields();
        Map<String, DataField> map = new HashMap<>();
        Map<String, Integer> index = new HashMap<>();
        fields.forEach(
                dataField -> {
                    map.put(dataField.name(), dataField);
                    index.put(dataField.name(), rowType.getFieldIndex(dataField.name()));
                });
        for (Map.Entry<FileIndexOptions.Column, Map<String, Options>> entry :
                fileIndexOptions.entrySet()) {
            FileIndexOptions.Column entryColumn = entry.getKey();
            String colName = entryColumn.getColumnName();
            if (colNameMapping != null) {
                colName = colNameMapping.getOrDefault(colName, null);
                if (colName == null) {
                    continue;
                }
            }

            String columnName = colName;
            DataField field = map.get(columnName);
            if (field == null) {
                throw new IllegalArgumentException(columnName + " does not exist in column fields");
            }

            for (Map.Entry<String, Options> typeEntry : entry.getValue().entrySet()) {
                String indexType = typeEntry.getKey();
                Map<String, IndexMaintainer> column2maintainers =
                        indexMaintainers.computeIfAbsent(indexType, k -> new HashMap<>());
                IndexMaintainer maintainer = column2maintainers.get(columnName);
                if (entryColumn.isNestedColumn()) {
                    if (field.type().getTypeRoot() != DataTypeRoot.MAP) {
                        throw new IllegalArgumentException(
                                "Column "
                                        + columnName
                                        + " is nested column, but is not map type. Only should map type yet.");
                    }
                    MapFileIndexMaintainer mapMaintainer = (MapFileIndexMaintainer) maintainer;
                    if (mapMaintainer == null) {
                        MapType mapType = (MapType) field.type();
                        mapMaintainer =
                                new MapFileIndexMaintainer(
                                        columnName,
                                        indexType,
                                        mapType.getKeyType(),
                                        mapType.getValueType(),
                                        fileIndexOptions.getMapTopLevelOptions(
                                                columnName, typeEntry.getKey()),
                                        index.get(columnName));
                        column2maintainers.put(columnName, mapMaintainer);
                    }
                    mapMaintainer.add(entryColumn.getNestedColumnName(), typeEntry.getValue());
                } else {
                    if (maintainer == null) {
                        maintainer =
                                new FileIndexMaintainer(
                                        columnName,
                                        indexType,
                                        FileIndexer.create(
                                                        indexType,
                                                        field.type(),
                                                        typeEntry.getValue())
                                                .createWriter(),
                                        InternalRow.createFieldGetter(
                                                field.type(), index.get(columnName)));
                        column2maintainers.put(columnName, maintainer);
                    }
                }
            }
        }
        this.inManifestThreshold = fileIndexOptions.fileIndexInManifestThreshold();
    }

    /**
     * 写入一条记录,更新所有配置的索引。
     *
     * <p>对于每个索引维护器:
     * <ul>
     *   <li>从记录中提取对应字段的值
     *   <li>调用索引写入器更新索引结构
     *   <li>例如Bloom Filter会添加该值的哈希
     * </ul>
     *
     * @param row 要索引的数据行
     */
    public void write(InternalRow row) {
        indexMaintainers
                .values()
                .forEach(
                        column2maintainers ->
                                column2maintainers.values().forEach(index -> index.write(row)));
    }

    /**
     * 关闭索引写入器并序列化索引数据。
     *
     * <p>执行步骤:
     * <ol>
     *   <li>序列化所有索引维护器的数据
     *   <li>使用FileIndexFormat格式化索引数据
     *   <li>根据大小决定存储位置:
     *       <ul>
     *         <li>≤ threshold: 存入embeddedIndexBytes(嵌入式)
     *         <li>> threshold: 写入独立文件,记录文件名
     *       </ul>
     * </ol>
     *
     * @throws IOException 写入文件失败
     */
    @Override
    public void close() throws IOException {
        Map<String, Map<String, byte[]>> indexMaps = serializeMaintainers();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(out)) {
            writer.writeColumnIndexes(indexMaps);
        }

        if (out.size() > inManifestThreshold) {
            try (OutputStream outputStream = fileIO.newOutputStream(path, true)) {
                outputStream.write(out.toByteArray());
            }
            resultFileName = path.getName();
        } else {
            embeddedIndexBytes = out.toByteArray();
        }
    }

    public Map<String, Map<String, byte[]>> serializeMaintainers() {
        Map<String, Map<String, byte[]>> indexMaps = new HashMap<>();
        for (Map<String, IndexMaintainer> columnIndexMaintainers : indexMaintainers.values()) {
            for (IndexMaintainer indexMaintainer : columnIndexMaintainers.values()) {
                Map<String, byte[]> mapBytes = indexMaintainer.serializedBytes();
                for (Map.Entry<String, byte[]> entry : mapBytes.entrySet()) {
                    indexMaps
                            .computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                            .put(indexMaintainer.getIndexType(), entry.getValue());
                }
            }
        }
        return indexMaps;
    }

    public FileIndexResult result() {
        return FileIndexResult.of(embeddedIndexBytes, resultFileName);
    }

    @Nullable
    public static DataFileIndexWriter create(
            FileIO fileIO, Path path, RowType rowType, FileIndexOptions fileIndexOptions) {
        return create(fileIO, path, rowType, fileIndexOptions, null);
    }

    @Nullable
    public static DataFileIndexWriter create(
            FileIO fileIO,
            Path path,
            RowType rowType,
            FileIndexOptions fileIndexOptions,
            @Nullable Map<String, String> colNameMapping) {
        return fileIndexOptions.isEmpty()
                ? null
                : new DataFileIndexWriter(fileIO, path, rowType, fileIndexOptions, colNameMapping);
    }

    /** 文件索引结果,包含嵌入式索引字节或独立索引文件名。 */
    public interface FileIndexResult {

        @Nullable
        byte[] embeddedIndexBytes();

        @Nullable
        String independentIndexFile();

        static FileIndexResult of(byte[] embeddedIndexBytes, String resultFileName) {
            return new FileIndexResult() {

                @Override
                public byte[] embeddedIndexBytes() {
                    return embeddedIndexBytes;
                }

                @Override
                public String independentIndexFile() {
                    return resultFileName;
                }
            };
        }
    }

    interface IndexMaintainer {

        void write(InternalRow row);

        String getIndexType();

        Map<String, byte[]> serializedBytes();
    }

    /** 单列索引维护器,为一个列维护一种类型的索引。 */
    private static class FileIndexMaintainer implements IndexMaintainer {

        private final String columnName;
        private final String indexType;
        private final FileIndexWriter fileIndexWriter;
        private final InternalRow.FieldGetter getter;

        public FileIndexMaintainer(
                String columnName,
                String indexType,
                FileIndexWriter fileIndexWriter,
                InternalRow.FieldGetter getter) {
            this.columnName = columnName;
            this.indexType = indexType;
            this.fileIndexWriter = fileIndexWriter;
            this.getter = getter;
        }

        public void write(InternalRow row) {
            fileIndexWriter.writeRecord(getter.getFieldOrNull(row));
        }

        public String getIndexType() {
            return indexType;
        }

        public Map<String, byte[]> serializedBytes() {
            return Collections.singletonMap(columnName, fileIndexWriter.serializedBytes());
        }
    }

    /**
     * Map类型字段的索引维护器,支持为Map的不同key创建独立索引。
     *
     * <p>使用场景:
     * <ul>
     *   <li>Map<String, Integer> tags: 为tags['region']、tags['city']创建索引
     *   <li>每个嵌套key独立维护一个索引写入器
     *   <li>如果某行没有该key,写入null到索引
     * </ul>
     */
    private static class MapFileIndexMaintainer implements IndexMaintainer {

        private final String columnName;
        private final String indexType;
        private final Options options;
        private final DataType valueType;
        private final Map<String, org.apache.paimon.fileindex.FileIndexWriter> indexWritersMap;
        private final InternalArray.ElementGetter valueElementGetter;
        private final int position;

        public MapFileIndexMaintainer(
                String columnName,
                String indexType,
                DataType keyType,
                DataType valueType,
                Options options,
                int position) {
            this.columnName = columnName;
            this.indexType = indexType;
            this.valueType = valueType;
            this.options = options;
            this.position = position;
            this.indexWritersMap = new HashMap<>();
            this.valueElementGetter = InternalArray.createElementGetter(valueType);

            DataTypeRoot rootType = keyType.getTypeRoot();
            if (rootType != DataTypeRoot.CHAR && rootType != DataTypeRoot.VARCHAR) {
                throw new IllegalArgumentException(
                        "Only support map data type with key field of CHAR、VARCHAR、STRING.");
            }
        }

        public void write(InternalRow row) {
            if (row.isNullAt(position)) {
                indexWritersMap.values().forEach(write -> write.writeRecord(null));
                return;
            }
            InternalMap internalMap = row.getMap(position);
            InternalArray keyArray = internalMap.keyArray();
            InternalArray valueArray = internalMap.valueArray();

            Set<String> writtenKeys = new HashSet<>();
            for (int i = 0; i < keyArray.size(); i++) {
                String key = keyArray.getString(i).toString();
                org.apache.paimon.fileindex.FileIndexWriter writer =
                        indexWritersMap.getOrDefault(key, null);
                if (writer != null) {
                    writtenKeys.add(key);
                    writer.writeRecord(valueElementGetter.getElementOrNull(valueArray, i));
                }
            }

            for (Map.Entry<String, FileIndexWriter> writerEntry : indexWritersMap.entrySet()) {
                if (!writtenKeys.contains(writerEntry.getKey())) {
                    writerEntry.getValue().writeRecord(null);
                }
            }
        }

        public void add(String nestedKey, Options nestedOptions) {
            indexWritersMap.put(
                    nestedKey,
                    FileIndexer.create(
                                    indexType,
                                    valueType,
                                    new Options(options.toMap(), nestedOptions.toMap()))
                            .createWriter());
        }

        public String getIndexType() {
            return indexType;
        }

        public Map<String, byte[]> serializedBytes() {
            Map<String, byte[]> result = new HashMap<>();
            indexWritersMap.forEach(
                    (k, v) -> {
                        if (!v.empty()) {
                            result.put(
                                    FileIndexCommon.toMapKey(columnName, k), v.serializedBytes());
                        } else {
                            result.put(FileIndexCommon.toMapKey(columnName, k), null);
                        }
                    });
            return result;
        }
    }
}
