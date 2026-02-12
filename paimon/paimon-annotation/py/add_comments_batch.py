#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量为 operation 包文件添加中文注释的脚本
"""

import os
import re

# 待处理文件列表
FILES_TO_ANNOTATE = [
    ("DataEvolutionFileStoreScan.java", "已完成"),
    ("AbstractFileStoreWrite.java", "待处理"),
    ("KeyValueFileStoreWrite.java", "待处理"),
    ("AppendFileStoreWrite.java", "已完成"),
    ("BaseAppendFileStoreWrite.java", "待处理"),
    ("BucketedAppendFileStoreWrite.java", "待处理"),
    ("MemoryFileStoreWrite.java", "待处理"),
    ("BundleFileStoreWriter.java", "待处理"),
    ("FileSystemWriteRestore.java", "待处理"),
    ("FileStoreCommitImpl.java", "待处理"),
    ("SplitRead.java", "待处理"),
    ("MergeFileSplitRead.java", "待处理"),
    ("RawFileSplitRead.java", "待处理"),
    ("DataEvolutionSplitRead.java", "待处理"),
    ("SnapshotDeletion.java", "待处理"),
    ("ChangelogDeletion.java", "待处理"),
    ("TagDeletion.java", "待处理"),
    ("PartitionExpire.java", "待处理"),
    ("FileDeletionBase.java", "待处理"),
    ("OrphanFilesClean.java", "待处理"),
    ("LocalOrphanFilesClean.java", "待处理"),
    ("CleanOrphanFilesResult.java", "待处理"),
    ("Lock.java", "待处理"),
    ("ManifestFileMerger.java", "待处理"),
    ("RestoreFiles.java", "待处理"),
    ("BucketSelectConverter.java", "待处理"),
    ("WriteRestore.java", "待处理"),
    ("ReverseReader.java", "待处理"),
    ("ListUnexistingFiles.java", "待处理"),
]

def count_pending():
    return sum(1 for _, status in FILES_TO_ANNOTATE if status == "待处理")

if __name__ == "__main__":
    pending = count_pending()
    print(f"总文件数: {len(FILES_TO_ANNOTATE)}")
    print(f"待处理文件数: {pending}")
    print(f"已完成文件数: {len(FILES_TO_ANNOTATE) - pending}")
