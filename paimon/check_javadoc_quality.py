import os
import re

def check_javadoc_quality(file_path):
    """检查JavaDoc的质量"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 查找类定义前的JavaDoc
        lines = content.split('\n')
        javadoc_block = []
        class_line_idx = -1
        
        for i, line in enumerate(lines):
            if re.match(r'\s*(public\s+)?(abstract\s+)?(final\s+)?(class|interface|enum)\s+\w+', line):
                class_line_idx = i
                break
        
        if class_line_idx <= 0:
            return None
        
        # 找JavaDoc块
        for i in range(class_line_idx-1, -1, -1):
            if '/**' in lines[i]:
                # 从/**开始向前收集
                start = i
                end = i
                for j in range(i, class_line_idx):
                    if '*/' in lines[j]:
                        end = j
                        break
                javadoc_block = lines[start:end+1]
                break
        
        if not javadoc_block:
            return "NO_JAVADOC"
        
        javadoc_text = '\n'.join(javadoc_block)
        
        # 检查是否有中文描述
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', javadoc_text))
        
        # 检查是否有@see, @param, @return等标签
        has_tags = bool(re.search(r'@\w+', javadoc_text))
        
        # 计算描述长度
        desc_lines = [l.strip() for l in javadoc_block if l.strip() and not l.strip().startswith('*') and not l.strip().startswith('/**') and not l.strip().startswith('*/')]
        desc_length = sum(len(l) for l in desc_lines)
        
        if desc_length < 20:
            return "TOO_SHORT"
        
        if not has_chinese:
            return "NO_CHINESE"
        
        return "OK"
    except:
        return "ERROR"

paimon_core_path = r"D:\a_git\paimon\paimon-core\src\main\java"
java_files = []

for root, dirs, files in os.walk(paimon_core_path):
    for file in files:
        if file.endswith('.java'):
            file_path = os.path.join(root, file)
            java_files.append(file_path)

# 检查质量
quality_stats = {
    "OK": [],
    "NO_JAVADOC": [],
    "TOO_SHORT": [],
    "NO_CHINESE": [],
    "ERROR": []
}

for file_path in java_files:
    quality = check_javadoc_quality(file_path)
    if quality:
        relative_path = os.path.relpath(file_path, paimon_core_path)
        quality_stats[quality].append(relative_path)

print(f"总文件数: {len(java_files)}")
print(f"质量OK: {len(quality_stats['OK'])}")
print(f"缺少JavaDoc: {len(quality_stats['NO_JAVADOC'])}")
print(f"JavaDoc太短: {len(quality_stats['TOO_SHORT'])}")
print(f"缺少中文: {len(quality_stats['NO_CHINESE'])}")
print(f"错误: {len(quality_stats['ERROR'])}")

if quality_stats['NO_JAVADOC']:
    print(f"\n缺少JavaDoc的文件({len(quality_stats['NO_JAVADOC'])}):")
    for f in sorted(quality_stats['NO_JAVADOC'])[:20]:
        print(f"  - {f}")

if quality_stats['NO_CHINESE']:
    print(f"\n缺少中文的文件({len(quality_stats['NO_CHINESE'])}):")
    for f in sorted(quality_stats['NO_CHINESE'])[:20]:
        print(f"  - {f}")

if quality_stats['TOO_SHORT']:
    print(f"\nJavaDoc太短的文件({len(quality_stats['TOO_SHORT'])}):")
    for f in sorted(quality_stats['TOO_SHORT'])[:20]:
        print(f"  - {f}")
