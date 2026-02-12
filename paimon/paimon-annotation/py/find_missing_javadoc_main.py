import os
import re
from pathlib import Path

def has_class_javadoc(file_path):
    """检查文件是否有类级别的JavaDoc注释"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 检查是否有类或接口定义前的JavaDoc
        lines = content.split('\n')
        
        # 查找第一个类或接口定义
        for i, line in enumerate(lines):
            if re.match(r'\s*(public\s+)?(abstract\s+)?(final\s+)?(class|interface|enum)\s+\w+', line):
                # 检查前面是否有JavaDoc
                if i > 0:
                    # 向上查找JavaDoc注释
                    for j in range(i-1, -1, -1):
                        if '/**' in lines[j]:
                            return True
                        if '*/' in lines[j]:
                            return True
                        if lines[j].strip() and not lines[j].strip().startswith('*') and not lines[j].strip().startswith('//') and not lines[j].strip().startswith('@'):
                            break
                # 如果没找到JavaDoc就返回False
                return False
        
        return False
    except:
        return False

# 只检查 src/main/java 下的文件
paimon_core_path = r"D:\a_git\paimon\paimon-core\src\main\java"
java_files = []

for root, dirs, files in os.walk(paimon_core_path):
    for file in files:
        if file.endswith('.java'):
            file_path = os.path.join(root, file)
            java_files.append(file_path)

# 检查没有JavaDoc的文件
missing_javadoc = []
for file_path in sorted(java_files):
    if not has_class_javadoc(file_path):
        relative_path = os.path.relpath(file_path, paimon_core_path)
        missing_javadoc.append((relative_path, file_path))

print(f"总Java文件数: {len(java_files)}")
print(f"缺少JavaDoc的文件数: {len(missing_javadoc)}\n")

# 按包名分组显示
packages = {}
for file_path, full_path in missing_javadoc:
    package = os.path.dirname(file_path).replace(os.sep, '.')
    if package not in packages:
        packages[package] = []
    packages[package].append((os.path.basename(file_path), full_path))

for package in sorted(packages.keys()):
    print(f"\n{package}:")
    for file, full_path in sorted(packages[package]):
        print(f"  - {file}")
        print(f"    {full_path}")
