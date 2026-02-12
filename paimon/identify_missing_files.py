import os
import re

def extract_package_info(file_path):
    """提取文件的包名和类名"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        package = None
        class_name = None
        has_javadoc = False
        
        for i, line in enumerate(lines):
            if 'package ' in line and not line.strip().startswith('//'):
                pkg_match = re.search(r'package\s+([\w.]+)', line)
                if pkg_match:
                    package = pkg_match.group(1)
            
            # 检查是否有JavaDoc（以/** 开头）
            if line.strip().startswith('/**'):
                has_javadoc = True
            
            # 查找类定义
            if re.match(r'\s*(public\s+)?(abstract\s+)?(final\s+)?(class|interface|enum)\s+\w+', line):
                class_match = re.search(r'(class|interface|enum)\s+(\w+)', line)
                if class_match:
                    class_name = class_match.group(2)
                break
        
        return package, class_name, has_javadoc
    except:
        return None, None, False

paimon_core_path = r"D:\a_git\paimon\paimon-core\src\main\java"
java_files = []

for root, dirs, files in os.walk(paimon_core_path):
    for file in files:
        if file.endswith('.java'):
            file_path = os.path.join(root, file)
            java_files.append(file_path)

# 检查每个文件的JavaDoc情况
files_with_javadoc = 0
files_without_javadoc = 0
missing_files = []

for file_path in sorted(java_files):
    package, class_name, has_javadoc = extract_package_info(file_path)
    relative_path = os.path.relpath(file_path, paimon_core_path)
    
    if has_javadoc:
        files_with_javadoc += 1
    else:
        files_without_javadoc += 1
        missing_files.append((relative_path, package, class_name))

print(f"总Java文件数: {len(java_files)}")
print(f"有JavaDoc的文件: {files_with_javadoc}")
print(f"无JavaDoc的文件: {files_without_javadoc}")

if missing_files:
    print(f"\n无JavaDoc的文件列表:")
    for rel_path, pkg, cls in sorted(missing_files):
        print(f"  {rel_path} ({cls})")
else:
    print("\n所有文件都有JavaDoc!")
