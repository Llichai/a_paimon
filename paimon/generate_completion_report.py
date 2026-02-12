import os
import re
from collections import defaultdict

def extract_file_info(file_path):
    """提取文件的包名、类名和JavaDoc信息"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        package = None
        class_name = None
        has_javadoc = False
        javadoc_lines = 0
        has_chinese = False
        javadoc_content = []
        
        for i, line in enumerate(lines):
            if 'package ' in line and not line.strip().startswith('//'):
                pkg_match = re.search(r'package\s+([\w.]+)', line)
                if pkg_match:
                    package = pkg_match.group(1)
            
            # 检查JavaDoc
            if line.strip().startswith('/**'):
                has_javadoc = True
                # 收集JavaDoc内容直到*/
                javadoc_content = [line]
                for j in range(i+1, min(i+100, len(lines))):
                    javadoc_content.append(lines[j])
                    javadoc_lines += 1
                    if '*/' in lines[j]:
                        break
                javadoc_text = '\n'.join(javadoc_content)
                if re.search(r'[\u4e00-\u9fff]', javadoc_text):
                    has_chinese = True
            
            # 查找类定义
            if re.match(r'\s*(public\s+)?(abstract\s+)?(final\s+)?(class|interface|enum)\s+\w+', line):
                class_match = re.search(r'(class|interface|enum)\s+(\w+)', line)
                if class_match:
                    class_name = class_match.group(2)
                break
        
        return {
            'package': package,
            'class': class_name,
            'has_javadoc': has_javadoc,
            'has_chinese': has_chinese,
            'javadoc_lines': javadoc_lines
        }
    except:
        return {
            'package': None,
            'class': None,
            'has_javadoc': False,
            'has_chinese': False,
            'javadoc_lines': 0
        }

paimon_core_path = r"D:\a_git\paimon\paimon-core\src\main\java"
java_files = []

for root, dirs, files in os.walk(paimon_core_path):
    for file in files:
        if file.endswith('.java'):
            file_path = os.path.join(root, file)
            java_files.append(file_path)

# 统计信息
total_files = 0
files_with_javadoc = 0
files_with_chinese = 0
total_javadoc_lines = 0
package_stats = defaultdict(lambda: {'total': 0, 'with_javadoc': 0, 'with_chinese': 0})

for file_path in java_files:
    info = extract_file_info(file_path)
    package = info['package'] if info['package'] else 'unknown'
    
    total_files += 1
    package_stats[package]['total'] += 1
    
    if info['has_javadoc']:
        files_with_javadoc += 1
        package_stats[package]['with_javadoc'] += 1
        total_javadoc_lines += info['javadoc_lines']
    
    if info['has_chinese']:
        files_with_chinese += 1
        package_stats[package]['with_chinese'] += 1

# 打印统计信息
print("=" * 80)
print("PAIMON-CORE 模块中文JavaDoc完成情况统计")
print("=" * 80)
print()
print(f"总文件数: {total_files}")
print(f"有JavaDoc的文件: {files_with_javadoc} ({100*files_with_javadoc/total_files:.1f}%)")
print(f"有中文的文件: {files_with_chinese} ({100*files_with_chinese/total_files:.1f}%)")
print(f"总JavaDoc行数: {total_javadoc_lines}")
print()
print("=" * 80)
print("按包统计")
print("=" * 80)
print()

for package in sorted(package_stats.keys()):
    stats = package_stats[package]
    pct_javadoc = 100 * stats['with_javadoc'] / stats['total'] if stats['total'] > 0 else 0
    pct_chinese = 100 * stats['with_chinese'] / stats['total'] if stats['total'] > 0 else 0
    
    print(f"{package}")
    print(f"  文件数: {stats['total']}")
    print(f"  有JavaDoc: {stats['with_javadoc']} ({pct_javadoc:.1f}%)")
    print(f"  有中文: {stats['with_chinese']} ({pct_chinese:.1f}%)")
    print()

