import os
import re

def check_file_javadoc(file_path):
    """检查单个文件的JavaDoc质量"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        lines = content.split('\n')
        
        # 找到第一个类定义
        class_line = -1
        for i, line in enumerate(lines):
            if re.match(r'\s*(public\s+)?(abstract\s+)?(final\s+)?(class|interface|enum)\s+\w+', line):
                class_line = i
                break
        
        if class_line <= 0:
            return None
        
        # 找JavaDoc块
        javadoc_start = -1
        javadoc_end = -1
        
        for i in range(class_line-1, -1, -1):
            if '/**' in lines[i]:
                javadoc_start = i
                break
        
        if javadoc_start < 0:
            return {'has_javadoc': False, 'lines': 0}
        
        for i in range(javadoc_start, class_line):
            if '*/' in lines[i]:
                javadoc_end = i
                break
        
        if javadoc_end < 0:
            return {'has_javadoc': False, 'lines': 0}
        
        javadoc_text = '\n'.join(lines[javadoc_start:javadoc_end+1])
        javadoc_lines = javadoc_end - javadoc_start + 1
        
        # 检查质量指标
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', javadoc_text))
        has_description = bool(re.search(r'/\*\*\s*\n\s*\*\s+[\w\u4e00-\u9fff]', javadoc_text))
        has_tags = bool(re.search(r'@\w+', javadoc_text))
        
        return {
            'has_javadoc': True,
            'lines': javadoc_lines,
            'has_chinese': has_chinese,
            'has_description': has_description,
            'has_tags': has_tags,
            'javadoc': javadoc_text[:200]
        }
    except:
        return None

# 检查一些关键文件
test_files = [
    r"D:\a_git\paimon\paimon-core\src\main\java\org\apache\paimon\AbstractFileStore.java",
    r"D:\a_git\paimon\paimon-core\src\main\java\org\apache\paimon\mergetree\compact\MergeTreeCompactManager.java",
    r"D:\a_git\paimon\paimon-core\src\main\java\org\apache\paimon\deletionvectors\DeletionVector.java",
    r"D:\a_git\paimon\paimon-core\src\main\java\org\apache\paimon\operation\FileStoreCommit.java",
    r"D:\a_git\paimon\paimon-core\src\main\java\org\apache\paimon\table\source\TableRead.java",
]

print("关键文件JavaDoc质量检查")
print("=" * 80)

for file_path in test_files:
    if os.path.exists(file_path):
        info = check_file_javadoc(file_path)
        file_name = os.path.basename(file_path)
        
        if info:
            print(f"\n{file_name}")
            print(f"  是否有JavaDoc: {info.get('has_javadoc', False)}")
            print(f"  行数: {info.get('lines', 0)}")
            print(f"  是否有中文: {info.get('has_chinese', False)}")
            print(f"  是否有描述: {info.get('has_description', False)}")
            print(f"  是否有标签: {info.get('has_tags', False)}")
