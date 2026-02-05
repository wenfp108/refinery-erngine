import pandas as pd
import hashlib
import json
import os
import importlib
import glob

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        # 动态加载 masters 文件夹下所有大师插件
        self.masters = self._load_masters(masters_path)

    def _load_masters(self, path):
        masters = {}
        # 扫描 masters/*.py 文件
        for file in glob.glob(os.path.join(path, "*.py")):
            name = os.path.basename(file)[:-3]
            if name == "__init__": continue
            # 动态导入大师逻辑模块
            spec = importlib.util.spec_from_file_location(name, file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            masters[name] = module
        return masters

    def generate_ref_id(self, row_dict):
        """基于原始数据核心特征生成永久哈希 ID"""
        # 针对 Polymarket 数据的核心特征：标题 + 问题内容
        content = str(row_dict.get('eventTitle', '')) + str(row_dict.get('question', ''))
        return hashlib.sha256(content.encode()).hexdigest()

    def process_and_ship(self, input_raw, vault_path):
        """加工并送回中央银行"""
        df = pd.read_parquet(input_raw) # 读取原始归档
        instructions = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            ref_id = self.generate_ref_id(row_dict) # 生成锚点 ID

            # 遍历所有已加载的大师进行并行审计
            for master_name, master_mod in self.masters.items():
                # 获取该大师当前的逻辑版本号
                ver = getattr(master_mod, "VERSION", "1.0")
                thought, output = master_mod.audit(row_dict)

                instructions.append({
                    "ref_id": ref_id,
                    "master": master_name,
                    "version": ver, # 记录是谁、哪个版本的逻辑在评价
                    "instruction": f"请分析事件: {row_dict.get('eventTitle', '未命名')}",
                    "thought": thought,
                    "output": output
                })

        # 送回中央银行：保存习题集资产
        output_file = os.path.join(vault_path, "instructions/teachings.jsonl")
        with open(output_file, 'a', encoding='utf-8') as f:
            for entry in instructions:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')

if __name__ == "__main__":
    factory = UniversalFactory(masters_path="../Masters-Council/masters")
    factory.process_and_ship("temp_raw.parquet", "../Central-Bank")
