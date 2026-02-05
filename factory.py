import pandas as pd
import hashlib, json, os, requests, importlib.util, subprocess, time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        # API 配置
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        # SQL 配置
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.vault_path = None
        
        # 性能与计费控制
        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.free_model = "Qwen/Qwen2.5-7B-Instruct" # 免费版

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists(): return masters
        for file_path in self.masters_path.glob("*.py"):
            if file_path.name.startswith("__"): continue
            try:
                name = file_path.stem
                spec = importlib.util.spec_from_file_location(name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                if hasattr(module, 'audit'): masters[name] = module
            except: pass
        return masters

    def fetch_best_signals(self, limit=300):
        """从 SQL 中挑选最优质的 300 条数据，不再无脑处理 1000 条"""
        print(f"📡 正在从中央银行 SQL 筛选前 {limit} 条高价值信号...")
        supabase = create_client(self.supabase_url, self.supabase_key)
        # 逻辑：按时间倒序，或者你可以改为按热度/点赞数排序
        response = supabase.table("raw_signals").select("*").order("created_at", desc=True).limit(limit).execute()
        return response.data

    def call_ai(self, model, sys, usr, temp=0.7):
        if not self.api_key: return "ERROR", "Missing Key"
        payload = {
            "model": model, "messages": [{"role": "system", "content": sys}, {"role": "user", "content": usr}],
            "temperature": temp, "max_tokens": 1024
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=45).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except: return "ERROR", "Timeout"

    def git_push_assets(self):
        """每50条强制押运一次"""
        if not self.vault_path: return
        try:
            cwd = self.vault_path
            subprocess.run(["git", "add", "."], cwd=cwd, check=True)
            status = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd)
            if status.returncode != 0:
                print("📦 [押运中] 50条资产已打包，正在同步至云端仓库...")
                subprocess.run(["git", "commit", "-m", f"🧠 Batch Sync: {datetime.now().strftime('%H:%M:%S')}"], cwd=cwd, check=True)
                subprocess.run(["git", "push"], cwd=cwd, check=True)
                print("✅ [同步成功] 云端已更新。")
        except Exception as e: print(f"⚠️ Git推送失败: {e}")

    def audit_process(self, row):
        """智能漏斗审计逻辑：精华用 V3，普通走免费"""
        content = str(row.get('full_text') or row.get('eventTitle') or '')
        ref_id = hashlib.sha256(content.encode()).hexdigest()
        
        # 1. 评分初筛 (Scout) - 使用免费模型
        scout_sys = "你是一个高价值信息筛选器。给以下内容打分(0-100)。涉及宏观经济、技术转折或深度哲学的内容打高分。只回答数字。"
        _, score_reply = self.call_ai(self.free_model, scout_sys, content[:600], temp=0.1)
        
        try: score = int(''.join(filter(str.isdigit, score_reply)))
        except: score = 50

        results = []
        title = content[:50]
        
        # 🎯 核心逻辑：只有大于 80 分的才请“大师议会”用顶级 V3
        if score > 80:
            def ask_v3(s, u):
                st, r = self.call_ai(self.v3_model, s, u)
                if st == "SUCCESS" and "### Output" in r:
                    return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
                return "深度分析", r
            
            for name, mod in self.masters.items():
                try:
                    t, o = mod.audit(row, ask_v3)
                    if t and o: results.append(json.dumps({"ref_id":ref_id, "master":name, "instruction":f"研判: {title}", "thought":t, "output":o}, ensure_ascii=False))
                except: continue
        
        # 🎯 备选逻辑：50-80 分的，只请一位轮值大师用免费模型处理
        elif score > 50:
            st, r = self.call_ai(self.free_model, "请简要分析此信息价值", content[:500])
            results.append(json.dumps({"ref_id":ref_id, "master":"system", "instruction":f"简评: {title}", "thought":"快速扫描", "output":r}, ensure_ascii=False))

        return results

    def process_and_ship(self, _, vault_path):
        """主入口：忽略本地input，直接SQL取数"""
        self.vault_path = Path(vault_path)
        signals = self.fetch_best_signals(limit=300) # ✅ 只取300条
        
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        batch_size = 50
        print(f"🚀 工厂开工！目标：300 条优质信号，批次大小：{batch_size}")

        for i in range(0, len(signals), batch_size):
            batch_rows = signals[i : i + batch_size]
            
            # 🚀 并发提升：利用 10 个并发窗口加速，确保 30 分钟内跑完
            with ThreadPoolExecutor(max_workers=10) as executor:
                batch_results = list(executor.map(self.audit_process, batch_rows))
            
            # 写入磁盘
            written_count = 0
            with open(output_file, 'a', encoding='utf-8') as f:
                for res_list in batch_results:
                    if res_list:
                        f.write('\n'.join(res_list) + '\n')
                        written_count += 1
            
            print(f"✨ 已处理一批 ({i+batch_size}/300)。本批次产出 {written_count} 条。")
            self.git_push_assets() # ✅ 50条一押运

        print("🏁 任务圆满完成。")
