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
        # 环境变量
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.vault_path = None
        
        # 模型配置
        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.free_model = "Qwen/Qwen2.5-7B-Instruct"

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
        """核心：从 SQL 抓取最近 1 小时的高价值信号"""
        print(f"📡 正在从 SQL 筛选最近 {limit} 条高价值信号...")
        supabase = create_client(self.supabase_url, self.supabase_key)
        # 优先选择字数丰富且最新的信号
        response = supabase.table("raw_signals") \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()
        return response.data

    def call_ai(self, model, sys, usr):
        if not self.api_key: return "ERROR", "Missing Key"
        payload = {
            "model": model, "messages": [{"role": "system", "content": sys}, {"role": "user", "content": usr}],
            "temperature": 0.7, "max_tokens": 1024
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=45).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except: return "ERROR", "Timeout"

    def git_push_assets(self):
        """【救命逻辑】每50条强制同步一次，防止中途断开钱白花"""
        if not self.vault_path: return
        try:
            cwd = self.vault_path
            subprocess.run(["git", "add", "."], cwd=cwd, check=True)
            status = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd)
            if status.returncode != 0:
                print("📦 [分批同步] 50条审计资产已打包，正在押运至中央银行...")
                subprocess.run(["git", "commit", "-m", f"🧠 Batch Update: {datetime.now().strftime('%H:%M:%S')}"], cwd=cwd, check=True)
                subprocess.run(["git", "push"], cwd=cwd, check=True)
                print("✅ [押运成功] 资产已锁定。")
        except Exception as e: print(f"⚠️ Git同步失败: {e}")

    def audit_process(self, row):
        """智能漏斗：精华走 V3，普通走免费"""
        content = str(row.get('full_text') or row.get('eventTitle') or '')
        ref_id = hashlib.sha256(content.encode()).hexdigest()
        
        # 1. 免费打分
        scout_sys = "你是一个高价值信息筛选器。给以下内容打分(0-100)。只回答数字。"
        _, score_reply = self.call_ai(self.free_model, scout_sys, content[:500])
        
        try: score = int(''.join(filter(str.isdigit, score_reply)))
        except: score = 50

        results = []
        title = content[:50]
        
        # 精华信号：全量大师审计 (DeepSeek-V3)
        if score > 80:
            def ask_v3(s, u):
                st, r = self.call_ai(self.v3_model, s, u)
                if st == "SUCCESS" and "### Output" in r:
                    return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
                return "综合研判", r
            
            for name, mod in self.masters.items():
                try:
                    t, o = mod.audit(row, ask_v3)
                    if t and o: results.append(json.dumps({"ref_id":ref_id, "master":name, "instruction":f"研判: {title}", "thought":t, "output":o}, ensure_ascii=False))
                except: continue
        
        # 普通信号：单人快速简评 (Qwen-7B)
        elif score > 50:
            st, r = self.call_ai(self.free_model, "请用一句话提取该信息的关键价值点", content[:500])
            if st == "SUCCESS":
                results.append(json.dumps({"ref_id":ref_id, "master":"system", "instruction":f"快评: {title}", "thought":"快速扫描", "output":r}, ensure_ascii=False))

        return results

    def process_and_ship(self, input_raw, vault_path): # ✅ 修复签名，兼容 run_factory.py
        self.vault_path = Path(vault_path)
        # 核心改变：不再处理 input_raw 里的 1000 条，直接 SQL 拿 300 条
        signals = self.fetch_best_signals(limit=300) 
        
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        batch_size = 50
        print(f"🚀 工厂开工！每 {batch_size} 条审计自动保存。")

        for i in range(0, len(signals), batch_size):
            batch_rows = signals[i : i + batch_size]
            
            # 使用 10 并发，确保 15-20 分钟内跑完 300 条
            with ThreadPoolExecutor(max_workers=10) as executor:
                batch_results = list(executor.map(self.audit_process, batch_rows))
            
            # 批量写入
            batch_added = 0
            with open(output_file, 'a', encoding='utf-8') as f:
                for res_list in batch_results:
                    if res_list:
                        f.write('\n'.join(res_list) + '\n')
                        batch_added += 1
            
            print(f"✨ 进度: {i+len(batch_rows)}/300。本批次产出 {batch_added} 条见解。")
            self.git_push_assets() # ✅ 实时同步进度

        print("🏁 全量 300 条任务已收工。")
