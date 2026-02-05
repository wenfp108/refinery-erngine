import pandas as pd
import hashlib, json, os, requests, subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.vault_path = None
        
        # 打印 Key 的状态（只显示前几位，防止泄露）
        if self.api_key:
            print(f"🔑 API Key 检测: 已加载 (前缀: {self.api_key[:4]}...)")
        else:
            print("❌ 严重警告: 未检测到 SILICON_FLOW_KEY！API 将无法工作。")

        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.free_model = "Qwen/Qwen2.5-7B-Instruct"

    def _load_masters(self):
        # ... (保持原样，省略以节省篇幅，加载逻辑没问题) ...
        import importlib.util
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

    def configure_git(self):
        if not self.vault_path: return
        try:
            cwd = self.vault_path
            subprocess.run(["git", "config", "user.email", "bot@factory.com"], cwd=cwd, check=False)
            subprocess.run(["git", "config", "user.name", "Cognitive Bot"], cwd=cwd, check=False)
        except: pass

    def fetch_best_signals(self, limit=300):
        print(f"📡 尝试 SQL 获取...")
        supabase = create_client(self.supabase_url, self.supabase_key)
        return supabase.table("raw_signals").select("*").order("created_at", desc=True).limit(limit).execute().data

    def call_ai(self, model, sys, usr):
        if not self.api_key: return "ERROR", "No API Key"
        payload = {
            "model": model, "messages": [{"role": "system", "content": sys}, {"role": "user", "content": usr}],
            "temperature": 0.7, "max_tokens": 1024
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=45)
            if res.status_code == 200:
                return "SUCCESS", res.json()['choices'][0]['message']['content']
            else:
                # 🔥 关键：打印出具体的 API 报错信息
                print(f"❌ API 报错 [{res.status_code}]: {res.text[:100]}")
                return "ERROR", f"API Fail: {res.status_code}"
        except Exception as e:
            print(f"❌ 网络错误: {str(e)}")
            return "ERROR", "Timeout/NetError"

    def git_push_assets(self):
        if not self.vault_path: return
        try:
            cwd = self.vault_path
            subprocess.run(["git", "add", "."], cwd=cwd, check=True)
            if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd).returncode != 0:
                print("📦 [Git] 正在推送...")
                subprocess.run(["git", "commit", "-m", f"🧠 Batch: {datetime.now().strftime('%H:%M:%S')}"], cwd=cwd, check=True)
                subprocess.run(["git", "push"], cwd=cwd, check=True)
        except Exception as e: print(f"⚠️ Git Warning: {e}")

    def audit_process(self, row):
        # 🔥 1. 强力内容提取 (根据你的调试日志优化)
        # 将 question 和 full_text 拼起来，防止遗漏信息
        parts = []
        if row.get('title'): parts.append(str(row.get('title')))
        if row.get('question'): parts.append(str(row.get('question')))
        if row.get('full_text'): parts.append(str(row.get('full_text')))
        
        content = "\n".join(parts)
        if len(content) < 5: return [] # 真的没内容才跳过

        ref_id = hashlib.sha256(content.encode()).hexdigest()
        title = content[:50].replace('\n', ' ')

        # 🔥 2. 免费打分
        _, score_reply = self.call_ai(self.free_model, "给此信息价值打分(0-100)。只回数字。", content[:800])
        try: score = int(''.join(filter(str.isdigit, score_reply)))
        except: score = 0 # 如果 API 挂了，默认为 0

        results = []

        # 🎯 3. 分流逻辑
        # 情况 A: 顶级信号 (V3)
        if score > 80:
            def ask_v3(s, u):
                st, r = self.call_ai(self.v3_model, s, u)
                if st == "SUCCESS" and "### Output" in r:
                    return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
                return "分析", r
            for name, mod in self.masters.items():
                try:
                    t, o = mod.audit(row, ask_v3)
                    if t and o: results.append(json.dumps({"ref_id":ref_id, "type":"V3_MASTER", "master":name, "input":title, "thought":t, "output":o}, ensure_ascii=False))
                except: continue

        # 情况 B: 普通信号 (免费模型) - 只要 API 活着就跑
        elif score > 0: 
            st, r = self.call_ai(self.free_model, "一句话总结核心价值", content[:800])
            if st == "SUCCESS":
                results.append(json.dumps({"ref_id":ref_id, "type":"FREE_SCAN", "master":"system", "input":title, "output":r}, ensure_ascii=False))
        
        # 情况 C: API 全挂了 (保底措施) - 存原始数据，证明流程通了
        else:
            # 这是一个“死信”，虽然没 AI 分析，但至少让你知道数据流到了这里
            results.append(json.dumps({"ref_id":ref_id, "type":"RAW_BACKUP", "master":"backup", "input":title, "error": "API_FAILED_OR_TRASH"}, ensure_ascii=False))

        return results

    def process_and_ship(self, input_raw, vault_path):
        self.vault_path = Path(vault_path)
        self.configure_git()
        
        signals = []
        try:
            signals = self.fetch_best_signals(limit=300)
            print(f"✅ SQL 获取 {len(signals)} 条。")
        except:
            print(f"🔄 降级读取本地文件...")
            try:
                # 兼容性处理：如果文件里混了非 dict 格式
                df = pd.read_parquet(input_raw)
                signals = df.head(300).to_dict('records')
            except: 
                print("❌ 无法读取数据源"); return

        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # 📉 降速：并发降到 5，防止瞬间把免费 API 打挂
        print(f"🚀 工厂开工！处理 {len(signals)} 条数据 (并发: 5)...")
        
        for i in range(0, len(signals), 50):
            batch = signals[i : i + 50]
            with ThreadPoolExecutor(max_workers=5) as executor:
                res = list(executor.map(self.audit_process, batch))
            
            added = 0
            with open(output_file, 'a', encoding='utf-8') as f:
                for r_list in res:
                    if r_list:
                        f.write('\n'.join(r_list) + '\n')
                        added += 1
            
            print(f"✨ 进度 {i+len(batch)}/{len(signals)} | 本批入库: {added} 条")
            if added > 0: self.git_push_assets()

        print("🏁 任务结束。")
