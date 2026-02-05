import pandas as pd
import hashlib, json, os, requests, subprocess, time, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client
import importlib.util

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        # 配置
        self.api_key = os.environ.get("SILICON_FLOW_KEY") 
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.vault_path = None
        self.memory = {} # 🧠 认知记忆库

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists(): 
            self.masters_path.mkdir(exist_ok=True)
            return masters
        for file_path in self.masters_path.glob("*.py"):
            if file_path.name.startswith("__"): continue
            try:
                name = file_path.stem
                spec = importlib.util.spec_from_file_location(name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                if hasattr(module, 'audit'): masters[name] = module
                print(f"✅ 已加载 Master: {name}")
            except Exception as e:
                print(f"⚠️ Master {file_path.name} 加载失败: {e}")
        return masters

    def build_memory(self, output_file):
        """🧠 扫描今日已产出数据，构建短期记忆索引"""
        if not output_file.exists(): return
        print(f"🧐 正在同步今日记忆: {output_file.name}...")
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        tid = data.get('topic_id')
                        m = data.get('master')
                        if tid and m:
                            if tid not in self.memory: self.memory[tid] = {}
                            self.memory[tid][m] = data.get('output', "")
                    except: continue
            print(f"✅ 记忆构建完成，涉及 {len(self.memory)} 个主题")
        except Exception as e:
            print(f"⚠️ 记忆构建中断: {e}")

    def git_push_assets(self):
        if not self.vault_path: return
        cwd = self.vault_path
        print("🔄 [Git] 正在执行追加同步 (Rebase Mode)...")
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd, check=False)
        subprocess.run(["git", "add", "."], cwd=cwd, check=False)
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd).returncode != 0:
            msg = f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"
            subprocess.run(["git", "commit", "-m", msg], cwd=cwd, check=False)
            subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd, check=False)
            res = subprocess.run(["git", "push", "origin", "main"], cwd=cwd, check=False)
            if res.returncode == 0: print("✅ [Git] 认知资产已安全追加")
        else:
            print("💤 [Git] 无新变化")

    def call_ai(self, model, sys_prompt, usr_prompt):
        if not self.api_key: return "ERROR", "No Key"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": model, 
            "messages": [{"role": "system", "content": sys_prompt}, {"role": "user", "content": usr_prompt}],
            "temperature": 0.7, "max_tokens": 1500
        }
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=60).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except Exception as e: return "ERROR", str(e)

    def audit_process(self, row, processed_ids):
        # 1. 识别主题唯一 ID（跨时间线追踪的核心）
        topic_id = row.get('url') or row.get('slug') or row.get('repo_name') or "unknown"
        source = row.get('signal_type', 'unknown').lower()
        
        # 2. 构建输入
        parts = [f"【Source: {source.upper()}】"]
        if source == 'github':
            parts.append(f"项目: {row.get('repo_name')} | Stars: {row.get('stars')} | 描述: {row.get('full_text')}")
        elif source == 'polymarket':
            raw = row.get('raw_json')
            if isinstance(raw, str): 
                try: raw = json.loads(raw)
                except: raw = {}
            parts.append(f"预测: {row.get('title')} | 价格: {row.get('prices') or raw.get('outcome_prices')} | 流动性: ${raw.get('liquidity')}")
        else: # Twitter/Reddit
            parts.append(f"用户: {row.get('user_name') or row.get('subreddit')} | 内容: {row.get('full_text') or row.get('title')}")
        
        input_content = "\n".join(parts)
        ref_id = hashlib.sha256(input_content.encode()).hexdigest()
        
        # 如果哈希完全一样，说明数据没变，跳过以省钱
        if ref_id in processed_ids: return []

        results = []
        def ask_v3(s, u):
            st, r = self.call_ai(self.v3_model, s, u)
            if st == "SUCCESS" and "### Output" in r:
                return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
            return "Analysis", r

        # 3. 大师会审 + 漂移检测
        for name, mod in self.masters.items():
            # 🔍 检索历史记忆
            prev_opinion = self.memory.get(topic_id, {}).get(name)
            drift_context = ""
            if prev_opinion:
                drift_context = f"\n\n[历史记忆]：你此前对该主题的观点是：'{prev_opinion}'。若当前数据触发了你的观点转向，请在 Thought 中详述逻辑变化，并在 Output 开头标记 [DRIFT_DETECTED]。"

            try:
                if hasattr(mod, 'audit'):
                    # 注入历史上下文
                    row['_drift_context'] = drift_context
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "topic_id": topic_id,
                            "ref_id": ref_id,
                            "source": source,
                            "master": name,
                            "drift": "[DRIFT_DETECTED]" in o,
                            "thought": t,
                            "output": o,
                            "prev_opinion": prev_opinion
                        }, ensure_ascii=False))
                        print(f"💡 [{name}] {'🔄 漂移检测' if '[DRIFT_DETECTED]' in o else '洞察生成'}: {topic_id[:30]}...")
            except: continue
        return results

    def process_and_ship(self, vault_path="vault"):
        self.vault_path = Path(vault_path)
        (self.vault_path / "instructions").mkdir(parents=True, exist_ok=True)
        
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        
        # 1. 初始化 Git 和 记忆
        self.configure_git()
        self.build_memory(output_file)
        
        processed_ids = set()
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: processed_ids.add(json.loads(line).get('ref_id'))
                    except: pass

        # 2. 抓取新鲜精锐信号 (此处略去 fetch_elite_signals 逻辑，保持您原有的即可)
        from refinery import create_client as supabase_client
        supabase = supabase_client(self.supabase_url, self.supabase_key)
        # 这里模拟抓取逻辑，建议保留您原有的 fetch_elite_signals 函数
        signals = self.fetch_elite_signals() 

        if not signals: return

        print(f"🚀 启动认知审计流水线: {len(signals)} 个信号待处理...")

        # 3. 20 线程并发加工
        batch_size = 50
        for i in range(0, len(signals), batch_size):
            chunk = signals[i : i + batch_size]
            with ThreadPoolExecutor(max_workers=20) as executor:
                res = list(executor.map(lambda r: self.audit_process(r, processed_ids), chunk))
            
            added = []
            for r_list in res:
                if r_list: added.extend(r_list)
            
            if added:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write('\n'.join(added) + '\n')
                self.git_push_assets() # 实时追加

    def configure_git(self):
        subprocess.run(["git", "config", "--global", "user.email", "bot@factory.com"], check=False)
        subprocess.run(["git", "config", "--global", "user.name", "Cognitive Bot"], check=False)

    def fetch_elite_signals(self):
        # 请保留您之前 factory.py 中完整的 fetch_elite_signals 逻辑
        # 此处为示意，实际运行时请将原有的 fetch 代码粘贴于此
        pass

if __name__ == "__main__":
    factory = UniversalFactory()
    factory.process_and_ship()
