import pandas as pd
import hashlib, json, os, requests, subprocess, time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        # API 与 数据库配置
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.vault_path = None
        
        # 模型设定
        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.free_model = "Qwen/Qwen2.5-7B-Instruct"
        
        # 🔥 高价值关键词保送名单
        self.priority_keywords = [
            'Iran', 'Trump', 'Fed', 'Powell', 'War', 'Strike', 'Nominate', 
            'Solana', 'BTC', 'NVIDIA', 'LLM', 'Paper', 'GitHub', 'Exploit'
        ]

    def _load_masters(self):
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
        """确保 GitHub Actions 身份合法，防止 Exit 128"""
        if not self.vault_path: return
        cwd = self.vault_path
        subprocess.run(["git", "config", "--global", "user.email", "bot@factory.com"], check=False)
        subprocess.run(["git", "config", "--global", "user.name", "Cognitive Bot"], check=False)

    def fetch_elite_signals(self, total_limit=300):
        """🌟 核心逻辑：稀缺源优先 + 质量过滤"""
        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            print("💎 正在从 SQL 执行‘精英数据’筛选...")

            # 1. 捞干 GitHub 和 Paper (全量收割)
            rare_signals = supabase.table("raw_signals") \
                .select("*") \
                .or("signal_type.eq.github,signal_type.eq.paper") \
                .order("created_at", desc=True) \
                .limit(60).execute().data or []

            # 2. 精选 Twitter/Reddit (长文 + 高赞)
            social_signals = supabase.table("raw_signals") \
                .select("*") \
                .or("signal_type.eq.twitter,signal_type.eq.reddit") \
                .gt("likes", 5) \
                .order("likes", desc=True) \
                .limit(100).execute().data or []

            # 3. Polymarket 兜底 (只看大资金池)
            remain = total_limit - len(rare_signals) - len(social_signals)
            poly_signals = supabase.table("raw_signals") \
                .select("*") \
                .eq("signal_type", "polymarket") \
                .gt("liquidity", 5000) \
                .order("liquidity", desc=True) \
                .limit(max(0, remain)).execute().data or []

            all_data = rare_signals + social_signals + poly_signals
            print(f"📊 构成比例：GitHub/Paper({len(rare_signals)}) | Social({len(social_signals)}) | Poly({len(poly_signals)})")
            return all_data
        except Exception as e:
            print(f"⚠️ SQL 筛选失败: {e}，将尝试全量兜底...")
            return []

    def call_ai(self, model, sys, usr):
        if not self.api_key: return "ERROR", "No Key"
        # 🧠 注入‘逻辑接骨’指令
        enhanced_sys = sys + "\n[重要]：若输入信号断档，请基于你的知识库推演缺失逻辑。在 Thought 中展示接骨过程。"
        payload = {
            "model": model, "messages": [{"role": "system", "content": enhanced_sys}, {"role": "user", "content": usr}],
            "temperature": 0.7, "max_tokens": 1500
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=60).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except: return "ERROR", "Timeout"

    def git_push_assets(self):
        if not self.vault_path: return
        cwd = self.vault_path
        subprocess.run(["git", "add", "."], cwd=cwd)
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd).returncode != 0:
            subprocess.run(["git", "commit", "-m", f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"], cwd=cwd)
            subprocess.run(["git", "push"], cwd=cwd)

    def audit_process(self, row, processed_ids):
        # 🔥 强化版多源内容拼接
        source = row.get('signal_type', 'unknown').lower()
        parts = [f"【Source: {source.upper()}】"]
        
        if source == 'github':
            parts.append(f"项目: {row.get('repo_name')} | Stars: {row.get('stars')} | Topics: {row.get('topics')}")
            parts.append(f"描述: {row.get('full_text') or '新项目发布'}")
        elif source == 'paper':
            parts.append(f"论文: {row.get('title')} | 期刊: {row.get('journal')}")
            parts.append(f"摘要: {row.get('full_text')}")
        elif source in ['twitter', 'reddit']:
            parts.append(f"用户: @{row.get('screen_name')} | 内容: {row.get('full_text')}")
        else: # Polymarket
            parts.append(f"预测: {row.get('question')} | 流动性: {row.get('liquidity')} | 价格: {row.get('prices')}")

        content = "\n".join(parts)
        ref_id = hashlib.sha256(content.encode()).hexdigest()
        
        # 智能去重
        if ref_id in processed_ids or len(content) < 15: return []

        # 1. 评分分流
        scout_sys = "你是一个高价值信息初筛官。打分(0-100)。只要涉及宏观博弈、技术转折或大资金动态就给高分。只回数字。"
        _, score_reply = self.call_ai(self.free_model, scout_sys, content[:1000])
        try: score = int(''.join(filter(str.isdigit, score_reply)))
        except: score = 50
        
        # 关键词保送
        if any(kw.lower() in content.lower() for kw in self.priority_keywords):
            score = max(score, 90)

        results = []
        # 2. 大师审计 (V3)
        if score >= 85:
            def ask_v3(s, u):
                st, r = self.call_ai(self.v3_model, s, u)
                if st == "SUCCESS" and "### Output" in r:
                    return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
                return "逻辑推演", r
            
            for name, mod in self.masters.items():
                try:
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "ref_id": ref_id, "type": "V3_MASTER", "source": source,
                            "master": name, "input": content[:150].replace('\n',' '), "thought": t, "output": o
                        }, ensure_ascii=False))
                except: continue
        return results

    def process_and_ship(self, input_raw, vault_path):
        self.vault_path = Path(vault_path)
        self.configure_git()
        
        # 加载去重 ID
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        processed_ids = set()
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: processed_ids.add(json.loads(line).get('ref_id'))
                    except: pass

        # 抓取精英信号
        signals = self.fetch_elite_signals(total_limit=300)
        if not signals:
            print("🔄 SQL 抓取为空，尝试读取本地缓存文件...")
            df = pd.read_parquet(input_raw)
            signals = df.head(300).to_dict('records')

        print(f"🚀 工厂开工！目标：300 条精华审计。")

        batch_size = 50
        for i in range(0, len(signals), batch_size):
            chunk = signals[i : i + batch_size]
            with ThreadPoolExecutor(max_workers=10) as executor:
                res = list(executor.map(lambda r: self.audit_process(r, processed_ids), chunk))
            
            added = []
            for r_list in res:
                if r_list: added.extend(r_list)
            
            if added:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write('\n'.join(added) + '\n')
                    f.flush()
                print(f"✨ 批次 {i//50 + 1} 完成 | 新增 {len(added)} 条大师级资产。")
                self.git_push_assets() # ✅ 50条一押运

        print("🏁 全量多源精英收割任务圆满完成。")
