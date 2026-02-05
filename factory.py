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
        self.api_key = os.environ.get("SILICON_FLOW_KEY") 
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.vault_path = None
        self.memory = {} # 🧠 认知记忆库：存储 {topic_id: {master_name: last_output}}

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists(): 
            try: self.masters_path.mkdir(exist_ok=True)
            except: pass
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
        """🧠 扫描今日已产出数据，构建短期记忆，用于检测‘改口’"""
        if not output_file.exists(): return
        print(f"🧐 正在加载今日历史记忆...")
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        tid = data.get('topic_id')
                        m = data.get('master')
                        if tid and m:
                            if tid not in self.memory: self.memory[tid] = {}
                            # 记录该主题下该大师的最后一次观点
                            self.memory[tid][m] = data.get('output', "")
                    except: continue
            print(f"✅ 记忆构建完成，涉及 {len(self.memory)} 个历史话题")
        except Exception as e:
            print(f"⚠️ 记忆读取失败: {e}")

    def fetch_elite_signals(self):
        """🌟 核心逻辑：精锐席位筛选 (完全保留你的权重算法)"""
        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            print("💎 启动精锐筛选 (目标: ~220 条)...")

            # 1. GitHub & Paper
            rare_raw = supabase.table("raw_signals").select("*").or_("signal_type.eq.github,signal_type.eq.paper").order("created_at", desc=True).limit(50).execute().data or []
            unique_rare = {}
            for r in rare_raw:
                k = r.get('repo_name') or r.get('title')
                if k and k not in unique_rare: unique_rare[k] = r
            rare_picks = list(unique_rare.values())

            # 2. Twitter (含 VIP 权重)
            tw_raw = supabase.table("raw_signals").select("*").eq("signal_type", "twitter").order("created_at", desc=True).limit(500).execute().data or []
            vip_list = ['Karpathy', 'Musk', 'Vitalik', 'LeCun', 'Dalio', 'Naval', 'Sama', 'PaulG']
            def score_twitter(row):
                rt, bm, like = row.get('retweets',0), row.get('bookmarks',0), row.get('likes',0)
                user = str(row.get('user_name', '')).lower()
                score = (rt * 5) + (bm * 10) + like
                if any(v.lower() in user for v in vip_list):
                    score += 10000 if (rt > 10 or like > 50) else 500
                return score
            for r in tw_raw: r['_rank'] = score_twitter(r)
            tw_picks = sorted(tw_raw, key=lambda x:x['_rank'], reverse=True)[:60]

            # 3. Reddit (含 Vibe 权重)
            rd_raw = supabase.table("raw_signals").select("*").eq("signal_type", "reddit").order("created_at", desc=True).limit(500).execute().data or []
            unique_rd = {r.get('url'): r for r in rd_raw if r.get('url')}
            def score_reddit(row): return (row.get('score') or 0) * (1 + abs(float(row.get('vibe') or 0)))
            rd_picks = sorted(unique_rd.values(), key=score_reddit, reverse=True)[:30]

            # 4. Polymarket
            poly_raw = supabase.table("raw_signals").select("*").eq("signal_type", "polymarket").order("created_at", desc=True).limit(800).execute().data or []
            unique_poly = {}
            for p in poly_raw:
                raw = p.get('raw_json')
                if isinstance(raw, str):
                    try: raw = json.loads(raw)
                    except: raw = {}
                p['_parsed'] = raw
                slug = p.get('slug') or raw.get('slug')
                if slug:
                    curr_liq = float(p.get('liquidity') or 0)
                    if slug not in unique_poly or curr_liq > float(unique_poly[slug].get('liquidity',0)):
                        unique_poly[slug] = p
            def score_poly(row):
                raw = row['_parsed']
                liq = float(row.get('liquidity') or 0)
                if 'TAIL_RISK' in raw.get('strategy_tags', []): return 10000000 + liq
                if any(x in str(row.get('category','')).upper() for x in ['ECONOMY', 'TECH']): return 5000000 + liq
                return 1000000 + liq
            poly_picks = sorted(unique_poly.values(), key=score_poly, reverse=True)[:80]

            return rare_picks + tw_picks + rd_picks + poly_picks
        except Exception as e:
            print(f"⚠️ 筛选异常: {e}"); return []

    def call_ai(self, model, sys_prompt, usr_prompt):
        enhanced_sys = sys_prompt + "\n[重要]：你现在是首席审计官。不要机械总结，要展示鲜明的立场。若信号变动，请分析其背后的因果漂移。"
        payload = {"model": model, "messages": [{"role": "system", "content": enhanced_sys}, {"role": "user", "content": usr_prompt}], "temperature": 0.7}
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=60).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except: return "ERROR", "API_FAILED"

    def git_push_assets(self):
        if not self.vault_path: return
        cwd = self.vault_path
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd, check=False)
        subprocess.run(["git", "add", "."], cwd=cwd, check=False)
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd).returncode != 0:
            msg = f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"
            subprocess.run(["git", "commit", "-m", msg], cwd=cwd, check=False)
            subprocess.run(["git", "push", "origin", "main"], cwd=cwd, check=False)

    def audit_process(self, row, processed_ids):
        # 1. 唯一主题 ID (追踪的核心)
        topic_id = row.get('url') or row.get('slug') or row.get('repo_name') or "unknown"
        source = row.get('signal_type', 'unknown').lower()
        
        # 2. 构造格式化内容 (严格对齐你的字段)
        parts = [f"【Source: {source.upper()}】"]
        if source == 'github':
            parts.append(f"项目: {row.get('repo_name')} | Stars: {row.get('stars')} | Topics: {row.get('topics')}")
            parts.append(f"描述: {row.get('full_text') or '新项目发布'} | Link: {row.get('url')}")
        elif source == 'paper':
            parts.append(f"论文: {row.get('title')} | 摘要: {row.get('full_text')}")
        elif source in ['twitter', 'reddit']:
            parts.append(f"用户: {row.get('user_name') or row.get('subreddit')} | 内容: {row.get('full_text') or row.get('title')}")
        else: # Polymarket
            raw = row.get('_parsed') or row.get('raw_json') or {}
            parts.append(f"预测: {row.get('title')} | 价格: {row.get('prices') or raw.get('outcome_prices')}")
            parts.append(f"流动性: ${raw.get('liquidity')} | 标签: {raw.get('strategy_tags')}")

        content = "\n".join(parts)
        ref_id = hashlib.sha256(content.encode()).hexdigest()
        
        if ref_id in processed_ids: return []

        results = []
        def ask_v3(s, u):
            st, r = self.call_ai(self.v3_model, s, u)
            if st == "SUCCESS" and "### Output" in r:
                return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
            return "Analysis", r

        # 3. 大师审计 + 漂移检测
        for name, mod in self.masters.items():
            # 🔍 获取记忆
            prev_opinion = self.memory.get(topic_id, {}).get(name)
            drift_prompt = ""
            if prev_opinion:
                drift_prompt = f"\n\n[历史记忆]：你此前对该主题的结论是：'{prev_opinion}'。若当前数据触发了你的‘改口’，请在 Thought 中详述转向逻辑，并在 Output 开头标记 [DRIFT_DETECTED]。"

            try:
                if hasattr(mod, 'audit'):
                    row['_drift_context'] = drift_prompt
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "ref_id": ref_id, "topic_id": topic_id, "master": name,
                            "drift": "[DRIFT_DETECTED]" in o,
                            "input": content[:200], "thought": t, "output": o,
                            "prev_opinion": prev_opinion
                        }, ensure_ascii=False))
                        print(f"💡 [{name}] {'🔄 改口监测' if '[DRIFT_DETECTED]' in o else '洞察生成'}: {topic_id[:30]}...")
            except: continue
        return results

    def process_and_ship(self, vault_path="vault"):
        self.vault_path = Path(vault_path)
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        
        # 1. 加载记忆
        self.build_memory(output_file)
        
        processed_ids = set()
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: processed_ids.add(json.loads(line).get('ref_id'))
                    except: pass

        # 2. 进货
        signals = self.fetch_elite_signals()
        if not signals: return

        # 3. 20线程加工
        batch_size = 50
        for i in range(0, len(signals), batch_size):
            chunk = signals[i : i + batch_size]
            with ThreadPoolExecutor(max_workers=20) as executor:
                res = list(executor.map(lambda r: self.audit_process(r, processed_ids), chunk))
            
            added = [r for r_list in res for r in r_list]
            if added:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write('\n'.join(added) + '\n')
                self.git_push_assets()

if __name__ == "__main__":
    factory = UniversalFactory()
    factory.process_and_ship()
