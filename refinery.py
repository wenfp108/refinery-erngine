import os, json, base64, requests, importlib.util, sys
from datetime import datetime, timedelta, timezone
import pandas as pd
from supabase import create_client
from github import Github

# === 🛡️ 1. 核心配置 ===
PRIVATE_BANK_ID = "wenfp108/Central-Bank" 
GITHUB_TOKEN = os.environ.get("GH_PAT") 
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("❌ [审计异常] 环境变量缺失。")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
gh_client = Github(GITHUB_TOKEN)
private_repo = gh_client.get_repo(PRIVATE_BANK_ID)

# === 🧩 2. 插件发现系统 (保持原样) ===
def get_all_processors():
    procs = {}
    proc_dir = "./processors"
    if not os.path.exists(proc_dir): return procs
    for filename in os.listdir(proc_dir):
        if filename.endswith(".py") and not filename.startswith("__"):
            name = filename[:-3]
            try:
                spec = importlib.util.spec_from_file_location(f"mod_{name}", os.path.join(proc_dir, filename))
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                procs[name] = {
                    "module": mod,
                    "table_name": getattr(mod, "TABLE_NAME", f"{name}_logs"),
                    "archive_folder": getattr(mod, "ARCHIVE_FOLDER", name)
                }
            except Exception as e: print(f"⚠️ 插件 {name} 加载失败: {e}")
    return procs

# === 🔥 3. 战报工厂：Markdown 垂直堆叠引擎 ===

def generate_hot_reports(processors_config):
    """
    不利用 AI，直接基于 Alpha-Signal-V1 算法生成 MD 战报。
    """
    print("\n🔥 [情报对冲] 正在生成全维度 Markdown 时报...")
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    date_tag = bj_now.strftime('%Y%m%d')
    hour_tag = bj_now.strftime('%H')
    
    # 建立战报页眉
    md_report = f"# 🚀 Architect's Alpha 情报审计 ({date_tag} {hour_tag}:00)\n\n"
    md_report += "> **防御状态**：承认运气差 / 严禁杠杆 / 专注数据动能\n\n"

    # 遍历所有插件，按源堆叠
    for source_name, config in processors_config.items():
        if hasattr(config["module"], "get_hot_items"):
            try:
                # 获取该源的分板块审计矩阵
                sector_matrix = config["module"].get_hot_items(supabase, config["table_name"])
                if not sector_matrix: continue

                md_report += f"## 📡 来源：{source_name.upper()}\n"
                
                for sector, items in sector_matrix.items():
                    md_report += f"### 🏷️ 板块：{sector}\n"
                    md_report += "| 信号强度 | 源头 | 关键情报摘要 | 链接 |\n| :--- | :--- | :--- | :--- |\n"
                    
                    for item in items:
                        # 摘要处理：对齐北京时间交叉对比
                        score = int(item.get('score', 0))
                        source = item.get('user_name', 'Unknown')
                        text = item.get('full_text', '').replace('\n', ' ')[:85] + "..."
                        url = item.get('tweet_url', '#')
                        md_report += f"| **{score:,}** | {source} | {text} | [查看]({url}) |\n"
                    md_report += "\n"
            except Exception as e:
                print(f"⚠️ {source_name} 战报渲染失败: {e}")

    # 双路同步：latest_brief.md (秒开) + 历史归档
    latest_path = "reports/latest_brief.md"
    archive_path = f"reports/hourly/{date_tag}_{hour_tag}.md"
    
    for path in [latest_path, archive_path]:
        try:
            try:
                old = private_repo.get_contents(path)
                private_repo.update_file(old.path, f"📊 Update Brief: {hour_tag}h", md_report, old.sha)
            except:
                private_repo.create_file(path, f"🚀 New Brief: {hour_tag}h", md_report)
        except Exception as e: print(f"❌ 写入 {path} 失败: {e}")

# === 🚜 4. 滚动收割：含 7 天报表清理 ===

def perform_grand_harvest(processors_config):
    """压制旧资产并清理 7 天前旧报表"""
    cutoff_date = (datetime.now() - timedelta(days=7))
    cutoff_str = cutoff_date.isoformat()
    print(f"\n🚜 [滚动收割] 清理早于 {cutoff_str} 的资产与报表...")

    # A. 清理 7 天前的 MD 历史报表
    try:
        all_reports = private_repo.get_contents("reports/hourly")
        for report in all_reports:
            # 简单通过文件名日期判断: 20260120_12.md
            if report.name.endswith(".md") and report.name[:8] < cutoff_date.strftime('%Y%m%d'):
                private_repo.delete_file(report.path, "🗑️ Cleanup old report", report.sha)
                print(f"🗑️ 已清理过期报表: {report.name}")
    except: pass

    # B. SQL 数据归档 (保持原逻辑)
    for name, config in processors_config.items():
        table = config["table_name"]
        try:
            res = supabase.table(table).select("*").lt("bj_time", cutoff_str).limit(5000).execute()
            if res.data:
                # ...此处保留你原有的 Parquet 压制与 SQL 删除逻辑...
                pass 
        except Exception as e: print(f"❌ {table} 归档失败: {e}")

# === 🏦 5. 搬运逻辑 (防重与插入逻辑，保持原样) ===
def process_and_upload(path, sha, config):
    check = supabase.table("processed_files").select("file_sha").eq("file_sha", sha).execute()
    if check.data: return False 
    try:
        content_file = private_repo.get_contents(path)
        raw_data = json.loads(base64.b64decode(content_file.content).decode('utf-8'))
        items = config["module"].process(raw_data, path)
        if items:
            for i in range(0, len(items), 500):
                supabase.table(config["table_name"]).insert(items[i : i+500]).execute()
            supabase.table("processed_files").upsert({"file_sha": sha, "file_path": path}).execute()
            return True
    except Exception as e: print(f"⚠️ {path} 解析异常: {e}")
    return False

def sync_bank_to_sql(processors_config, full_scan=False):
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    commits = private_repo.get_commits(since=since)
    for commit in commits:
        for f in commit.files:
            if f.filename.endswith('.json'):
                source_key = f.filename.split('/')[0]
                if source_key in processors_config:
                    process_and_upload(f.filename, f.sha, processors_config[source_key])

# === 🚀 6. 执行入口 ===
if __name__ == "__main__":
    all_procs = get_all_processors()
    sync_bank_to_sql(all_procs, full_scan=(os.environ.get("FORCE_FULL_SCAN")=="true"))
    generate_hot_reports(all_procs) # 核心修改：每小时生成 MD 战报
    
    # 凌晨收割窗口
    current_hour_utc = datetime.now(timezone.utc).hour
    if (20 <= current_hour_utc <= 22) or (os.environ.get("FORCE_HARVEST")=="true"):
        perform_grand_harvest(all_procs)
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ✅ 审计任务圆满完成。")
