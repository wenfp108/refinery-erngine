import os, json, base64, requests, importlib.util, sys
from datetime import datetime, timedelta, timezone
import pandas as pd
from supabase import create_client
from github import Github, Auth

# === 🛡️ 1. 核心配置 ===
PRIVATE_BANK_ID = "wenfp108/Central-Bank" 
GITHUB_TOKEN = os.environ.get("GH_PAT") 
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("❌ [审计异常] 环境变量缺失。")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
auth = Auth.Token(GITHUB_TOKEN)
gh_client = Github(auth=auth)
private_repo = gh_client.get_repo(PRIVATE_BANK_ID)

# === 🧩 2. 插件发现系统 ===
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
                }
            except Exception as e: print(f"⚠️ 插件 {name} 加载失败: {e}")
    return procs

# === ⏱️ 辅助：检查数据新鲜度 ===
def get_data_freshness(table_name):
    try:
        res = supabase.table(table_name).select("bj_time").order("bj_time", desc=True).limit(1).execute()
        if not res.data: return (False, 9999, "无数据")
        
        last_time_str = res.data[0]['bj_time']
        if not last_time_str: return (False, 9999, "无时间戳")

        try:
            last_time_str = last_time_str.replace('Z', '+00:00')
            last_time = datetime.fromisoformat(last_time_str)
        except:
            return (False, 9999, last_time_str)
        
        now = datetime.now(timezone(timedelta(hours=8)))
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone(timedelta(hours=8)))
        
        diff = now - last_time
        minutes_ago = int(diff.total_seconds() / 60)
        
        return (minutes_ago <= 65, minutes_ago, last_time.strftime('%H:%M'))
    except Exception as e:
        return (True, 0, "CheckError")

# === 🔥 3. 战报工厂 ===

def generate_hot_reports(processors_config):
    # print("\n🔥 [情报对冲] 正在生成 Markdown 时报...") # 日志简化，这行可注释
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    
    file_name = bj_now.strftime('%Y-%m-%d-%H') + ".md"
    report_path = f"reports/{file_name}"
    
    date_display = bj_now.strftime('%Y-%m-%d %H:%M')
    
    md_report = f"# 🚀 Architect's Alpha 情报审计 ({date_display})\n\n"
    md_report += "> **机制说明**：全源智能去重 | 无更新源自动折叠\n\n"

    has_content = False

    for source_name, config in processors_config.items():
        if hasattr(config["module"], "get_hot_items"):
            try:
                table = config["table_name"]
                is_fresh, mins_ago, last_update_time = get_data_freshness(table)
                
                if not is_fresh:
                    md_report += f"## 💤 来源：{source_name.upper()} (上次更新: {last_update_time})\n"
                    md_report += f"> *距上次更新已过 {int(mins_ago/60)} 小时，暂无新数据。*\n\n"
                    continue 

                sector_matrix = config["module"].get_hot_items(supabase, table)
                if not sector_matrix: continue

                has_content = True
                md_report += f"## 📡 来源：{source_name.upper()}\n"
                
                for sector, items in sector_matrix.items():
                    md_report += f"### 🏷️ 板块：{sector}\n"
                    md_report += "| 信号强度 | 源头 | 关键情报摘要 | 链接 |\n| :--- | :--- | :--- | :--- |\n"
                    
                    for item in items:
                        score = int(item.get('score', 0))
                        source = item.get('user_name', 'Unknown')
                        text = item.get('full_text', '').replace('\n', ' ')[:85] + "..."
                        url = item.get('url') or item.get('tweet_url') or '#'
                        md_report += f"| **{score:,}** | {source} | {text} | [查看]({url}) |\n"
                    md_report += "\n"
            except Exception as e:
                pass # 静默失败，保持日志整洁

    if not has_content:
        md_report += "\n\n**🛑 本轮扫描全域静默，请查阅历史归档。**"

    try:
        try:
            old = private_repo.get_contents(report_path)
            private_repo.update_file(old.path, f"📊 Update: {file_name}", md_report, old.sha)
            # print(f"✅ 更新战报: {report_path}") # 日志简化
        except:
            private_repo.create_file(report_path, f"🚀 New: {file_name}", md_report)
            # print(f"✅ 创建战报: {report_path}") # 日志简化
    except Exception as e: 
        print(f"❌ 写入 {report_path} 失败: {e}")

# === 🚜 4. 滚动收割 (定制日志版) ===

def perform_grand_harvest(processors_config):
    print("⏰ 触发每日滚动收割 (检查过期数据)...")
    
    cutoff_date = (datetime.now() - timedelta(days=7))
    cutoff_str = cutoff_date.isoformat()
    print(f"🚜 [滚动收割] 启动... 检查 7 天前 ({cutoff_date.strftime('%Y-%m-%d %H:%M')} 之前) 的数据")

    # A. 清理报表 (静默处理，除非有删除)
    try:
        all_reports = private_repo.get_contents("reports")
        for report in all_reports:
            if not report.name.endswith(".md"): continue
            file_date_str = report.name[:10].replace('-', '')
            cutoff_date_str = cutoff_date.strftime('%Y%m%d')
            
            if file_date_str.isdigit() and file_date_str < cutoff_date_str:
                private_repo.delete_file(report.path, "🗑️ Cleanup old report", report.sha)
                print(f"🗑️ 已清理过期报表: {report.name}")
    except: pass

    # B. SQL 数据归档 (定制日志)
    for name, config in processors_config.items():
        table = config["table_name"]
        try:
            # 尝试删除并返回计数
            res = supabase.table(table).delete().lt("bj_time", cutoff_str).execute()
            count = len(res.data) if res.data else 0
            
            if count > 0:
                print(f"🧹 {table}: 已清理 {count} 条过期数据。")
            else:
                print(f"ℹ️ {table}: 未找到 7 天前数据 (所有数据均为最新)。")
        except Exception as e: 
            print(f"ℹ️ {table}: 未找到 7 天前数据 (或表结构不支持)。")

# === 🏦 5. 搬运逻辑 (静默版) ===

def process_and_upload(path, sha, config):
    check = supabase.table("processed_files").select("file_sha").eq("file_sha", sha).execute()
    if check.data: return 0
    
    # print(f"📥 正在处理: {path} ...") # 注释掉，保持日志清爽
    try:
        content_file = private_repo.get_contents(path)
        raw_data = json.loads(base64.b64decode(content_file.content).decode('utf-8'))
        
        items = config["module"].process(raw_data, path)
        count = len(items) if items else 0
        
        if items:
            for i in range(0, len(items), 500):
                supabase.table(config["table_name"]).insert(items[i : i+500]).execute()
            supabase.table("processed_files").upsert({
                "file_sha": sha, 
                "file_path": path,
                "engine": config.get("table_name", "unknown").split('_')[0],
                "item_count": count
            }).execute()
            return count
    except Exception as e: pass
    return 0

def sync_bank_to_sql(processors_config, full_scan=False):
    # 1. 打印带时间的标头
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] 🏦 巡检开始: 1小时增量提取")
    
    # 初始化计数器
    stats = {name: 0 for name in processors_config.keys()}
    
    if full_scan:
        # print("🚜 [全量模式] ...") # 静默
        try:
            contents = private_repo.get_contents("")
            while contents:
                file_content = contents.pop(0)
                if file_content.type == "dir":
                    contents.extend(private_repo.get_contents(file_content.path))
                elif file_content.name.endswith(".json"):
                    source_key = file_content.path.split('/')[0]
                    if source_key in processors_config:
                        added = process_and_upload(file_content.path, file_content.sha, processors_config[source_key])
                        stats[source_key] += added
        except Exception as e: print(f"❌ Scan Error: {e}")
            
    else:
        # print("⚡ [增量模式] ...") # 静默
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        commits = private_repo.get_commits(since=since)
        for commit in commits:
            for f in commit.files:
                if f.filename.endswith('.json'):
                    source_key = f.filename.split('/')[0]
                    if source_key in processors_config:
                        added = process_and_upload(f.filename, f.sha, processors_config[source_key])
                        stats[source_key] += added

    # 2. 打印格式化看板
    # 格式：✅ twitter     | 现状：发现新动态 (+79)
    for source, count in stats.items():
        # 对齐填充: source名补齐到 12 字符
        source_display = f"{source:<12}"
        if count > 0:
            print(f"✅ {source_display} | 现状：发现新动态 (+{count})")
        else:
            print(f"➖ {source_display} | 现状：无新文件变动 (+0)")

# === 🚀 6. 执行入口 ===
if __name__ == "__main__":
    all_procs = get_all_processors()
    
    is_full_scan = (os.environ.get("FORCE_FULL_SCAN") == "true")
    
    # 1. 同步 (输出巡检日志)
    sync_bank_to_sql(all_procs, full_scan=is_full_scan)
    
    # 2. 战报 (静默生成)
    generate_hot_reports(all_procs)
    
    # 3. 归档 (输出收割日志)
    # 每天凌晨 4 点 (UTC 20点) 运行，或者强制运行
    current_hour_utc = datetime.now(timezone.utc).hour
    if (20 <= current_hour_utc <= 22) or (os.environ.get("FORCE_HARVEST")=="true"):
        perform_grand_harvest(all_procs)
