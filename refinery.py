import os, json, base64, requests, importlib.util, sys
from datetime import datetime, timedelta, timezone
import pandas as pd
from supabase import create_client
from github import Github

# === 🛡️ 1. 核心配置 (通过 Secrets 注入) ===
# 您的私人金库 ID，引擎在此处执行“输入/输出”操作
PRIVATE_BANK_ID = "wenfp108/Central-Bank" 

GITHUB_TOKEN = os.environ.get("GH_PAT") 
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("❌ [审计异常] 环境变量配置缺失，请检查 GitHub Secrets。")

# 初始化基础设施
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
gh_client = Github(GITHUB_TOKEN)
private_repo = gh_client.get_repo(PRIVATE_BANK_ID)

# === 🧩 2. 插件发现系统 (通用性核心) ===

def get_all_processors():
    """动态扫描并加载 ./processors/ 目录下的所有审计插件"""
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
            except Exception as e:
                print(f"⚠️ [系统警告] 无法加载插件 {name}: {e}")
    return procs

# === 🔥 3. 情报对冲 (Top N 每小时简报) ===

def generate_hot_reports(processors_config):
    """基于各插件自定义算法生成每小时热门快报并推回私人库"""
    print("\n🔥 [情报对冲] 正在启动每小时热门情报审计...")
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    report_data = {
        "timestamp_bj": bj_now.isoformat(),
        "brief": {}
    }

    for name, config in processors_config.items():
        if hasattr(config["module"], "get_hot_items"):
            try:
                # 决策条数逻辑已下放到插件内部
                hot_items = config["module"].get_hot_items(supabase, config["table_name"])
                if hot_items:
                    report_data["brief"][name] = hot_items
                    print(f"✅ {name}: 成功对冲出 {len(hot_items)} 条关键信号")
            except Exception as e:
                print(f"⚠️ {name} 提取热门失败: {e}")

    if report_data["brief"]:
        date_tag = bj_now.strftime('%Y%m%d')
        hour_tag = bj_now.strftime('%H')
        target_path = f"reports/hourly/{date_tag}_{hour_tag}.json"
        content = json.dumps(report_data, ensure_ascii=False, indent=2)
        
        try:
            old = private_repo.get_contents(target_path)
            private_repo.update_file(old.path, f"🔥 Intelligence Brief: {hour_tag}h", content, old.sha)
        except:
            private_repo.create_file(target_path, f"🚀 New Intelligence Brief: {hour_tag}h", content)
        print(f"✨ 每小时对冲快报已同步至私人库: {target_path}")

# === 🚜 4. 滚动收割 (7天弹性窗口归档) ===

def perform_grand_harvest(processors_config):
    """清理 7 天前旧资产，压制为 Parquet 冷存储"""
    # 滑动窗口：清理 7 天前之前的过期数据
    cutoff_date = (datetime.now() - timedelta(days=7)).isoformat()
    date_tag = datetime.now().strftime('%Y%m%d')
    print(f"\n🚜 [滚动收割] 启动巡检。目标：早于 {cutoff_date} 的陈年资产...")
    
    for name, config in processors_config.items():
        table = config["table_name"]
        try:
            # 分批查询
            res = supabase.table(table).select("*").lt("bj_time", cutoff_date).limit(5000).execute()
            if not res.data:
                print(f"ℹ️ {table}: 未发现过期数据 (当前所有数据均为 7 天内热资产)。")
                continue
            
            print(f"📦 {table}: 发现 {len(res.data)} 条过期数据，开始资产压制...")
            local_file = f"{table}_{date_tag}.parquet"
            pd.DataFrame(res.data).astype(str).to_parquet(local_file, index=False)
            
            target_path = f"archives/{config['archive_folder']}/{date_tag}.parquet"
            with open(local_file, "rb") as f: content = f.read()
            
            try:
                old = private_repo.get_contents(target_path)
                private_repo.update_file(old.path, f"📦 Archive Update: {date_tag}", content, old.sha)
            except:
                private_repo.create_file(target_path, f"📦 Archive New: {date_tag}", content)
            
            # 🛡️ 防御型操作：确认上传成功后，分批删除 (200条一波) 以避开 SQL 限制
            ids = [row['id'] for row in res.data]
            print(f"🗑️ 正在清空 SQL 历史缓存...")
            for i in range(0, len(ids), 200):
                supabase.table(table).delete().in_("id", ids[i:i+200]).execute()
            
            if os.path.exists(local_file): os.remove(local_file)
            print(f"✅ {table}: 资产归档成功。")
        except Exception as e:
            print(f"❌ {table} 归档失败 (审计官异常): {e}")

# === 🏦 5. 搬运逻辑 (指纹防重与 24 小时回溯) ===

def process_and_upload(path, sha, config):
    """SHA 指纹检查 -> 数据入库"""
    # 幂等性检查：防重是防御型架构的灵魂
    check = supabase.table("processed_files").select("file_sha").eq("file_sha", sha).execute()
    if check.data: return False 

    try:
        content_file = private_repo.get_contents(path)
        raw_data = json.loads(base64.b64decode(content_file.content).decode('utf-8'))
        items = config["module"].process(raw_data, path)
        
        if items:
            # 🛡️ 核心修复：500条一波插入，防止 1000 条 API 报错
            for i in range(0, len(items), 500):
                supabase.table(config["table_name"]).insert(items[i : i+500]).execute()
            
            # 记录 SHA 指纹锁
            supabase.table("processed_files").upsert({"file_sha": sha, "file_path": path}).execute()
            return True
    except Exception as e:
        print(f"⚠️ 文件 {path} 解析异常: {e}")
    return False

def sync_bank_to_sql(processors_config, full_scan=False):
    """
    【同步核心】采用 24 小时回溯窗口，对冲采集延迟风险。
    """
    print(f"\n🏦 [中央银行] 启动巡检模式: {'全量扫描' if full_scan else '24h 重叠扫描'}...")
    
    if full_scan:
        for name, config in processors_config.items():
            folder = config["archive_folder"]
            try:
                contents = private_repo.get_contents(folder)
                while contents:
                    file_content = contents.pop(0)
                    if file_content.type == "dir":
                        contents.extend(private_repo.get_contents(file_content.path))
                    elif file_content.name.endswith(".json"):
                        process_and_upload(file_content.path, file_content.sha, config)
            except Exception as e:
                print(f"⚠️ 扫描私人库 {folder} 失败: {e}")
    else:
        # 对冲延迟策略：回溯过去 24 小时的所有 Commit
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
    # 环境参数控制：支持手动强制执行
    IS_FULL_SCAN = os.environ.get("FORCE_FULL_SCAN", "false").lower() == "true"
    FORCE_HARVEST = os.environ.get("FORCE_HARVEST", "false").lower() == "true"

    # 1. 第一步：插件装载
    all_procs = get_all_processors()
    print(f"🔍 检测到 {len(all_procs)} 个活跃处理器插件")
    
    # 2. 第二步：增量同步 (基于 SHA 指纹防重)
    sync_bank_to_sql(all_procs, full_scan=IS_FULL_SCAN)
    
    # 3. 第三步：情报时报生成 (Top N)
    generate_hot_reports(all_procs)
    
    # 4. 第四步：资产收割 (弹性窗口：北京时间凌晨 4:00 - 6:00)
    # UTC 20点 - 22点 均为有效收割时间，对冲 GitHub Action 延迟风险
    current_hour_utc = datetime.now(timezone.utc).hour
    is_harvest_window = 20 <= current_hour_utc <= 22 

    if is_harvest_window or FORCE_HARVEST:
        perform_grand_harvest(all_procs)
    else:
        print(f"⏳ 当前 UTC 时间 {current_hour_utc}h，未到预定收割时间 (20-22h UTC)。")
        
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ✅ 审计任务结束。")
