import json
from datetime import datetime, timedelta

# === ⚙️ 基础配置 ===
TABLE_NAME = "github_logs"
ARCHIVE_FOLDER = "github"

# === 🛠️ 1. 数据清洗 (入库用) ===
def process(raw_data, path):
    processed_list = []
    
    # 1. 获取该批次的统一时间 (信赖采集端的时间)
    meta = raw_data.get('meta', {})
    batch_time = meta.get('scanned_at_bj')
    
    # 容错：如果采集端没传时间，才用当前时间兜底
    if not batch_time:
        batch_time = (datetime.utcnow() + timedelta(hours=8)).isoformat()

    items = raw_data.get('items', [])
    for item in items:
        entry = {
            "bj_time": batch_time,             # 批次号
            "repo_name": item.get('name'),
            "url": item.get('url'),
            "stars": int(item.get('stars', 0)),
            "reason": item.get('reason'),      # 相信采集端的判断
            "topics": item.get('tags', []),    # 相信采集端的分类
            "raw_json": item
        }
        processed_list.append(entry)
        
    return processed_list

# === 📤 2. 战报生成 (原文直出模式) ===
def get_hot_items(supabase, table_name):
    # 1. 找到“最新一期”的时间点
    try:
        latest = supabase.table(table_name).select("bj_time").order("bj_time", desc=True).limit(1).execute()
        if not latest.data: return {}
        target_time = latest.data[0]['bj_time']
        
        # 2. 拉取该期所有数据 (不做任何 limit 限制，因为采集端已经筛过了)
        res = supabase.table(table_name).select("*").eq("bj_time", target_time).execute()
        all_repos = res.data if res.data else []
        
    except Exception as e:
        print(f"⚠️ GitHub 数据拉取失败: {e}")
        return {}

    if not all_repos: return {}

    # 3. 简单的按 Tag 分组
    sector_pool = {}
    for repo in all_repos:
        tags = repo.get('topics', [])
        if not tags: tags = ["Uncategorized"]
        
        for tag in tags:
            if tag not in sector_pool: sector_pool[tag] = []
            sector_pool[tag].append(repo)

    # 4. 生成输出矩阵
    intelligence_matrix = {}
    
    # 获取板块顺序 (可选：按项目数量降序，或者您手动定死)
    sorted_sectors = sorted(sector_pool.keys(), key=lambda k: len(sector_pool[k]), reverse=True)
    
    for tag in sorted_sectors:
        items = sector_pool[tag]
        
        # 依然按 Stars 简单排个序，方便阅读 (可选)
        items.sort(key=lambda x: x['stars'], reverse=True)
        
        display_items = []
        for r in items:
            display_items.append({
                # 既然没有算法，score 直接给 stars，或者给 1 都可以
                "score": r['stars'],
                "user_name": f"{tag} | {r['reason']}", # 抬头显示：板块 | 理由
                "full_text": f"{r['repo_name']}",
                "url": r['url']
            })
        
        # 直接全量输出，因为“采集端已经做过筛选了”
        intelligence_matrix[tag] = display_items

    return intelligence_matrix
