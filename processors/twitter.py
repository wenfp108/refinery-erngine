import json
import math
from datetime import datetime, timedelta

# === ⚙️ 1. 基础配置 ===
TABLE_NAME = "twitter_logs"
ARCHIVE_FOLDER = "twitter"

SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
TARGET_TOTAL_QUOTA = 30  # 基准总配额

# === 🛠️ 2. 数据清洗 (入库) ===
def to_iso_bj(date_str):
    try:
        utc_dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')
        return (utc_dt + timedelta(hours=8)).isoformat()
    except:
        return datetime.now().isoformat()

def process(raw_data, path):
    items = raw_data if isinstance(raw_data, list) else [raw_data]
    refined_results = []
    
    for i in items:
        user = i.get('user', {})
        metrics = i.get('metrics', {})
        growth = i.get('growth', {})
        
        row = {
            "bj_time": to_iso_bj(i.get('createdAt')),
            "user_name": user.get('name'),
            "screen_name": user.get('screenName'),
            "followers_count": user.get('followersCount'),
            "full_text": i.get('fullText'),
            "url": i.get('tweetUrl'), 
            "tags": i.get('tags', []),
            
            # 基础数据
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "replies": metrics.get('replies', 0),
            "quotes": metrics.get('quotes', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "views": metrics.get('views', 0),
            
            # 增长数据
            "growth_views": growth.get('views', 0),
            "growth_likes": growth.get('likes', 0),
            "growth_retweets": growth.get('retweets', 0),
            "growth_replies": growth.get('replies', 0),
            
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

# === 🧮 3. 核心打分公式 ===
def calculate_twitter_score(item):
    base_interaction = (
        item.get('retweets', 0) * 8 + 
        item.get('quotes', 0) * 12 + 
        item.get('replies', 0) * 5 + 
        item.get('bookmarks', 0) * 10
    )
    
    growth_momentum = (
        item.get('growth_likes', 0) * 15 + 
        item.get('growth_retweets', 0) * 25 + 
        item.get('growth_replies', 0) * 10
    )
    
    synergy_boost = 1 + (len(item.get('tags', [])) * 0.3)
    
    return (base_interaction + growth_momentum) * synergy_boost

# === 📤 4. 战报生成 (含去重算法) ===
def get_hot_items(supabase, table_name):
    # 1. 拉取过去 24 小时全量数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e:
        print(f"⚠️ Twitter 数据拉取失败: {e}")
        return {}

    if not all_tweets: return {}

    # 2. 预计算分数
    for t in all_tweets:
        t['_score'] = calculate_twitter_score(t)

    # 🔥🔥 3. 核心去重算法 (新增) 🔥🔥
    unique_map = {}
    for t in all_tweets:
        # 使用 URL 作为唯一身份证
        # 如果没有 URL，退而求其次用 (用户名+内容) 组合
        key = t.get('url') or (t.get('user_name'), t.get('full_text'))
        
        if key not in unique_map:
            unique_map[key] = t
        else:
            # 如果重复，保留“分数更高”的那个（说明互动更多，数据更新）
            if t['_score'] > unique_map[key]['_score']:
                unique_map[key] = t
    
    # 替换为去重后的列表
    deduplicated_tweets = list(unique_map.values())
    total_unique_tweets = len(deduplicated_tweets)

    # 4. 计算板块密度
    sector_pools = {s: [] for s in SECTORS}
    
    for t in deduplicated_tweets:
        tags = t.get('tags', [])
        for tag in tags:
            if tag in sector_pools:
                sector_pools[tag].append(t)

    # 5. 生成最终矩阵
    intelligence_matrix = {}
    
    for sector, pool in sector_pools.items():
        if not pool: continue
        
        # 排序
        pool.sort(key=lambda x: x['_score'], reverse=True)
        
        # 配额
        quota = max(3, math.ceil((len(pool) / total_unique_tweets) * TARGET_TOTAL_QUOTA))
        
        display_items = []
        for t in pool[:quota]:
            display_items.append({
                "score": int(t['_score']),
                "user_name": t['user_name'],
                "full_text": t['full_text'],
                "tweet_url": t['url']
            })
        
        intelligence_matrix[sector] = display_items

    return intelligence_matrix
