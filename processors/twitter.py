import json
import math
from datetime import datetime, timedelta

# === ⚙️ 1. 基础配置 ===
TABLE_NAME = "twitter_logs"
ARCHIVE_FOLDER = "twitter"

# ⚠️ 注意：这个列表的顺序决定了归类的优先级
# 例如：一条推文同时有 Politics 和 Tech，它会优先进入 Politics 板块
SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
TARGET_TOTAL_QUOTA = 30 

# === 🛠️ 2. 数据清洗 (入库) ===
def fmt_k(num):
    if not num: return "-"
    try: n = float(num)
    except: return "-"
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

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

# === 📤 4. 战报生成 (含去重 + 独占逻辑) ===
def get_hot_items(supabase, table_name):
    # 1. 拉取过去 24 小时全量数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e:
        return {}

    if not all_tweets: return {}

    # 2. 预计算分数
    for t in all_tweets:
        t['_score'] = calculate_twitter_score(t)

    # 3. URL 去重 (保留分数最高的版本)
    unique_map = {}
    for t in all_tweets:
        key = t.get('url') or (t.get('user_name'), t.get('full_text'))
        if key not in unique_map:
            unique_map[key] = t
        else:
            if t['_score'] > unique_map[key]['_score']:
                unique_map[key] = t
    
    deduplicated_tweets = list(unique_map.values())
    total_unique_tweets = len(deduplicated_tweets)

    # 🔥🔥 4. 独占式分配 (核心修改点) 🔥🔥
    sector_pools = {s: [] for s in SECTORS}
    
    for t in deduplicated_tweets:
        tags = t.get('tags', [])
        
        # 按照 SECTORS 列表的顺序进行匹配
        # 优先级高的板块 (如 Politics) 会先抢走推文
        matched = False
        for sector in SECTORS:
            if sector in tags:
                sector_pools[sector].append(t)
                matched = True
                break # <--- 🛑 关键：找到归宿后立即停止，防止一稿多投！
        
        # (可选) 如果没匹配到任何板块，可以放入 Other，这里暂不处理

    # 5. 生成最终矩阵 (适配 6 列布局)
    intelligence_matrix = {}
    
    for sector, pool in sector_pools.items():
        if not pool: continue
        
        # 排序
        pool.sort(key=lambda x: x['_score'], reverse=True)
        
        # 配额
        quota = max(3, math.ceil((len(pool) / total_unique_tweets) * TARGET_TOTAL_QUOTA))
        
        display_items = []
        for t in pool[:quota]:
            score = fmt_k(t['_score'])
            views = fmt_k(t.get('views', 0))
            user = t['user_name']
            text = t['full_text'].replace('\n', ' ')[:85] + "..." # 稍微加长摘要
            url = t['url']
            
            # 组装适配 Refinery 的数据
            display_items.append({
                "display_score": score,
                "display_heat": f"👁️ {views}", # 对应 资金/热度
                "display_source": user,        # 对应 状态/源头
                "display_tags": "",            # Twitter 不需要额外标签列
                "display_summary": text,       # 对应 摘要
                "url": url
            })
        
        intelligence_matrix[sector] = display_items

    return intelligence_matrix
