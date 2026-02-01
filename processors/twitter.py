import json
from datetime import datetime, timedelta

# === 基础配置 ===
TABLE_NAME = "twitter_logs"
ARCHIVE_FOLDER = "twitter"

# 7大核心审计板块
SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Defense"]
# 每板块精品篇数 (建议 5 条以对冲阅读压力)
PER_SECTOR_COUNT = 5 

def to_iso_bj(date_str):
    """
    将 UTC 时间强制对齐至北京时间
    输入格式: Fri Jan 30 20:06:09 +0000 2026
    """
    try:
        utc_dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')
        bj_dt = utc_dt + timedelta(hours=8)
        return bj_dt.isoformat()
    except:
        return datetime.now().isoformat()

# === 数据搬运逻辑 ===

def process(raw_data, path):
    """
    全量解析并保留所有原始资产
    """
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
            "tweet_url": i.get('tweetUrl'),
            "tags": i.get('tags', []),
            
            # 存量指标
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "replies": metrics.get('replies', 0),
            "quotes": metrics.get('quotes', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "views": metrics.get('views', 0),
            
            # 增量动量 (Alpha 核心)
            "growth_views": growth.get('views', 0),
            "growth_likes": growth.get('likes', 0),
            "growth_retweets": growth.get('retweets', 0),
            "growth_replies": growth.get('replies', 0),
            
            # 原始备份
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

# === Alpha-Signal-V1 专业审计算法 ===

def get_hot_items(supabase, table_name):
    """
    执行多板块动量对冲审计
    """
    intelligence_matrix = {}
    
    # 专业评分公式：结合传播深度、行动意向与爆发速度
    # 算法逻辑：(Retweets*8 + Quotes*12 + Replies*5 + Bookmarks*10) + (Growth_Retweets*25 + ...)
    score_formula = """
        (
            (retweets * 8 + quotes * 12 + replies * 5 + bookmarks * 10) + 
            (growth_likes * 15 + growth_retweets * 25 + growth_replies * 10)
        ) * (1 + jsonb_array_length(tags) * 0.3)
    """

    for sector in SECTORS:
        try:
            # 针对每个板块执行独立加权排序
            res = supabase.table(table_name).select("*") \
                .contains("tags", [sector]) \
                .order(score_formula, descending=True) \
                .limit(PER_SECTOR_COUNT) \
                .execute()
            
            if res.data:
                # 在返回数据中注入计算后的分值，方便生成 MD 表格
                for record in res.data:
                    # 简单模拟评分赋值，便于渲染层读取
                    record['score'] = (
                        (record['retweets'] * 8 + record['quotes'] * 12 + record['replies'] * 5 + record['bookmarks'] * 10) +
                        (record['growth_likes'] * 15 + record['growth_retweets'] * 25 + record['growth_replies'] * 10)
                    ) * (1 + len(record['tags']) * 0.3)
                
                intelligence_matrix[sector] = res.data
        except Exception as e:
            print(f"Error auditing {sector}: {e}")

    return intelligence_matrix
