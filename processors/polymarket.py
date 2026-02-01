import json
import math
from datetime import datetime, timedelta

# === ⚙️ 1. 基础配置 ===
TABLE_NAME = "polymarket_logs"  # 对应你的 SQL 表名
ARCHIVE_FOLDER = "polymarket"

# === 🛠️ 2. 数据清洗工具 (入库用) ===
def to_bj_time(utc_str):
    """把 UTC 时间强制转为北京时间 (ISO格式)"""
    if not utc_str: return None
    try:
        # 处理 Polymarket 的 Z 结尾时间
        dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        return (dt + timedelta(hours=8)).isoformat()
    except: return None

def parse_num(val):
    """清洗数值：去掉逗号、$符号、百分号，转为 float"""
    if not val: return 0
    s = str(val).replace(',', '').replace('$', '').replace('%', '')
    try:
        return float(s)
    except:
        return 0

# === 📥 3. 入库算法 (Process) ===
# 严格对应你的 SQL 结构：bj_time, title, slug, ticker...
def process(raw_data, path):
    processed_list = []
    
    # 自动识别引擎：从文件名判断是 sniper 还是 radar
    engine_type = "sniper" if "sniper" in path.lower() else "radar"
    
    for item in raw_data:
        # 构造符合 SQL 的字典
        entry = {
            "bj_time": to_bj_time(item.get('updatedAt')),
            "title": item.get('eventTitle'),
            "slug": item.get('slug'),
            "ticker": item.get('ticker'),
            "question": item.get('question'),
            "prices": str(item.get('prices')), # 存为文本
            "category": item.get('category', 'OTHER'),
            
            # 数值清洗
            "volume": parse_num(item.get('volume')),
            "liquidity": parse_num(item.get('liquidity')),
            "vol24h": parse_num(item.get('vol24h')),
            "day_change": parse_num(item.get('dayChange')),
            
            # 引擎与策略
            "engine": engine_type,
            "strategy_tags": item.get('strategy_tags', []), # 存为 JSONB
            
            # 完整备份 (防后悔药)
            "raw_json": item
        }
        processed_list.append(entry)
        
    return processed_list

# === 🧮 4. 动态审计评分 (出库用) ===
# 因为数据库没存 score，我们读出来的时候现算
def calculate_score(item):
    vol24h = float(item.get('vol24h') or 0)
    day_change = abs(float(item.get('dayChange') or item.get('day_change') or 0)) # 兼容 SQL 字段名
    
    # 基础公式：量 * (波动+1)
    score = vol24h * (day_change + 1)
    
    # 狙击加成 (读取 raw_json 或字段)
    text = (str(item.get('title')) + " " + str(item.get('question'))).lower()
    snipers = ["gold", "bitcoin", "btc", "fed", "federal reserve", "xau"]
    if any(k in text for k in snipers) and "warsh" not in text:
        score *= 100
        
    # 策略加成
    tags = item.get('strategy_tags', [])
    if 'TAIL_RISK' in tags: score *= 50
    if 'HIGH_CERTAINTY' in tags: score *= 30
        
    return score

def get_win_rate(price_str):
    try:
        if "Yes: " in price_str: return float(price_str.split("Yes: ")[1].split("%")[0])
        if "Up: " in price_str: return float(price_str.split("Up: ")[1].split("%")[0])
    except: pass
    return 50.0

# === 📤 5. 战报生成算法 (Get Hot Items) ===
def get_hot_items(supabase, table_name):
    # 1. 拉取过去 24 小时的数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    # 注意：这里 select * 会把 raw_json 也拉出来，方便我们计算 score
    res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
    if not res.data: return {}
    
    all_data = res.data
    
    # 2. 区分引擎池
    sniper_pool = [i for i in all_data if i['engine'] == 'sniper']
    radar_pool = [i for i in all_data if i['engine'] == 'radar']
    
    sector_matrix = {}

    # --- 辅助函数：防刷屏 (同一 Slug 只取共识和冲突) ---
    def anti_flood_filter(items):
        grouped = {}
        for i in items:
            s = i['slug']
            if s not in grouped: grouped[s] = []
            grouped[s].append(i)
        
        final = []
        for s, rows in grouped.items():
            # 必须先计算 score 才能排序
            for r in rows: r['_temp_score'] = calculate_score(r)
            rows.sort(key=lambda x: x['_temp_score'], reverse=True)
            
            # 提取逻辑
            consensus = [r for r in rows if get_win_rate(r['prices']) > 80]
            conflict = [r for r in rows if get_win_rate(r['prices']) < 15]
            
            picks = []
            if consensus: picks.append(consensus[0])
            if conflict: picks.append(conflict[0])
            if not picks: picks.append(rows[0])
            
            final.extend(picks[:2])
        return final

    # A. 狙击区 (Sniper)
    if sniper_pool:
        refined = anti_flood_filter(sniper_pool)
        refined.sort(key=lambda x: x['_temp_score'], reverse=True)
        
        display_list = []
        for i in refined:
            display_list.append({
                "score": i['_temp_score'],
                "user_name": f"SNIPER | {get_win_rate(i['prices'])}%",
                "full_text": f"{i['question']} (Vol: ${int(i['vol24h']):,})",
                "tweet_url": f"https://polymarket.com/event/{i['slug']}"
            })
        sector_matrix["🎯 SNIPER (核心监控)"] = display_list

    # B. 雷达区 (Radar) - 比例配额
    SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
    MAP = {'POLITICS': 'Politics', 'GEOPOLITICS': 'Geopolitics', 'TECH': 'Tech', 'FINANCE': 'Finance', 'CRYPTO': 'Crypto'} # 简写映射
    
    if radar_pool:
        for s in SECTORS:
            # 过滤当前板块的数据
            pool = [i for i in radar_pool if MAP.get(i['category'], 'Other') == s or i['category'] == s.upper()]
            if not pool: continue
            
            refined = anti_flood_filter(pool)
            refined.sort(key=lambda x: x['_temp_score'], reverse=True)
            
            # 动态配额：占比 * 30，最少 3 条
            quota = max(3, math.ceil((len(pool) / len(radar_pool)) * 30))
            
            display_list = []
            for i in refined[:quota]:
                display_list.append({
                    "score": i['_temp_score'],
                    "user_name": f"{s} | {get_win_rate(i['prices'])}%",
                    "full_text": f"{i['title']} -> {i['question']}",
                    "tweet_url": f"https://polymarket.com/event/{i['slug']}"
                })
            sector_matrix[s] = display_list

    return sector_matrix
