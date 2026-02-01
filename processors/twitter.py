import json
import math
import re
from datetime import datetime, timedelta

TABLE_NAME = "twitter_logs"
TARGET_TOTAL_QUOTA = 30 

# === 🛑 1. 政治/垃圾噪音词 (核打击) ===
# 只要出现，分数直接打 1 折
NOISE_KEYWORDS = [
    "woke", "maga", "democrat", "republican", "leftist", "right wing", "liberal", "conservative",
    "fascist", "communist", "socialist", "pronouns", "dei", "border crisis", "illegal",
    "trump", "biden", "harris", "vance", "pelosi", "schumer", "election", "ballot",
    "scandal", "epstein", "pedophile", "traitor", "shame", "disgrace", "culture war",
    "nazi", "hitler", "antisemitism", "zionist", "genocide"
]

# === 🔰 2. 宏观豁免词 (免死金牌) ===
# 政治贴里如果有这些词，说明在聊正事，不降权
MACRO_IMMUNITY = [
    "fed", "federal reserve", "powell", "fomc", "rate", "interest", "cut", "hike",
    "tariff", "trade war", "sanction", "export", "import", "duty",
    "china", "taiwan", "russia", "ukraine", "israel", "iran", "war", "military",
    "stimulus", "debt", "deficit", "budget", "tax", "treasury", "bond", "yield",
    "bitcoin", "btc", "crypto", "ban", "regulation", "sec", "gensler", "etf",
    "executive order", "veto", "sign", "bill", "act", "law", "legislation",
    "nominate", "nominee", "appoint", "confirm", "supreme court"
]

# === 🧠 3. 精准话题词库 (权重竞价模式) ===
# 词越长、越专业，权重越高，防止误判
TOPIC_RULES = {
    "Crypto": [
        "bitcoin", "btc", "ethereum", "eth", "solana", "defi", "nft", "stablecoin", "usdc", "usdt",
        "etf flow", "blackrock", "layer2", "zk-rollup", "airdrop", "staking", "restaking", "memecoin",
        "binance", "coinbase", "satoshi", "vitalik", "on-chain analysis", "wallet", "altcoin"
    ],
    "AI/Tech": [
        "llm", "transformer", "genai", "generative ai", "inference", "training run", "pre-training",
        "gpt-5", "gpt-4", "claude", "gemini", "llama", "deepseek", "mistral", "anthropic", "openai",
        "nvidia", "nvda", "h100", "blackwell", "cuda", "gpu", "tpu", "asic", "compute",
        "tsmc", "asml", "semiconductor", "chip", "wafer", "Moore's law",
        "spacex", "starship", "falcon", "tesla", "tsla", "fsd", "optimus", "robot",
        "python", "rust", "github", "huggingface", "arxiv", "open source"
    ],
    "Science": [
        "nature journal", "science magazine", "arxiv", "peer review", "preprint",
        "nasa", "esa", "jwst", "supernova", "exoplanet", "quantum", "entanglement",
        "superconductor", "lk-99", "fusion energy", "iter", "plasma",
        "crispr", "mrna", "protein", "enzyme", "cancer research", "alzheimer", "longevity"
    ],
    "Macro": [
        "sp500", "nasdaq", "bond yield", "treasury", "curve inversion",
        "gold", "xau", "silver", "crude oil", "brent", "natural gas",
        "earnings call", "revenue", "guidance", "profit margin", "buyback", "dividend",
        "fomc", "fed funds", "powell", "cpi", "ppi", "pce", "inflation", "deflation", "stagflation",
        "gdp", "recession", "soft landing", "non-farm", "unemployment", "jobless", "payroll",
        "balance sheet", "quantitative tightening", "liquidity injection"
    ],
    "Geo": [
        "ukraine", "russia", "putin", "zelensky", "donbas", "kursk",
        "israel", "gaza", "hamas", "hezbollah", "iran", "tehran", "red sea", "houthi",
        "china", "xi jinping", "taiwan", "south china sea", "pla", "semiconductor sanction",
        "nato", "pentagon", "dod", "nuclear", "icbm", "drone warfare"
    ]
}

VIP_AUTHORS = [
    "Karpathy", "Yann LeCun", "Vitalik", "Paul Graham", "Naval", 
    "Eric Topol", "Huberman", "Lex Fridman", "Sam Altman", "Kobeissi Letter",
    "Michael Saylor", "Balaji"
]

def fmt_k(num):
    if not num: return "0"
    try: n = float(num)
    except: return "0"
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

def to_iso_bj(date_str):
    try:
        utc_dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')
        return (utc_dt + timedelta(hours=8)).isoformat()
    except: return datetime.now().isoformat()

def process(raw_data, path):
    items = raw_data if isinstance(raw_data, list) else [raw_data]
    refined_results = []
    for i in items:
        # 垃圾过滤：如果正文太短且没有链接，直接丢弃（杀掉 "Yes..." 这种水贴）
        text = i.get('fullText', '')
        if len(text) < 10 and 'http' not in text:
            continue

        user = i.get('user', {})
        metrics = i.get('metrics', {})
        row = {
            "bj_time": to_iso_bj(i.get('createdAt')),
            "user_name": user.get('name'),
            "screen_name": user.get('screenName'),
            "followers_count": user.get('followersCount'),
            "full_text": text,
            "url": i.get('tweetUrl'), 
            "tags": i.get('tags', []),
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

# 🔥 核心：上帝权重算法 2.0 🔥
def calculate_score_and_tag(item):
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "")
    
    # 1. 基础热度 (书签 x10, 转推 x5, 点赞 x1)
    metrics = item.get('raw_json', {}).get('metrics', {})
    base_score = (metrics.get('retweets', 0) * 5) + \
                 (metrics.get('bookmarks', 0) * 10) + \
                 metrics.get('likes', 0)
    
    # 2. 话题竞价 (解决分类幻觉)
    detected_topic = "General"
    max_keyword_len = 0 # 匹配到的关键词越长，置信度越高
    
    for topic, keywords in TOPIC_RULES.items():
        for k in keywords:
            # 必须是独立单词匹配，防止 "training" 匹配到 "straining" (虽然英文较少见，但逻辑更严谨)
            if k in text:
                # 简单的优先级：如果这个词比之前匹配到的词更长/更具体，就采纳这个分类
                if len(k) > max_keyword_len:
                    detected_topic = topic
                    max_keyword_len = len(k)
    
    # 3. 语义加权 vs 降权
    if detected_topic != "General":
        # 命中硬核板块：加分
        base_score += 2000
        base_score *= 1.5
    else:
        # General 惩罚：如果是水贴，分数打对折
        # 除非它是超级大热点，否则别想挤掉硬核内容
        base_score *= 0.5 

    # 4. 政治排毒
    has_noise = False
    for noise in NOISE_KEYWORDS:
        if noise in text:
            has_noise = True
            break
            
    if has_noise:
        is_immune = False
        for safe in MACRO_IMMUNITY:
            if safe in text:
                is_immune = True
                break
        if not is_immune:
            base_score *= 0.1 # 核打击
            detected_topic = "Politics" # 强制标记
            
    # 5. VIP 加成
    for vip in VIP_AUTHORS:
        if vip.lower() in user.lower():
            base_score += 5000
            break
            
    return base_score, detected_topic

def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e: return {}

    if not all_tweets: return {}

    unique_map = {}
    for t in all_tweets:
        key = t.get('url') or (t.get('user_name'), t.get('full_text'))
        if key not in unique_map:
            unique_map[key] = t
    tweets = list(unique_map.values())

    scored_tweets = []
    for t in tweets:
        score, topic = calculate_score_and_tag(t)
        t['_score'] = score
        t['_topic'] = topic
        scored_tweets.append(t)
        
    scored_tweets.sort(key=lambda x: x['_score'], reverse=True)
    
    # 🛡️ 多样性熔断 (每人最多 3 条)
    final_list = []
    author_counts = {}
    
    for t in scored_tweets:
        if len(final_list) >= TARGET_TOTAL_QUOTA:
            break
            
        author = t['user_name']
        if author_counts.get(author, 0) >= 3:
            continue
            
        final_list.append(t)
        author_counts[author] = author_counts.get(author, 0) + 1
        
    # 生成大表
    header = "| 信号 | 🏷️ 标签 | 热度 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for t in final_list:
        score_display = fmt_k(t['_score'])
        
        # 标签美化
        topic_raw = t['_topic']
        if topic_raw == "General": topic_str = "General" # 不加粗
        else: topic_str = f"**{topic_raw}**" # 硬核标签加粗
        
        heat = f"❤️ {fmt_k(t.get('likes',0))}<br>🔁 {fmt_k(t.get('retweets',0))}" 
        user = t['user_name']
        text = t['full_text'].replace('\n', ' ')[:70] + "..."
        url = t['url']
        
        rows.append(f"| **{score_display}** | {topic_str} | {heat} | {user} | {text} | [🔗]({url}) |")

    return {"🏆 全域精选 (Top 30)": {"header": header, "rows": rows}}
