import json
import math
from datetime import datetime, timedelta

# ==========================================
# ⚙️ 配置区 (V4.1 - 2026-02-01 Final)
# ==========================================

TABLE_NAME = "twitter_logs"
TARGET_TOTAL_QUOTA = 30  # 🌟 最终战报只选 Top 30

# === 🛑 1. 政治/垃圾噪音词 (核打击) ===
# 仅保留无意义的情绪宣泄词，避免误杀正经政治讨论
NOISE_KEYWORDS = [
    "woke", "libtard", "magatard", "shame", "disgrace", "traitor", 
    "pedophile", "epstein", "pronouns", "culture war", "scandal",
    "destroy", "lies", "liar", "clown", "hypocrite", "idiot", "scam"
]

# === 🔰 2. 宏观豁免词 (免死金牌) ===
# 政治贴里如果有这些词，说明在聊正事（立法/宏观/监管），不降权
MACRO_IMMUNITY = [
    "fed", "federal reserve", "powell", "fomc", "rate", "interest", "cut", "hike",
    "tariff", "trade war", "sanction", "export", "import", "duty",
    "china", "taiwan", "russia", "ukraine", "israel", "iran", "war", "military",
    "stimulus", "debt", "deficit", "budget", "tax", "treasury", "bond", "yield",
    "bitcoin", "btc", "crypto", "ban", "regulation", "sec", "etf",
    "executive order", "veto", "sign", "bill", "act", "law", "legislation",
    "nominate", "nominee", "appoint", "confirm", "supreme court", "ruling"
]

# === 🧠 3. 精准话题词库 (7大板块 - 权重竞价模式) ===
# 改进重点：彻底拆分 Finance(市场) 与 Economy(宏观)，MSTR 归入 Crypto
TOPIC_RULES = {
    "Tech": [ # 科技：AI, 芯片, 编程, 硬科技
        "llm", "genai", "gpt-5", "gpt-4", "claude", "gemini", "llama", "deepseek", "anthropic", "openai",
        "nvidia", "nvda", "h100", "blackwell", "cuda", "gpu", "semiconductor", "tsmc", "asml", "wafer",
        "spacex", "starship", "falcon", "tesla", "tsla", "fsd", "robot", "optimus", "figure ai",
        "python", "rust", "github", "huggingface", "open source", "coding", "developer"
    ],
    "Politics": [ # 政治：只保留机构与立法，强制降噪
        "white house", "biden", "trump", "harris", "vance", "congress", "senate", "house of rep",
        "supreme court", "scotus", "legislation", "bill", "veto", "executive order", "amendment",
        "election", "poll", "voter", "ballot", "campaign", "republican", "democrat", "gop", "dnc",
        "regulator", "antitrust", "doj", "ftc", "sec chairman"
    ],
    "Finance": [ # 金融：二级市场, 投行, 财报, 波动率 (Micro/Market)
        "sp500", "nasdaq", "spx", "ndx", "dow jones", "russell 2000", "vix", "volatility",
        "stock", "equity", "earnings", "revenue", "margin", "guidance", "buyback", "dividend",
        "goldman", "jpmorgan", "morgan stanley", "bloomberg", "blackrock", "citadel", "bridgewater",
        "ipo", "merger", "acquisition", "short seller", "long position", "call option", "put option",
        "liquidity", "market maker", "hedge fund", "pension fund"
    ],
    "Economy": [ # 经济：宏观, 央行, 周期, 国债 (Macro)
        "fomc", "federal reserve", "jerome powell", "fed funds", "interest rate", "hike", "cut",
        "cpi", "ppi", "pce", "inflation", "deflation", "stagflation", "recession", "soft landing",
        "gdp", "unemployment", "jobless", "jolts", "non-farm", "payroll", "labor market",
        "treasury", "bond", "yield", "10y", "2y", "curve inversion", "debt ceiling", "deficit",
        "ism", "pmi", "retail sales", "housing start"
    ],
    "Geo": [ # 地缘：战争, 外交, 制裁
        "ukraine", "russia", "putin", "zelensky", "kursk", "kyiv",
        "israel", "gaza", "hamas", "iran", "tehran", "red sea", "houthi", "hezbollah",
        "china", "xi jinping", "taiwan", "south china sea", "pla", "ccp",
        "nato", "pentagon", "nuclear", "weapon", "sanction", "trade war", "tariff"
    ],
    "Science": [ # 科学：学术, 能源, 生物, 航天
        "nature journal", "science magazine", "arxiv", "peer review", "preprint",
        "nasa", "esa", "jwst", "supernova", "exoplanet", "quantum", "fusion energy", "lk-99",
        "crispr", "mrna", "cancer", "alzheimer", "longevity", "biology", "physics", "chemistry"
    ],
    "Crypto": [ # 加密：Web3, 币, 链 (包含 MSTR)
        "bitcoin", "btc", "ethereum", "eth", "solana", "defi", "stablecoin", "usdc", "usdt",
        "etf flow", "blackrock", "coinbase", "binance", "satoshi", "vitalik", "memecoin", "doge",
        "wallet", "private key", "smart contract", "layer2", "zk-rollup", "airdrop",
        "mstr", "microstrategy", "michael saylor" # 🌟 Saylor 的 Alpha 归属
    ]
}

# === 🛡️ 4. VIP 白名单 (基础分加成) ===
VIP_AUTHORS = [
    # Tech / AI
    "Karpathy", "Yann LeCun", "Paul Graham", "Sam Altman", "François Chollet", 
    "Rowan Cheung", "Naval", "Palmer Luckey", "Anduril", "Elon Musk",
    
    # Finance / Macro / Economy
    "Nick Timiraos", "Ray Dalio", "Mohamed A. El-Erian", "Kobeissi Letter", 
    "Walter Bloomberg", "Zerohedge", "Lyn Alden", "MacroAlf", "Goldman Sachs",
    "Peter Schiff", "Michael Saylor", "Nassim Nicholas Taleb", "CME Group",
    "Fitch Ratings", "IMF", "Unusual Whales", "The Economist", "WSJ Central Banks",
    
    # Geo / Politics / Science
    "Ian Bremmer", "Eric Topol", "Vitalik", "SentDefender", "Visegrád 24",
    "Spectator Index", "Disclose.tv", "Defense News", "Council on Foreign Relations"
]

# ==========================================
# ⚙️ 核心逻辑函数
# ==========================================

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

# ✅ 修正版 Process: 完全对齐 SQL Schema
def process(raw_data, path):
    items = raw_data if isinstance(raw_data, list) else [raw_data]
    refined_results = []
    
    for i in items:
        # 🗑️ 垃圾过滤：过短且无链接直接丢弃
        text = i.get('fullText', '')
        if len(text) < 10 and 'http' not in text:
            continue

        user = i.get('user', {})
        metrics = i.get('metrics', {})
        
        # 容错提取 growth 数据
        growth_views = i.get('growth_views', 0)
        growth_likes = i.get('growth_likes', 0)
        growth_retweets = i.get('growth_retweets', 0)
        growth_replies = i.get('growth_replies', 0)

        row = {
            # --- 基础信息 (Base Info) ---
            "bj_time": to_iso_bj(i.get('createdAt')),
            "user_name": user.get('name'),
            "screen_name": user.get('screenName'),
            "followers_count": user.get('followersCount'),
            "full_text": text,
            "tweet_url": i.get('tweetUrl'),         # 对应 SQL: tweet_url
            "tags": json.dumps(i.get('tags', [])),  # 对应 SQL: tags (JSONB)
            
            # --- 📊 实时总量数据 (Metrics) ---
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "replies": metrics.get('replies', 0),   # 对应 SQL: replies
            "quotes": metrics.get('quotes', 0),     # 对应 SQL: quotes
            "bookmarks": metrics.get('bookmarks', 0),
            "views": i.get('views', metrics.get('viewCount', 0)), # 对应 SQL: views
            
            # --- 📈 增长数据 (Growth) ---
            "growth_views": growth_views,
            "growth_likes": growth_likes,
            "growth_retweets": growth_retweets,
            "growth_replies": growth_replies,
            
            # --- 原始数据备份 ---
            "raw_json": json.dumps(i) if isinstance(i, dict) else i
        }
        refined_results.append(row)
        
    return refined_results

# 🔥 核心：上帝权重算法 4.1 (Finance/Economy拆分 + 政治降噪) 🔥
def calculate_score_and_tag(item):
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "")
    
    # 1. 基础热度 (书签 x10, 转推 x5, 点赞 x1)
    # 注意：这里的 item 已经是 process 后的 SQL 格式，metrics 都在顶层
    base_score = (item.get('retweets', 0) * 5) + \
                 (item.get('bookmarks', 0) * 10) + \
                 item.get('likes', 0)
    
    # 2. 话题竞价 (Strict Tagging)
    detected_topic = "General"
    max_keyword_len = 0 # 匹配到的关键词越长，置信度越高
    
    for topic, keywords in TOPIC_RULES.items():
        for k in keywords:
            if k in text:
                # 优先级逻辑：保留匹配到的最长/最具体的关键词所属的话题
                if len(k) > max_keyword_len:
                    detected_topic = topic
                    max_keyword_len = len(k)
    
    # 3. 语义加权 vs 降权
    if detected_topic != "General":
        # 💎 命中硬核板块：大幅加分
        base_score += 2000
        base_score *= 1.5
    else:
        # 📉 General 惩罚
        base_score *= 0.5 

    # 4. 政治排毒 (Nuclear Detox)
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
            base_score *= 0.1 # 💣 无豁免的政治噪音，直接打1折
            detected_topic = "Politics" # 强制标记，方便追溯
            
    # 5. VIP 加成
    for vip in VIP_AUTHORS:
        if vip.lower() in user.lower():
            base_score += 5000
            break
            
    return base_score, detected_topic

def get_hot_items(supabase, table_name):
    # 假设 supabase 客户端已经初始化并传入
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e:
        print(f"Database error: {e}")
        return {}

    if not all_tweets: return {}

    # 1. URL 去重 (防止重复抓取导致数据污染)
    unique_map = {}
    for t in all_tweets:
        # 优先用 tweet_url 做唯一键
        key = t.get('tweet_url') or (t.get('user_name'), t.get('full_text'))
        if key not in unique_map:
            unique_map[key] = t
    tweets = list(unique_map.values())

    # 2. 算分 & 打标
    scored_tweets = []
    for t in tweets:
        score, topic = calculate_score_and_tag(t)
        t['_score'] = score
        t['_topic'] = topic
        scored_tweets.append(t)
        
    # 3. 全局排序 (分数从高到低)
    scored_tweets.sort(key=lambda x: x['_score'], reverse=True)
    
    # 4. 🛡️ 多样性熔断 (Diversity Breaker)
    # 限制单人霸榜，每人最多保留前 3 条
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
        
    # 5. 生成战报 (V4.1 视觉优化版)
    header = "| 信号 | 🏷️ 标签 | 热度 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for t in final_list:
        score_display = fmt_k(t['_score'])
        
        topic_raw = t['_topic']
        
        # 🎨 视觉降噪逻辑：
        # General 和 Politics -> 不加粗 (视觉上变弱)
        # Tech, Crypto, Finance 等硬核板块 -> **加粗** (强调 Alpha)
        if topic_raw in ["General", "Politics"]: 
            topic_str = topic_raw 
        else: 
            topic_str = f"**{topic_raw}**"
        
        # 热度展示
        heat = f"❤️ {fmt_k(t.get('likes',0))}<br>🔁 {fmt_k(t.get('retweets',0))}" 
        
        user = t['user_name']
        # 智能摘要：去除换行，截取前70字符
        text_preview = t['full_text'].replace('\n', ' ')[:70] + "..."
        url = t.get('tweet_url', '#')
        
        rows.append(f"| **{score_display}** | {topic_str} | {heat} | {user} | {text_preview} | [🔗]({url}) |")

    return {"🏆 全域精选 (Top 30)": {"header": header, "rows": rows}}
