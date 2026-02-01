import json
from datetime import datetime, timedelta

# 对应 Supabase 里的表名
TABLE_NAME = "papers_logs"

def fmt_k(num):
    if not num: return "0"
    try: n = float(num)
    except: return "0"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

# === 1. 数据清洗逻辑 ===
def process(raw_data, path):
    # 兼容处理：有些 JSON 是 dict (含 meta)，有些可能是 list
    data = raw_data if isinstance(raw_data, dict) else {}
    items = data.get("items", [])
    meta = data.get("meta", {})
    
    # 获取扫描时间，如果没有则用当前时间
    scanned_at = meta.get("scanned_at_bj")
    if not scanned_at:
        scanned_at = datetime.now().isoformat()
        
    refined_results = []
    for i in items:
        # 提取 metrics，防止 key 不存在报错
        metrics = i.get("metrics", {})
        
        row = {
            "bj_time": scanned_at,
            "title": i.get("title"),
            "journal": i.get("journal"),
            # 区分 ☢️ NUCLEAR 和 ⚡ EARLY_SIGNAL
            "signal_type": i.get("type", "General"), 
            "citations": int(metrics.get("citations", 0)),
            "impact_factor": float(metrics.get("impact_factor", 0.0)),
            # 数组转 JSON 字符串
            "strategies": i.get("strategies", []), 
            "url": i.get("url"),
            "reason": i.get("reason"),
            "raw_json": i
        }
        refined_results.append(row)
    return refined_results

# === 2. 战报生成逻辑 ===
def get_hot_items(supabase, table_name):
    # 只看最近 24 小时的数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_papers = res.data if res.data else []
    except Exception as e:
        print(f"Papers DB Error: {e}")
        return {}
    
    if not all_papers: return {}

    # 去重逻辑
    unique_map = {}
    for p in all_papers:
        title = p.get("title")
        if not title: continue
        unique_map[title] = p
    
    papers = list(unique_map.values())
    
    # 排序逻辑：核爆级置顶，然后按引用数降序
    def sort_key(p):
        score = p.get("citations", 0)
        if "NUCLEAR" in p.get("signal_type", ""):
            score += 100000 
        return score

    papers.sort(key=sort_key, reverse=True)
    
    # 构建 Markdown 表格
    # 🔥 修改：移除“期刊”列，精简为核心信息
    header = "| 信号 | 标题 | 引用 | 标签 (关键词) | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for p in papers:
        s_type = p.get("signal_type","")
        # 图标美化
        if "NUCLEAR" in s_type:
            icon = "☢️ **NUCLEAR**"
        elif "EARLY" in s_type:
            icon = "⚡ Early"
        else:
            icon = "📄 Paper"
            
        title = p.get("title", "")
        if len(title) > 65: title = title[:65] + "..."
            
        cite = fmt_k(p.get("citations", 0))
        
        # 处理标签显示
        tags = p.get("strategies", [])
        if isinstance(tags, str):
            try: tags = json.loads(tags)
            except: tags = []
        
        # 标签加粗显示，视觉更清晰
        tag_str = ", ".join([f"**{t}**" for t in tags[:2]])
        
        url = p.get("url", "#")
        
        # 🔥 修改：不包含 journal 字段
        rows.append(f"| {icon} | {title} | {cite} | {tag_str} | [🔗]({url}) |")
        
    return {"🔬 Science Radar (科研前哨)": {"header": header, "rows": rows}}
