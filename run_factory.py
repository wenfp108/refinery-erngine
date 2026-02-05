import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from supabase import create_client
from factory import UniversalFactory  # 导入通用工厂类

# === ⚙️ 配置区 ===
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 你的中央银行在 GitHub Action 里的相对路径
VAULT_PATH = "../vault"

# 你所有的情报源表名
TARGET_TABLES = [
    "polymarket_logs",
    "twitter_logs",
    "reddit_logs",
    "github_logs",
    "papers_logs"
]

def fetch_fresh_data(table_name, minutes=70):
    """
    从指定表捞取最近 N 分钟的数据
    """
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # ✅ 修复：强制对齐北京时间 (UTC+8)
        # 确保与 refinery.py 写入的 bj_time 格式一致，避免字符串比较时出现 8 小时偏差
        bj_now = datetime.now(timezone(timedelta(hours=8)))
        cutoff_time = (bj_now - timedelta(minutes=minutes)).isoformat()
        
        print(f"🎣 [{table_name}] 正在扫描新数据 (阈值: {cutoff_time})...")
        
        # 限制单次最大获取 1000 条
        res = supabase.table(table_name)\
            .select("*")\
            .gt("bj_time", cutoff_time)\
            .limit(1000)\
            .execute()
            
        data = res.data
        if data:
            print(f"   ✅ 捕获 {len(data)} 条信号")
            return data
        else:
            print(f"   💤 无新增信号")
            return []
            
    except Exception as e:
        print(f"   ⚠️ [{table_name}] 读取失败: {e}")
        return []

def main():
    bj_now_str = datetime.now(timezone(timedelta(hours=8))).isoformat()
    print(f"🚀 [Cognitive Factory] 启动时间: {bj_now_str}")
    
    all_signals = []
    
    # 1. 遍历所有源，收集新鲜原料
    for table in TARGET_TABLES:
        rows = fetch_fresh_data(table)
        if rows:
            all_signals.extend(rows)
            
    if not all_signals:
        print("📭 本轮巡检未发现任何新数据，工厂休眠。")
        return

    print(f"📦 原料准备完毕，共计 {len(all_signals)} 条混合信号。")

    # 2. 转换为 DataFrame 并进行预处理
    df = pd.DataFrame(all_signals)
    temp_file = "temp_run_batch.parquet"
    
    # ✅ 核心修复：强制将 raw_json 列转换为纯字符串格式
    # 解决 pyarrow 无法混合处理 dict 和 string 导致的 ArrowInvalid 报错
    if 'raw_json' in df.columns:
        df['raw_json'] = df['raw_json'].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else str(x)
        )
    
    # 兼容性：确保数值字段类型统一，防止空值报错
    numeric_cols = ['volume', 'liquidity', 'vol24h', 'day_change', 'stars', 'citations', 'score']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    # 保存为临时 Parquet
    try:
        df.to_parquet(temp_file, engine='pyarrow', index=False)
    except Exception as e:
        print(f"❌ Parquet 写入失败 (数据结构异常): {e}")
        return

    # 3. 唤醒大师，开工
    try:
        # masters_path="masters" 对应 workflow 里复制过来的插件目录
        factory = UniversalFactory(masters_path="masters")
        
        print("🏭 流水线全速运转中...")
        factory.process_and_ship(
            input_raw=temp_file, 
            vault_path=VAULT_PATH
        )
        
    except Exception as e:
        print(f"❌ 工厂运行严重错误: {e}")
        
    finally:
        # 4. 清理现场
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print("🧹 临时文件已清理。")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ [错误] 环境变量缺失 (SUPABASE_URL/KEY)")
    else:
        main()
