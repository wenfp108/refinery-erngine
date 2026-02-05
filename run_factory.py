import os, json, pandas as pd
from datetime import datetime, timedelta, timezone
from supabase import create_client
from factory import UniversalFactory

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
VAULT_PATH = "../vault"
TARGET_TABLES = ["polymarket_logs", "twitter_logs", "reddit_logs", "github_logs", "papers_logs"]

def fetch_fresh_data(table_name, minutes=70):
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        # ✅ 时区修复：对齐北京时间
        bj_now = datetime.now(timezone(timedelta(hours=8)))
        cutoff_time = (bj_now - timedelta(minutes=minutes)).isoformat()
        
        res = supabase.table(table_name).select("*").gt("bj_time", cutoff_time).limit(1000).execute()
        return res.data or []
    except Exception as e:
        print(f"⚠️ [{table_name}] 失败: {e}")
        return []

def main():
    all_signals = []
    for table in TARGET_TABLES:
        rows = fetch_fresh_data(table)
        if rows: all_signals.extend(rows)
            
    if not all_signals: return

    df = pd.DataFrame(all_signals)
    temp_file = "temp_run_batch.parquet"
    
    # ✅ 格式修复：统一 raw_json 为字符串
    if 'raw_json' in df.columns:
        df['raw_json'] = df['raw_json'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else str(x))
    
    # ✅ 数值类型转换
    for col in ['volume', 'liquidity', 'vol24h', 'day_change', 'stars', 'citations', 'score']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    df.to_parquet(temp_file, index=False)

    try:
        factory = UniversalFactory(masters_path="masters")
        factory.process_and_ship(input_raw=temp_file, vault_path=VAULT_PATH)
    finally:
        if os.path.exists(temp_file): os.remove(temp_file)

if __name__ == "__main__":
    main()
