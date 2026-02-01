iimport json
from datetime import datetime, timedelta

TABLE_NAME = "twitter_logs"
ARCHIVE_FOLDER = "twitter"

SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
PER_SECTOR_COUNT = 5

def to_iso_bj(date_str):
    try:
        utc_dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')
        bj_dt = utc_dt + timedelta(hours=8)
        return bj_dt.isoformat()
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
            "tweet_url": i.get('tweetUrl'),
            "tags": i.get('tags', []),
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "replies": metrics.get('replies', 0),
            "quotes": metrics.get('quotes', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "views": metrics.get('views', 0),
            "growth_views": growth.get('views', 0),
            "growth_likes": growth.get('likes', 0),
            "growth_retweets": growth.get('retweets', 0),
            "growth_replies": growth.get('replies', 0),
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

def get_hot_items(supabase, table_name):
    intelligence_matrix = {}
    
    score_formula = """
        (
            (retweets * 8 + quotes * 12 + replies * 5 + bookmarks * 10) + 
            (growth_likes * 15 + growth_retweets * 25 + growth_replies * 10)
        ) * (1 + jsonb_array_length(tags) * 0.3)
    """

    for sector in SECTORS:
        intelligence_matrix[sector] = []
        try:
            res = supabase.table(table_name).select("*") \
                .contains("tags", [sector]) \
                .order(score_formula, descending=True) \
                .limit(PER_SECTOR_COUNT) \
                .execute()
            
            if res.data:
                for record in res.data:
                    record['score'] = (
                        (record['retweets'] * 8 + record['quotes'] * 12 + record['replies'] * 5 + record['bookmarks'] * 10) +
                        (record['growth_likes'] * 15 + record['growth_retweets'] * 25 + record['growth_replies'] * 10)
                    ) * (1 + len(record['tags']) * 0.3)
                
                intelligence_matrix[sector] = res.data
        except Exception as e:
            print(f"Error auditing {sector}: {e}")

    return intelligence_matrix
