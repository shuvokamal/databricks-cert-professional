# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — YouTube Comments Ingestion
# MAGIC Fetches real comments from FIFA WC 2026 videos and stores them as raw Delta tables.
# MAGIC
# MAGIC **Videos covered:**
# MAGIC - Spain vs Argentina Final
# MAGIC - England vs Argentina Semifinals
# MAGIC - Spain Best Moments
# MAGIC - France vs Spain Semifinals

# COMMAND ----------

# MAGIC %pip install google-api-python-client vaderSentiment

# COMMAND ----------

# Restart Python after installing libraries
dbutils.library.restartPython()

# COMMAND ----------

from googleapiclient.discovery import build
from datetime import datetime
from pyspark.sql import Row

# YouTube API key — stored securely in Databricks widget (never commit keys to git)
dbutils.widgets.text("youtube_api_key", "", "YouTube API Key")
API_KEY = dbutils.widgets.get("youtube_api_key")

# Video IDs and their labels
VIDEOS = {
    "x-cpRHf4xd4": {"title": "Spain vs Argentina Final",       "teams": ["ARG", "ESP"], "stage": "Final"},
    "6HaHNYjnghE": {"title": "Spain 1-0 Argentina Final (FIFA)","teams": ["ARG", "ESP"], "stage": "Final"},
    "y-4saPWrPt0": {"title": "England vs Argentina Semis",      "teams": ["ARG", "ENG"], "stage": "Semifinal"},
    "AM_gWkmU2ak": {"title": "Spain Best Moments",              "teams": ["ESP"],        "stage": "Tournament"},
    "_cV8QcKp3GU": {"title": "France vs Spain Semis",           "teams": ["ESP", "FRA"], "stage": "Semifinal"},
}

youtube = build("youtube", "v3", developerKey=API_KEY)
print("YouTube client ready.")

# COMMAND ----------

# MAGIC %md ## Fetch Comments from YouTube

# COMMAND ----------

def fetch_comments(video_id, video_meta, max_comments=500):
    """Fetch up to max_comments top-level comments from a video."""
    comments = []
    next_page_token = None

    while len(comments) < max_comments:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(100, max_comments - len(comments)),
            pageToken=next_page_token,
            textFormat="plainText",
            order="relevance"
        )
        response = request.execute()

        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "comment_id":    item["id"],
                "video_id":      video_id,
                "video_title":   video_meta["title"],
                "stage":         video_meta["stage"],
                "teams":         ",".join(video_meta["teams"]),
                "comment_text":  snippet["textDisplay"],
                "author":        snippet["authorDisplayName"],
                "likes":         int(snippet["likeCount"]),
                "published_at":  snippet["publishedAt"],
                "ingested_at":   datetime.utcnow().isoformat(),
            })

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return comments

# COMMAND ----------

# Fetch comments from all videos
all_comments = []
for video_id, meta in VIDEOS.items():
    print(f"Fetching: {meta['title']}...")
    try:
        comments = fetch_comments(video_id, meta, max_comments=500)
        all_comments.extend(comments)
        print(f"  → {len(comments)} comments fetched")
    except Exception as e:
        print(f"  → Skipped ({e})")

print(f"\nTotal comments collected: {len(all_comments):,}")

# COMMAND ----------

# MAGIC %md ## Save to Bronze Delta Table

# COMMAND ----------

# Convert to Spark DataFrame
comments_df = spark.createDataFrame([Row(**c) for c in all_comments])
comments_df.printSchema()
comments_df.show(5, truncate=80)

# COMMAND ----------

# Save as Bronze Delta table
spark.sql("CREATE DATABASE IF NOT EXISTS fifa_wc")

comments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fifa_wc.bronze_comments")

count = spark.table("fifa_wc.bronze_comments").count()
print(f"Bronze table saved: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## Quick Sanity Check

# COMMAND ----------

spark.sql("""
    SELECT video_title, COUNT(*) as comment_count, AVG(likes) as avg_likes
    FROM fifa_wc.bronze_comments
    GROUP BY video_title
    ORDER BY comment_count DESC
""").show(truncate=False)
