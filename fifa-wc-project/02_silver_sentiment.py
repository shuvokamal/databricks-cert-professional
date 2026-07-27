# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer — Cleaning + Sentiment Analysis
# MAGIC Reads raw Bronze comments, cleans them, runs VADER sentiment,
# MAGIC tags team/player mentions, and saves to silver_comments Delta table.

# COMMAND ----------

# MAGIC %pip install vaderSentiment

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 1. Read Bronze Table

# COMMAND ----------

bronze_df = spark.table("fifa_wc.bronze_comments")
print(f"Bronze rows: {bronze_df.count():,}")
bronze_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 2. Clean the Data

# COMMAND ----------

from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, length,
    when, lit, array, array_contains
)

cleaned_df = bronze_df \
    .dropDuplicates(["comment_id"]) \
    .filter(col("comment_text").isNotNull()) \
    .filter(length(trim(col("comment_text"))) > 5) \
    .withColumn("comment_clean",
        regexp_replace(
            regexp_replace(
                regexp_replace(col("comment_text"),
                    r"http\S+", ""),        # remove URLs
                r"[^\x00-\x7F]+", " "),     # remove emojis/non-ASCII
            r"\s+", " ")                    # collapse multiple spaces
    ) \
    .withColumn("comment_clean", trim(col("comment_clean")))

print(f"After cleaning: {cleaned_df.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 3. Tag Team & Player Mentions

# COMMAND ----------

# Keywords to detect mentions of each team/player
TAGS = {
    "mentions_argentina": ["argentina", "albiceleste", "messi", "arg"],
    "mentions_spain":     ["spain", "españa", "espana", "la roja", "esp"],
    "mentions_england":   ["england", "three lions", "eng"],
    "mentions_france":    ["france", "les bleus", "fra", "mbappe", "mbappé"],
    "mentions_messi":     ["messi", "leo"],
    "mentions_yamal":     ["yamal", "lamine"],
    "mentions_ronaldo":   ["ronaldo", "cr7"],
}

tagged_df = cleaned_df
for col_name, keywords in TAGS.items():
    pattern = "|".join(keywords)
    tagged_df = tagged_df.withColumn(
        col_name,
        lower(col("comment_text")).rlike(pattern)
    )

tagged_df.select("comment_text", *TAGS.keys()).show(5, truncate=60)

# COMMAND ----------
# MAGIC %md ## 4. Run VADER Sentiment Analysis

# COMMAND ----------

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd

# COMMAND ----------

# Collect to Pandas — safe for our dataset size (~2500 rows)
# Avoids UDF serialization issues entirely
pandas_df = tagged_df.toPandas()

analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not text or len(str(text).strip()) == 0:
        return pd.Series([0.0, 0.0, 1.0, 0.0, "neutral"])
    scores = analyzer.polarity_scores(str(text))
    compound = float(scores["compound"])
    label = "positive" if compound >= 0.05 else "negative" if compound <= -0.05 else "neutral"
    return pd.Series([compound, float(scores["pos"]), float(scores["neu"]), float(scores["neg"]), label])

pandas_df[["sentiment_score","sentiment_pos","sentiment_neu","sentiment_neg","sentiment_label"]] = \
    pandas_df["comment_clean"].apply(get_sentiment)

print(f"Sentiment analysis complete: {len(pandas_df):,} rows")
print(pandas_df[["comment_clean","sentiment_score","sentiment_label"]].head(5))

# COMMAND ----------

# Convert back to Spark DataFrame
silver_df = spark.createDataFrame(pandas_df)
silver_df.show(5, truncate=60)

# COMMAND ----------
# MAGIC %md ## 5. Save to Silver Delta Table

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fifa_wc.silver_comments")

print(f"Silver table saved: {spark.table('fifa_wc.silver_comments').count():,} rows")

# COMMAND ----------
# MAGIC %md ## 6. Quick Sentiment Check — Is Argentina Most Negative?

# COMMAND ----------

spark.sql("""
    SELECT
        CASE
            WHEN mentions_argentina THEN 'Argentina'
            WHEN mentions_spain     THEN 'Spain'
            WHEN mentions_france    THEN 'France'
            WHEN mentions_england   THEN 'England'
            ELSE 'Other'
        END AS team_mentioned,
        COUNT(*)                          AS total_comments,
        ROUND(AVG(sentiment_score), 3)    AS avg_sentiment,
        SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_count,
        SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_count,
        ROUND(
            SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
        ) AS negative_pct
    FROM fifa_wc.silver_comments
    GROUP BY 1
    ORDER BY avg_sentiment ASC
""").show()
