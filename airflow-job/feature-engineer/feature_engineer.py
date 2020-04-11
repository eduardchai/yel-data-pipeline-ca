#!/opt/conda/default/bin/python
import argparse
import datetime
import pyspark
import logging

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, IntegerType, FloatType

log = logging.getLogger()


def remove_stopwords(words):
    """Remove stop words from list of tokenized words"""
    from nltk.corpus import stopwords

    new_words = []
    stop = stopwords.words("english")
    try:
        for word in words.split():
            if word not in stop:
                new_words.append(word)
    except:
        pass
    return " ".join(new_words)


def preprocess_content(sentence):
    from nltk.tokenize import RegexpTokenizer

    tokenizer = RegexpTokenizer(r"\w+")
    sentence = " ".join(word for word in tokenizer.tokenize(sentence))
    sentence = sentence.lower()

    return sentence


def compute_content_length(review):
    return int(len(review.split(" ")))


def compute_rating_deviation(user_rating, restaurant_rating):
    return float(abs(float(user_rating) - float(restaurant_rating)) / 4)


def compute_cosine_similarity(reviews):
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics import pairwise_distances

    vector = TfidfVectorizer(min_df=0)
    max = 0
    try:
        tfidf = vector.fit_transform(reviews)
        cosine = 1 - pairwise_distances(tfidf, metric="cosine")
        np.fill_diagonal(cosine, -np.inf)
        max = cosine.max()
    except:
        pass

    return float(max)


remove_stopwords_udf = udf(remove_stopwords)
preprocess_content_udf = udf(preprocess_content)
compute_content_length_udf = udf(compute_content_length, IntegerType())
compute_rating_deviation_udf = udf(compute_rating_deviation, FloatType())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="partition date")

    args = parser.parse_args()

    if args.date is None:
        curr_date = datetime.datetime.now()
        curr_date = curr_date.strftime("%Y-%m-%d")
    else:
        curr_date = args.date

    QUERY = f"SELECT * FROM `big-data-project-272506.mock.restaurant_reviews_raw` WHERE DATE(timestamp) = '{curr_date}'"
    log.info(QUERY)

    bq = bigquery.Client()
    query_job = bq.query(QUERY)
    query_job.result()

    sc = pyspark.SparkContext()

    spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

    df = (
        spark.read.format("bigquery")
        .option("dataset", query_job.destination.dataset_id)
        .option("table", query_job.destination.table_id)
        .load()
        .persist()
    )

    df_processed = df.withColumn(
        "review_content", remove_stopwords_udf("review_content")
    )
    df_processed = df_processed.withColumn(
        "review_content", preprocess_content_udf("review_content")
    )
    df_processed = df_processed.withColumn(
        "review_length", compute_content_length_udf("review_content")
    )
    df_processed = df_processed.withColumn(
        "rating_deviation", compute_rating_deviation_udf("rating", "restaurant_rating")
    )

    max_review_count = (
        df_processed.groupby("reviewer_id", "timestamp")
        .count()
        .groupby()
        .max()
        .collect()[0][0]
    )
    maximum_review_per_user_df = df_processed.groupby(
        "reviewer_id", "timestamp"
    ).count()

    df_processed = df_processed.join(
        maximum_review_per_user_df,
        (df_processed.reviewer_id == maximum_review_per_user_df.reviewer_id)
        & (df_processed.timestamp == maximum_review_per_user_df.timestamp),
    ).select(df_processed["*"], maximum_review_per_user_df["count"])

    mnr_udf = udf(lambda x: x / max_review_count, FloatType())
    df_processed = df_processed.withColumn("maximum_review_per_user", mnr_udf("count"))
    df_processed = df_processed.drop("count")

    cos_sim_df = (
        df_processed.select("reviewer_id", "review_content")
        .rdd.reduceByKey(lambda a, b: a.append(b) if type(a) == list else ([a] + [b]))
        .map(lambda x: (x[0], compute_cosine_similarity(x[1])))
        .toDF(["reviewer_id", "cos_sim"])
    )

    df_processed = df_processed.join(
        cos_sim_df, df_processed.reviewer_id == cos_sim_df.reviewer_id
    ).select(df_processed["*"], cos_sim_df["cos_sim"])

    date_decoration = curr_date.replace("-", "")

    # Need to repartition since our data is small and it is generating the small chunk problem
    df_processed.repartition(4).write.format("bigquery").mode("Overwrite").option(
        "table",
        f"big-data-project-272506:mock.restaurant_reviews_final${date_decoration}",
    ).option("temporaryGcsBucket", "big-data-project-272506-temp").option(
        "createDisposition", "CREATE_NEVER"
    ).save()
