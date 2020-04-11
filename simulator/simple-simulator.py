import json
import pandas as pd
import time
import random

from google.cloud import pubsub_v1
from random import sample

project_id = "big-data-project-272506"
topic_name = "yelp-restaurant-reviews"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

df = pd.read_csv("data/yelp-restaurant-reviews-raw.csv")
df_sampled = df[:1000]

rows = []
for row in df_sampled.to_dict(orient="records"):
    rows.append(row)

while True:
    # print(f"Publishing review #{n}")
    review = sample(rows, 1)[0]
    review_id = review["review_id"]
    if type(review_id) == str:
        review_id = review_id.strip()
        if review_id != "":
            review["timestamp"] = int(time.time())
            data = json.dumps(review)
            data = data.encode("utf-8")
            future = publisher.publish(topic_path, data=data)
            print("Review published with id:", future.result())
            time.sleep((random.randint(1, 5) / 10.0))

print("Published messages.")
