# IMPORTANT!!! You need to upload the data to GCS bucket first
# Then update the gcs path accordingly

bq load \
--source_format=CSV \
--skip_leading_rows=1 \
big-data-project-272506:yelp_dataset.restaurant_reviews_raw \
gs://big-data-raw-data/20200406/*.csv

bq load \
--source_format=CSV \
--skip_leading_rows=1 \
big-data-project-272506:yelp_dataset.restaurant_reviews_final \
gs://big-data-final-data/20200406/*.csv

bq load \
--source_format=CSV \
--skip_leading_rows=1 \
big-data-project-272506:yelp_dataset.restaurant_reviews_labeled \
gs://big-data-labeled-data/20200406/*.csv