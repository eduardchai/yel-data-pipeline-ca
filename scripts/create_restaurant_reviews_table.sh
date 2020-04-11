bq mk \
--table \
--schema schema/restaurant_reviews_raw.json \
--time_partitioning_type=DAY \
--time_partitioning_field timestamp \
big-data-project-272506:yelp_dataset.restaurant_reviews_raw

bq mk \
--table \
--schema schema/restaurant_reviews_final.json \
--time_partitioning_type=DAY \
--time_partitioning_field timestamp \
big-data-project-272506:yelp_dataset.restaurant_reviews_final

bq mk \
--table \
--schema schema/restaurant_reviews_labeled.json \
--time_partitioning_type=DAY \
--time_partitioning_field timestamp \
big-data-project-272506:yelp_dataset.restaurant_reviews_labeled