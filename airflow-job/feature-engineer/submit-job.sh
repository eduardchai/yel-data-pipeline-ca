gcloud dataproc jobs submit pyspark \
	--cluster big-data-cluster --region us-central1 \
    --project big-data-project-272506 \
	--jars gs://big-data-common-jars/gcs-connector-hadoop2-latest.jar,gs://big-data-common-jars/spark-bigquery-latest.jar \
	feature_engineer.py -- --date=$1