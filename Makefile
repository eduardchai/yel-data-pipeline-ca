publish-message:
	python simulator/simple-simulator.py

init-dataproc-cluster:
	gcloud dataproc clusters create big-data-cluster \
	--region us-central1 \
	--zone us-central1-a \
	--project big-data-project-272506 \
	--image-version 1.4-ubuntu18 \
	--metadata 'PIP_PACKAGES=apache-beam[gcp] black==19.10b0 ipykernel==5.2.0 google-cloud-pubsub==1.4.2 pandas==1.0.3 nltk==3.4.5 scikit-learn==0.22.2.post1' \
	--master-machine-type n1-standard-2 \
	--master-boot-disk-size 64GB \
	--master-boot-disk-type pd-ssd \
	--num-masters 1 \
	--worker-machine-type n1-standard-2 \
	--worker-boot-disk-size 64GB \
	--worker-boot-disk-type pd-ssd \
	--num-workers 2 \
	--initialization-actions gs://big-data-common-jars/pip-install.sh

submit-job:
	gcloud dataproc jobs submit pyspark \
	--cluster big-data-cluster --region us-central1 \
	--jars gs://big-data-common-jars/gcs-connector-hadoop2-latest.jar,gs://big-data-common-jars/spark-bigquery-latest.jar \
	./spark-job/process_review.py -- --date=2020-04-06

dataflow-local:
	python ingestion-dataflow/restaurant_reviews_pipeline.py --subscription=yelp-restaurant-reviews-sub --project=big-data-project-272506

dataflow-prod:
	python ingestion-dataflow/restaurant_reviews_pipeline.py --subscription=yelp-restaurant-reviews-sub --runner=DataflowRunner --project=big-data-project-272506