FROM google/cloud-sdk:slim

WORKDIR /app
COPY feature_engineer.py .

COPY submit-job.sh .
RUN chmod +x submit-job.sh

ENTRYPOINT [ "sh", "submit-job.sh" ]