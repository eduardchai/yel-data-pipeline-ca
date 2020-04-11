FROM google/cloud-sdk:slim

WORKDIR /app
COPY main.sh .

RUN chmod +x main.sh

ENTRYPOINT [ "sh", "main.sh" ]