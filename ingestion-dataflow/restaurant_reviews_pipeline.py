import argparse
import logging
import json
import sys
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import (
    SetupOptions,
    GoogleCloudOptions,
    StandardOptions,
    WorkerOptions,
)


class BQSizeChecker(beam.DoFn):
    TAG_OVERSIZE = "over"
    TAG_PASSED = "passed"

    def process(self, elem):
        data_dumps = json.dumps(elem)
        # BQ row size limitation is 1MB
        if sys.getsizeof(data_dumps) > (1 * 1024 * 1024):
            yield beam.pvalue.TaggedOutput(self.TAG_OVERSIZE, elem)
        else:
            yield elem


class RowFormatter(beam.DoFn):
    TAG_PASSED = "passed"
    TAG_ERROR = "error"

    def process(self, elem):
        row = {}

        for key in elem:
            row[key] = elem[key]

        if "error_message" in elem:
            yield beam.pvalue.TaggedOutput(self.TAG_ERROR, elem)
        else:
            yield row


def process_error(elem, error_message=None, remove_payload=False):
    if type(elem) == dict and remove_payload:
        elem.pop("payload", None)

    if "error_message" in elem and error_message is None:
        error_message = elem["error_message"]
        elem.pop("error_message")

    processed_row = {
        "timestamp": int(time.time()),
        "review_id": elem["review_id"],
        "review_snippet": (json.dumps(elem)).encode("utf-8"),
        "error_message": error_message,
    }

    return processed_row


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project", default="big-data-project-272506", help="GCP project"
    )
    parser.add_argument("--dataset", default="mock", help="BQ Dataset")
    parser.add_argument(
        "--table", default="restaurant_reviews_raw", help="BQ destination table"
    )
    parser.add_argument("--subscription", help="Pub/Sub subscription name")
    parser.add_argument("--job_name", help="Dataflow job name")
    parser.add_argument("--runner", default="DirectRunner", help="Dataflow job runner")
    parser.add_argument("--num_of_workers", default=50, help="Dataflow num of workers")
    parser.add_argument(
        "--machine_type", default="n1-standard-2", help="Dataflow num of workers"
    )
    known_args, _ = parser.parse_known_args(argv)

    common_bq_configs = {
        "dataset": known_args.dataset,
        "project": known_args.project,
        "insert_retry_strategy": "RETRY_ON_TRANSIENT_ERROR",
        "additional_bq_parameters": {"timePartitioning": {"field": "timestamp"}},
        "create_disposition": beam.io.BigQueryDisposition.CREATE_NEVER,  # Cannot use `CREATE_IF_NEEDED` on streaming job since when beam check if the table exists, it will count as API call and hit the API call quota.
    }

    # Configure destination table
    raw_data_tbl_schema = {}
    with open(f"schema/{known_args.table}.json") as fin:
        fields = json.load(fin)
        raw_data_tbl_schema = {"fields": fields}
    common_success_bq_configs = {
        **common_bq_configs,
        "table": known_args.table,
        "schema": raw_data_tbl_schema,
    }
    # print("=" * 50)
    # print(common_success_bq_configs)

    # Configure dead letter table
    raw_data_error_tbl_schema = {}
    with open("schema/generic_dead_letter_table_schema.json") as fin:
        fields = json.load(fin)
        raw_data_error_tbl_schema = {"fields": fields}

    common_error_bq_configs = {
        **common_bq_configs,
        "table": f"{known_args.table}_error",
        "schema": raw_data_error_tbl_schema,
    }
    # print("=" * 50)
    # print(common_error_bq_configs)

    if known_args.job_name:
        job_name = known_args.job_name
    else:
        job_name = f"{known_args.table}-{int(time.time())}"
        job_name = job_name.replace("_", "-")

    # print(job_name)

    # Configure pipeline options
    pipeline_options = PipelineOptions()

    #  gcloud options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.job_name = job_name
    google_cloud_options.region = "us-central1"
    google_cloud_options.staging_location = f"gs://{known_args.project}-temp/"
    google_cloud_options.temp_location = f"gs://{known_args.project}-temp/"
    google_cloud_options.enable_streaming_engine = True

    # standard options
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True
    standard_options.runner = known_args.runner

    # worker options
    worker_options = pipeline_options.view_as(WorkerOptions)
    worker_options.max_num_workers = known_args.num_of_workers
    worker_options.machine_type = known_args.machine_type
    worker_options.autoscaling_algorithm = "THROUGHPUT_BASED"

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # fmt: off
    pipeline = beam.Pipeline(options=pipeline_options)

    messages = (
        pipeline
        | "read pub/sub messages" >> beam.io.ReadFromPubSub(
            subscription=f"projects/{known_args.project}/subscriptions/{known_args.subscription}"
        )  # noqa
        | "parse data" >> beam.Map(lambda x: json.loads(x))  # noqa
    )

    # Check size
    bq_rows = (
        messages
        | "check row size" >> beam.ParDo(BQSizeChecker()).with_outputs(
            BQSizeChecker.TAG_OVERSIZE,
            main=BQSizeChecker.TAG_PASSED
        )
    )

    # Write oversize row to error table
    write_oversize_row = (
        bq_rows[BQSizeChecker.TAG_OVERSIZE]
        | "[oversize] process row" >> beam.Map(process_error, error_message="row size exceeded 1MB", remove_payload=True)
        | "[oversize] write to BQ" >> beam.io.WriteToBigQuery(**common_error_bq_configs)
    )

    (
        write_oversize_row["FailedRows"]
        | "[oversize] uncaught exception" >> beam.Map(lambda x: process_error(x[1], "[oversize] error while writing to BQ"))
        | "[oversize] write to error table" >> beam.io.WriteToBigQuery(**common_error_bq_configs)
    )

    # Format passed data
    formatted_bq_rows = (
        bq_rows[BQSizeChecker.TAG_PASSED]
        | "[passed] validate schema" >> beam.ParDo(RowFormatter()).with_outputs(
            RowFormatter.TAG_ERROR,
            main=RowFormatter.TAG_PASSED
        )
    )

    write_error_to_bq = (
        formatted_bq_rows[RowFormatter.TAG_ERROR]
        | "[invalid] process row" >> beam.Map(process_error)
        | "[invalid] write to BQ" >> beam.io.WriteToBigQuery(**common_error_bq_configs)
    )

    (
        write_error_to_bq["FailedRows"]
        | "[invalid] uncaught exception" >> beam.Map(lambda x: process_error(x[1], "[invalid] error while writing to BQ"))
        | "[invalid] write to error table" >> beam.io.WriteToBigQuery(**common_error_bq_configs)
    )

    # Write row to BQ
    write_to_bq = (
        formatted_bq_rows[RowFormatter.TAG_PASSED]
        | "[passed] write to BQ" >> beam.io.WriteToBigQuery(**common_success_bq_configs)
    )

    (
        write_to_bq["FailedRows"]
        | "[passed] uncaught exception" >> beam.Map(lambda x: process_error(x[1], "[passed] error while writing to BQ"))
        | "[passed] write to error table" >> beam.io.WriteToBigQuery(**common_error_bq_configs)
    )

    # fmt: on

    result = pipeline.run()
    if known_args.runner == "DirectRunner":
        result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
