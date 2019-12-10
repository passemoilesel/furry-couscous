from google.cloud import bigquery, storage
import traceback
import logging


def create_table(project, dataset_id, table_id):

        schema = [
            bigquery.SchemaField("event_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_pseudo_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event_date", "DATE", mode="NULLABLE")
        ]

        client = bigquery.Client()

        tbl = project + '.' + dataset_id + '.' + table_id

        table = bigquery.Table(tbl, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

def populate_table():

    client = bigquery.Client()

    query_job = client.query("""
INSERT INTO `?.?.app_events`
(event_name, user_id, user_pseudo_id, event_date)
SELECT event_name, user_id, user_pseudo_id, PARSE_DATE('%Y%m%d',  event_date) as event_date
FROM
  `ez-mobileapp-prodtest.analytics_200206438.events_*`
GROUP BY
  event_name,user_id,user_pseudo_id,event_date;
        """)

    results = query_job.result()

    for row in results:
        print("{} : {} views".format(row.url, row.view_count))


def download_table():

    client = bigquery.Client()

    destination_uri = "gs://{}/{}".format(bucket_name, "app_events.csv")
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location='US')
    extract_job.result()

    print(
        "Exported {}:{}.{} to {}".format(
            project, dataset_id, table_id, destination_uri)
    )


def download_blob(bucket_name, source_blob_name, destination_file_name): # test passed

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))

def clean_table():

    client = bigquery.Client()

    tbl = project + '.' + dataset_id + '.' + table_id

    client.delete_table(tbl, not_found_ok=False)
    print("Deleted table '{}'.".format(tbl))


if __name__ == '__main__':

        project = "?-?-?"
        dataset_id = "?"
        table_id = "app_events"
        bucket_name = "?"
        source_blob_name = "app_events.csv"
        destination_file_name = "/?/?/app_events_downloaded.csv"

exitCode = 0
try :
        create_table(project, dataset_id, table_id)
        populate_table()
        download_table()
        download_blob(bucket_name, source_blob_name, destination_file_name)
        clean_table()

except Exception as e :
	print('Exception')
	logging.error(traceback.format_exc())
	exitCode = 1
else :
	print('Success!')
	exitCode = 0
finally :
	print(': Done.')

exit(exitCode)
