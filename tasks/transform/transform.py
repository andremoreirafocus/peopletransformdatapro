from tasks.transform.services.get_records_from_bronze_jsonlines import (
    get_records_from_bronze_jsonlines,
)
from tasks.transform.services.generate_parquet_buffer_from_records import (
    generate_parquet_buffer_from_records,
)
from connections.get_minio_connection_data import get_minio_connection_data
from infra.storage.write_generic_bytes_to_minio import write_generic_bytes_to_minio


def transform(year, month, day, hour):
    # Ingestion from staging to bronze
    bronze_bucket_name = "bronze"
    silver_bucket_name = "silver"
    app_folder = "people"
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/hour={hour}/"
    destination_object_name = f"{app_folder}/year={year}/month={month}/day={day}/hour={hour}/consolidated-{year}{month}{day}{hour}.jsonl"
    connection_data = get_minio_connection_data()
    # Transformation from bronze to silver
    # Read the consolidated JSONL file from the bronze bucket
    object_name = f"{app_folder}/year={year}/month={month}/day={day}/hour={hour}/consolidated-{year}{month}{day}{hour}.jsonl"
    connection_data = get_minio_connection_data()
    bronze_records = get_records_from_bronze_jsonlines(
        connection_data, object_name, bronze_bucket_name
    )
    print(bronze_records)

    if bronze_records:
        out_buffer = generate_parquet_buffer_from_records(bronze_records)
        destination_object_name = (
            f"{prefix}consolidated-{year}{month}{day}{hour}.parquet"
        )
        destination_bucket_name = silver_bucket_name
        write_generic_bytes_to_minio(
            connection_data,
            buffer=out_buffer,
            bucket_name=destination_bucket_name,
            object_name=destination_object_name,
        )
    else:
        print("No records found to write to the destination bucket.")
