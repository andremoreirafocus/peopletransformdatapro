import time
from services.get_records_from_bronze_jsonlines import get_records_from_bronze_jsonlines
from services.generate_parquet_buffer_from_records import (
    generate_parquet_buffer_from_records,
)
from services.generate_rawdatatable_from_staging_files import (
    generate_rawdatatable_from_staging_files,
)
from connections.get_minio_connection_data import get_minio_connection_data
from storage.list_objects_in_minio_bucket import list_objects_in_minio_bucket
from storage.write_generic_bytes_to_minio import write_generic_bytes_to_minio


def main():
    # Ingestion from staging to bronze
    source_bucket_name = "staging"
    bronze_bucket_name = "bronze"
    silver_bucket_name = "silver"
    app_folder = "people"
    year = 2025
    month = 12
    day = 29
    hour = 15
    start_time = time.time()
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/hour={hour}/"
    destination_object_name = f"{app_folder}/year={year}/month={month}/day={day}/hour={hour}/consolidated-{year}{month}{day}{hour}.jsonl"
    connection_data = get_minio_connection_data()
    objects_to_be_transformed = list_objects_in_minio_bucket(
        connection_data,
        bucket_name=source_bucket_name,
        prefix=prefix,
    )
    print(f"Files to be loaded in {bronze_bucket_name}: {objects_to_be_transformed}")
    partial_ts = {
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
    }
    records_buffer_with_metadata = generate_rawdatatable_from_staging_files(
        connection_data,
        object_names=objects_to_be_transformed,
        source_bucket_name=source_bucket_name,
        partial_ts=partial_ts,
    )
    print(f"Records with metadata: {records_buffer_with_metadata}")
    # Write the consolidated JSONL file to the bronze bucket
    print(f"Writing consolidated JSONL to MinIO in the {bronze_bucket_name} bucket...")
    write_generic_bytes_to_minio(
        connection_data,
        buffer=records_buffer_with_metadata,
        bucket_name=bronze_bucket_name,
        object_name=destination_object_name,
    )

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
    elapsed = time.time() - start_time
    print(f"Elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
