# import time
from services.write_buffer_to_minio import write_buffer_to_minio
from services.get_minio_connection_data import get_minio_connection_data
from services.list_objects_in_minio_folder import list_objects_in_minio_folder
from services.get_table_from_minio_staging_files import (
    get_table_from_minio_staging_files,
)


def main():
    source_bucket_name = "staging"
    bronze_bucket_name = "bronze"
    # silver_bucket_name = "silver"
    app_folder = "people"
    year = 2025
    month = 12
    day = 29
    hour = 15
    # start_time = time.time()
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/hour={hour}/"
    destination_object_name = f"{app_folder}/year={year}/month={month}/day={day}/hour={hour}/consolidated-{year}{month}{day}{hour}.jsonl"
    connection_data = get_minio_connection_data()
    objects_to_be_transformed = list_objects_in_minio_folder(
        connection_data,
        bucket_name=source_bucket_name,
        prefix=prefix,
    )
    print(f"Files to be loaded in {bronze_bucket_name}: {objects_to_be_transformed}")
    records_buffer_with_metadata = get_table_from_minio_staging_files(
        connection_data,
        object_names=objects_to_be_transformed,
        source_bucket_name=source_bucket_name,
    )
    print(f"Records with metadata: {records_buffer_with_metadata}")
    write_buffer_to_minio(
        connection_data,
        buffer=records_buffer_with_metadata,
        destination_bucket_name=bronze_bucket_name,
        destination_object_name=destination_object_name,
    )
    return

    # print(
    #     f"Files to be transformed from {bronze_bucket_name} to {silver_bucket_name}: {objects_to_be_transformed}"
    # )
    # # Aggregate all records from all JSON files
    # all_flattened_records = get_raw_files_from_minio(
    #     object_names=objects_to_be_transformed,
    #     source_bucket_name=source_bucket_name,
    # )

    # if all_flattened_records:
    #     out_buffer = get_parquet_buffer_from_flattened_records(all_flattened_records)
    #     destination_object_name = (
    #         f"{prefix}consolidated-{year}{month}{day}{hour}.parquet"
    #     )
    #     # write_processed_to_minio(
    #     #     buffer=out_buffer,
    #     #     destination_bucket_name=destination_bucket_name,
    #     #     destination_object_name=destination_object_name,
    #     # )
    # else:
    #     print("No records found to write to the destination bucket.")
    # elapsed = time.time() - start_time
    # print(f"Elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
