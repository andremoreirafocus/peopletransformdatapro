from tasks.load.services.generate_rawdatatable_from_staging_files import (
    generate_rawdatatable_from_staging_files,
)
from connections.get_minio_connection_data import get_minio_connection_data
from infra.storage.list_objects_in_minio_bucket import list_objects_in_minio_bucket
from infra.storage.write_generic_bytes_to_minio import write_generic_bytes_to_minio


def load(year, month, day, hour):
    # Ingestion from staging to bronze
    source_bucket_name = "staging"
    bronze_bucket_name = "bronze"
    app_folder = "people"
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
