import json
from minio import Minio
from services.get_minio_connection_data import get_minio_connection_data
# from datetime import datetime


def generate_rawdatatable_from_staging_files(
    connection_data, object_names, source_bucket_name, partial_ts
):
    """
    Reads and flattens all JSON files from MinIO, returning a list of flattened records.
    """
    source_system = "people_app"
    raw_records_with_metadata = []
    records_buffer = ""
    for object_name in object_names:
        print(f"Reading JSON from MinIO: {object_name}")
        json_pos = object_name.find(".json")

        ts = partial_ts
        ts["minute"] = object_name[json_pos - 4 : json_pos - 2]
        ts["second"] = object_name[json_pos - 2 : json_pos]
        json_str = get_file_from_minio(
            connection_data, bucket_name=source_bucket_name, object_name=object_name
        )
        print(f"Original JSON string: {json_str}")
        ingest_ts = f"{ts['year']}-{ts['month']}-{ts['day']}T{ts['hour']}:{ts['minute']}:{ts['second']}.000Z"
        if json_str:
            envelope = {
                "_meta": {
                    # "ingest_ts": datetime.now().isoformat() + "Z",
                    "ingest_ts": ingest_ts,
                    # "dag_run_id": run_id,
                    # "task_id": task_id,
                    "source_system": source_system,
                },
                "payload": json.loads(json_str),  # Original JSON as escaped string
            }
            # print(f"ENVELOPE: {envelope}")
            single_line_json = json.dumps(envelope)
            # print(f"JSON Line: {single_line_json}")
            raw_records_with_metadata.append(single_line_json)
    records_buffer = "\n".join(raw_records_with_metadata)
    # print(f"Records buffer: {records_buffer}")
    return records_buffer.encode("utf-8")


def get_file_from_minio(connection_data, bucket_name, object_name):
    """
    Reads a JSON file from MinIO and returns its contents as a string.
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :param minio_endpoint: MinIO server endpoint
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    :return: JSON file contents as a string
    """
    try:
        client = get_minio_connection_data()
        client = Minio(
            connection_data["minio_endpoint"],
            access_key=connection_data["access_key"],
            secret_key=connection_data["secret_key"],
            secure=connection_data["secure"],
        )

        response = client.get_object(bucket_name, object_name)
        json_str = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return json_str
    except Exception as e:
        print(f"Error reading JSON from MinIO: {e}")
        return None


def flatten_json_string(json_str, sep="_"):
    """
    Flattens a nested JSON string to a one-level dictionary.
    :param json_str: JSON string (object)
    :param sep: Separator for nested keys (default: '.')
    :return: Flattened dictionary
    """

    def _flatten(obj, parent_key="", sep=sep):
        items = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                if "picture" in k or "info" in k or "postcode" in k:
                    continue  # Skip keys containing 'picture'
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                items.update(_flatten(v, new_key, sep=sep))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                items.update(_flatten(v, new_key, sep=sep))
        else:
            items[parent_key] = obj
        return items

    try:
        data = json.loads(json_str)
        return _flatten(data)
    except Exception as e:
        print(f"Error flattening JSON string: {e}")
        return None
