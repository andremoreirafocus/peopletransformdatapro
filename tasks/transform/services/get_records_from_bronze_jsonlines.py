import json
from infra.storage.read_file_from_minio import read_file_from_minio


def get_records_from_bronze_jsonlines(connection_data, object_name, source_bucket_name):
    """
    Reads and flattens all JSON files from MinIO, returning a list of flattened records.
    """
    all_flattened_records = []
    print(f"Reading JSON Lines file from MinIO: {object_name}")
    json_lines_raw_content = read_file_from_minio(
        connection_data, bucket_name=source_bucket_name, object_name=object_name
    )
    if json_lines_raw_content:
        json_lines = json_lines_raw_content.splitlines()
        try:
            for json_line in json_lines:
                data = json.loads(json_line)
                if isinstance(data, dict):
                    # If the dict has 'results', use the first result (legacy logic)
                    payload = data["payload"]
                    if "results" in payload and isinstance(payload["results"], list):
                        payload = [payload["results"][0]]
                    else:
                        payload = [payload]
                for record in payload:
                    flat = flatten_json_string(json.dumps(record))
                    if flat is not None:
                        all_flattened_records.append(flat)
        except Exception as e:
            print(f"Error processing {object_name}: {e}")
    return all_flattened_records


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
