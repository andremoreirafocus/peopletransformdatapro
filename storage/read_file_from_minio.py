from minio import Minio


def read_file_from_minio(connection_data, bucket_name, object_name):
    """
    Reads a file from MinIO and returns its contents as a string.
    :param connection data: MinIO connection data
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :return: file content as a string
    """
    try:
        client = Minio(
            connection_data["minio_endpoint"],
            access_key=connection_data["access_key"],
            secret_key=connection_data["secret_key"],
            secure=connection_data["secure"],
        )
        response = client.get_object(bucket_name, object_name)
        content = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return content
    except Exception as e:
        print(f"Error reading JSON from MinIO: {e}")
        return None
