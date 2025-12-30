from minio import Minio


def list_objects_in_minio_bucket(
    connection_data,
    bucket_name,
    prefix,
):
    """
    Lists files in a MinIO folder (prefix).
    :param bucket_name: MinIO bucket name
    :param prefix: Folder path (prefix) in the bucket
    :param connection_data: MinIO connection data
    :return: List of file object names
    """

    try:
        client = Minio(
            connection_data["minio_endpoint"],
            access_key=connection_data["access_key"],
            secret_key=connection_data["secret_key"],
            secure=connection_data["secure"],
        )
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except Exception as e:
        print(f"Error listing files in MinIO folder: {e}")
        return []
