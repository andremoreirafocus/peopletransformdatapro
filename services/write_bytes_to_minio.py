from minio import Minio
import io


def write_bytes_to_minio(
    connection_data, bytes_buffer, destination_bucket_name, destination_object_name
):
    """
    Writes a bytes_buffer to MinIO at the specified bucket and object name.
    """
    client = Minio(
        connection_data["minio_endpoint"],
        access_key=connection_data["access_key"],
        secret_key=connection_data["secret_key"],
        secure=connection_data["secure"],
    )

    if not client.bucket_exists(destination_bucket_name):
        client.make_bucket(destination_bucket_name)

    client.put_object(
        bucket_name=destination_bucket_name,
        object_name=destination_object_name,
        data=io.BytesIO(bytes_buffer),
        length=len(bytes_buffer),
        content_type="application/octet-stream",
    )
    print(
        f"Consolidated file uploaded to bucket '{destination_bucket_name}' as '{destination_object_name}'"
    )
