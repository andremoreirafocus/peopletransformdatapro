from minio import Minio
import io


def write_generic_bytes_to_minio(connection_data, buffer, bucket_name, object_name):
    """
    Writes a io bytes buffer (such as from Pandas) to MinIO at the specified bucket and object name.
    :param connection data: MinIO connection data
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :return: void
    """
    client = Minio(
        connection_data["minio_endpoint"],
        access_key=connection_data["access_key"],
        secret_key=connection_data["secret_key"],
        secure=connection_data["secure"],
    )

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    if isinstance(buffer, bytes):
        data_stream = io.BytesIO(buffer)
        data_length = len(buffer)
    elif isinstance(buffer, io.BytesIO):
        data_stream = buffer
        # Go to start of stream if it has been read before
        data_stream.seek(0)
        data_length = buffer.getbuffer().nbytes
    else:
        raise ValueError("Buffer must be either bytes or io.BytesIO")
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=data_length,
        content_type="application/octet-stream",
    )
    print(f"Consolidated file uploaded to bucket '{bucket_name}' as '{object_name}'")
