import s3fs


def get_s3_connection(key, secret):
    return s3fs.S3FileSystem(
        key=key,
        secret=secret,
        client_kwargs={
            "endpoint_url": "http://localhost:9000",
        },
    )
