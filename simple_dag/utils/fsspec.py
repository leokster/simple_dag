import functools
import fsspec
from simple_dag.utils import s3


@functools.wraps(fsspec.open)
def open(*args, **kwargs):
    urlpath = args[0]
    if urlpath.startswith("s3://"):
        res = s3.get_s3_args(urlpath)
        urlpath = res["urlpath"]
        kwargs["endpoint_url"] = res["endpoint_url"]

    args = (urlpath,) + args[1:]
    return fsspec.open(*args, **kwargs)


def list_files(pattern):
    # Define your wildcard pattern for S3
    if pattern.startswith("s3://"):
        res = s3.get_s3_args(pattern)
        pattern = res["urlpath"]
        endpoint_url = res["endpoint_url"]
        fs = fsspec.filesystem("s3", endpoint_url=endpoint_url)
    else:
        fs = fsspec.filesystem("file")

    # List all files matching the pattern on S3
    files = fs.glob(pattern)

    if pattern.startswith("s3://"):
        files = [
            endpoint_url.replace("https://", "s3://") + f"/{file}" for file in files
        ]
    return files
