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
    protocol = "local"
    if pattern.startswith("s3://"):
        res = s3.get_s3_args(pattern)
        pattern = res["urlpath"]
        endpoint_url = res["endpoint_url"]
        fs = fsspec.filesystem("s3", endpoint_url=endpoint_url)
        files = fs.glob(pattern)
        files = [
            endpoint_url.replace("https://", "s3://") + f"/{file}" for file in files
        ]
        return files
    elif pattern.startswith("az://") or pattern.startswith("abfs://"):
        protocol = "abfs://" if pattern.startswith("abfs://") else "az://"
        # format: az://<container>@<account>.dfs.core.windows.net/<path>
        sa = pattern.replace(protocol, "").split("@")[1].split(".")[0]
        container = pattern.replace(protocol, "").split("@")[0]
        blob = "/".join(pattern.replace(protocol, "").split("@")[1].split("/")[1:])
        pattern = f"{container}/{blob}"
        fs = fsspec.filesystem("abfs", account_name=sa)
        files = fs.glob(pattern)
        files = [
            f"{protocol}{container}@{sa}.dfs.core.windows.net/{'/'.join(file.split('/')[1:])}"
            for file in files
        ]
        return files
    else:
        fs = fsspec.filesystem("file")
        files = fs.glob(pattern)
        return files
