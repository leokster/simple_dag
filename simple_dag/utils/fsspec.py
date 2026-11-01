import functools

import fsspec
from fsspec.core import url_to_fs

from simple_dag import storage
from simple_dag.utils import s3


@functools.wraps(fsspec.open)
def open(*args, **kwargs):
    urlpath = args[0]
    mode = kwargs.get("mode", args[1] if len(args) > 1 else "rb")
    for key, value in storage.storage_options_for(urlpath, mode).items():
        kwargs.setdefault(key, value)
    urlpath = storage.resolve(urlpath)
    if urlpath.startswith("s3://"):
        res = s3.get_s3_args(urlpath)
        urlpath = res["urlpath"]
        kwargs["endpoint_url"] = res["endpoint_url"]

    args = (urlpath,) + args[1:]
    return fsspec.open(*args, **kwargs)


def list_files(pattern):
    """Expand a glob pattern.

    Patterns below the configured base return paths relative to the base, so
    the inputs built from them stay base-independent. Absolute patterns and
    URIs return absolute results as before.
    """
    if storage.is_below_base(pattern) and storage.is_relative(pattern):
        return _list_files_below_base(pattern)

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
            f"{protocol}{container}@{sa}.dfs.core.windows.net/"
            f"{'/'.join(file.split('/')[1:])}"
            for file in files
        ]
        return files
    else:
        fs = fsspec.filesystem("file")
        files = fs.glob(pattern)
        return files


def exists(path) -> bool:
    fs, fs_path = _filesystem_and_path(path)
    return fs.exists(fs_path)


def isdir(path) -> bool:
    fs, fs_path = _filesystem_and_path(path)
    return fs.isdir(fs_path)


def remove(path) -> None:
    """Delete a file; a missing file is not an error."""
    fs, fs_path = _filesystem_and_path(path, mode="w")
    if fs.exists(fs_path):
        fs.rm(fs_path)


def _list_files_below_base(pattern):
    base = storage.get_base()
    fs, root = url_to_fs(base, **storage.storage_options_for(pattern))
    root = root.rstrip("/")
    pattern_path = fs._strip_protocol(storage.resolve(pattern))
    matches = sorted(fs.glob(pattern_path))
    prefix = root + "/"
    return [
        match[len(prefix) :] if match.startswith(prefix) else match for match in matches
    ]


def _filesystem_and_path(path, mode="r"):
    return url_to_fs(storage.resolve(path), **storage.storage_options_for(path, mode))
