def get_s3_args(url):
    if not url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL: {url}")

    # split the url
    endpoint_url = "https://" + url.split("/")[2]
    urlpath = "s3://" + "/".join(url.split("/")[3:])
    return {"urlpath": urlpath, "endpoint_url": endpoint_url}
