"""Build context for UDF input captures stored in CDSE object storage."""

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import boto3
import openeo
import requests

from eo_processing.utils.helper import getUDFpath

_CAPTURE_URL_PATTERN = re.compile(r"presigned download:\s*(https?://\S+)")


def _build_s3_context(
    connection: openeo.Connection,
    region: str = "waw3-1",
    role_session_name: str = "openeo-udf",
    duration_hours: int = 12,
) -> dict[str, str]:
    """Build temporary object-storage credentials from an openEO OIDC token."""
    if duration_hours <= 0:
        raise ValueError("duration_hours must be positive")

    auth = connection.auth
    try:
        web_identity_token = auth.bearer.split("/", maxsplit=2)[2] if auth else None
    except (AttributeError, IndexError) as error:
        raise ValueError("Connection must use an OIDC bearer token") from error
    if not web_identity_token:
        raise ValueError("Connection must use an OIDC bearer token")

    sts_url = f"https://sts.{region}.openeo.v1.dataspace.copernicus.eu"
    s3_url = sts_url.replace("sts", "s3", 1)
    role_arn = f"arn:openeo:iam:::role/openeo-artifacts-{region}"

    response: dict[str, Any] = boto3.Session().client(
        "sts", endpoint_url=sts_url
    ).assume_role_with_web_identity(
        RoleArn=role_arn,
        RoleSessionName=role_session_name,
        WebIdentityToken=web_identity_token,
        DurationSeconds=duration_hours * 3600,
    )
    credentials = response["Credentials"]
    subject = response["SubjectFromWebIdentityToken"]
    subject_hash = hashlib.sha1(subject.encode()).hexdigest()
    prefix = f"{subject_hash}/{datetime.now(timezone.utc):%Y/%m/%d}/"

    return {
        "bucket": f"openeo-artifacts-{region}",
        "prefix": prefix,
        "endpoint": s3_url,
        "token": credentials["SessionToken"],
        "access_key_id": credentials["AccessKeyId"],
        "secret_access_key": credentials["SecretAccessKey"],
    }


def get_capture_udf(
    connection: openeo.Connection,
    *,
    output_format: str = "netcdf",
    region: str = "waw3-1",
    role_session_name: str = "openeo-udf",
    duration_hours: int = 12,
) -> openeo.UDF:
    """Create an input-capture UDF with temporary object-storage credentials as context."""
    output_format = output_format.lower()
    if output_format not in {"netcdf", "gtiff"}:
        raise ValueError("output_format must be 'netcdf' or 'gtiff'")

    return openeo.UDF.from_file(
        getUDFpath("udf_save_input.py"),
        context={
            **_build_s3_context(
            connection,
            region=region,
            role_session_name=role_session_name,
            duration_hours=duration_hours,
            ),
            "output_format": output_format,
        },
        runtime="Python",
        version="3.11",
    )


def download_captured_udf_inputs(
    connection: openeo.Connection,
    job: openeo.BatchJob | str,
    folder: str | Path,
    *,
    limit: int | None = None,
) -> list[Path]:
    """Download UDF inputs whose presigned URLs were emitted in batch-job logs.

    ``job`` can be an openEO batch job or its ID. URLs are read from the
    ``presigned download:`` messages emitted by ``udf_save_input.py``.
    """
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    job = connection.job(job) if isinstance(job, str) else job
    urls: list[str] = []
    for entry in job.logs(level="INFO"):
        message = entry.get("message", "")
        match = _CAPTURE_URL_PATTERN.search(message)
        if match and match.group(1) not in urls:
            urls.append(match.group(1))

    if limit is not None:
        urls = urls[:limit]

    output_folder = Path(folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    downloads: list[Path] = []
    for url in urls:
        filename = Path(unquote(urlparse(url).path)).name
        if not filename:
            raise ValueError(f"Capture URL has no filename: {url}")
        destination = output_folder / filename
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                file.write(chunk)
        downloads.append(destination)

    return downloads