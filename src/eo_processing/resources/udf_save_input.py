"""Save an openEO UDF input chunk to temporary object storage."""

import logging
from datetime import datetime
from uuid import uuid4

import boto3
import numpy as np
import xarray as xr
from openeo.udf import inspect
from rasterio.io import MemoryFile

logger = logging.getLogger(__name__)


def _s3_client(context: dict):
    return boto3.client(
        "s3",
        aws_access_key_id=context.get("access_key_id"),
        aws_secret_access_key=context.get("secret_access_key"),
        aws_session_token=context.get("token"),
        endpoint_url=context.get("endpoint"),
    )


def _build_object_key(cube: xr.DataArray, context: dict, extension: str) -> str:
    title = context.get("title") or context.get("name") or "input"
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    short_id = uuid4().hex[:6]

    coord_tag: str | None = None
    if "x" in cube.coords and "y" in cube.coords:
        try:
            coord_tag = f"x{int(cube.x.values[0])}_y{int(cube.y.values[0])}"
        except (TypeError, ValueError, IndexError):
            pass

    parts = [title]
    if coord_tag:
        parts.append(coord_tag)
    parts.extend([stamp, short_id])

    prefix = context.get("prefix", "").strip("/")
    filename = "_".join(parts) + extension
    return f"{prefix}/{filename}" if prefix else filename


def _serialize_capture(cube: xr.DataArray, output_format: str) -> tuple[bytes, str, str]:
    if output_format == "netcdf":
        dataset = (
            cube.to_dataset(dim="bands")
            if "bands" in cube.dims
            else cube.to_dataset(name=cube.name or "data")
        )
        return bytes(dataset.to_netcdf()), ".nc", "application/netcdf"

    if output_format == "gtiff":
        if "t" in cube.dims:
            return _serialize_capture(cube, "netcdf")
        if "y" not in cube.dims or "x" not in cube.dims:
            raise ValueError("GeoTIFF capture requires y and x dimensions")

        data = np.asarray(cube.values)
        if data.ndim == 2:
            data = data[np.newaxis, :, :]
        elif data.ndim != 3:
            raise ValueError("GeoTIFF capture requires a 2D or 3D spatial input cube")

        with MemoryFile() as memory_file:
            with memory_file.open(
                driver="GTiff",
                height=data.shape[-2],
                width=data.shape[-1],
                count=data.shape[0],
                dtype=data.dtype,
            ) as dataset:
                dataset.write(data)
            return memory_file.read(), ".tif", "image/tiff"

    raise ValueError("output_format must be 'netcdf' or 'gtiff'")


def apply_datacube(cube: xr.DataArray, context: dict) -> xr.DataArray:
    """Upload UDF input, then return it unchanged."""
    bucket = context.get("bucket")
    if not bucket:
        raise ValueError("Context must contain bucket")

    desired_dims = [dimension for dimension in ["t", "bands", "y", "x"] if dimension in cube.dims]
    capture_cube = cube.transpose(*desired_dims).load()
    output_format = context.get("output_format", "netcdf").lower()
    payload, extension, content_type = _serialize_capture(capture_cube, output_format)
    key = _build_object_key(capture_cube, context, extension)
    client = _s3_client(context)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType=content_type,
    )

    presigned_url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=int(context.get("presign_expires", 12 * 3600)),
    )
    inspect(message=f"uploaded to s3://{bucket}/{key}; presigned download: {presigned_url}")
    logger.info("uploaded to s3://%s/%s; presigned download: %s", bucket, key, presigned_url)
    return cube