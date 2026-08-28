"""Module to download alphaEarth embeddings for a given AOI and upload them to S3.

Some snippets extracted from Marcel's code.

This module provides functions to:
1. Identify intersecting grids from an AOI and a global grid.
2. Download the corresponding alphaEarth embeddings from S3 or HTTP.
3. Convert the downloaded VRT files to Cloud Optimized GeoTIFF (COG)
4. Upload the processed files to S3 using a specified storage utility.

Example usage:

    # get the intersecting grids and filenames
    df = get_embedding_filenames(
        "/vitodata/nca/weed/change/AOI/AOI_cze-change_UTM20k_grid.shp",
        "/data/mepvm/alphaearth/aef_source-coop_2017-2025_index.gpkg",
        '/vitodata/nca/weed/grid/global_terrestrial_UTM20k_grid_v2.gpkg',  # not needed if input is already in this grid
        years=[2022, 2023],
    )
    # download files to local directory
    downloaded_files = download_embeddings(
        df["path"].tolist(), "/vitodata/nca/test/Manu/alphaearth_embeddings/"
    )
    # edit the vrt and then convert to cog
    vrt_paths = [Path(f).with_suffix(".vrt") for f in downloaded_files]
    cog_paths = []
    for path_vrt in vrt_paths:
        path_vrt: Path = Path(path_vrt)
        patch_vrt_relative_path(path_vrt)
        path_tif = translate_to_cog(path_vrt)
        cog_paths.append(path_tif)

    # Upload to S3
    files_df = build_files_dataframe(cog_paths)
    storage = WEED_storage(...)   # Initialize with appropriate parameters)
    upload_files(files_df, storage, S3_ROOT)
    print('All done.')

"""

import logging
import re
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

import geopandas as gpd
import pandas as pd
from eo_processing.utils.storage import WEED_storage


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
    from botocore.exceptions import ClientError

    boto3_available = True
except ImportError:
    logger.warning(
        "boto3 not available, falling back to HTTP protocol for downloading."
    )
    boto3_available = False


# download settings
S3_BASE_URL = "s3://us-west-2.opendata.source.coop/"
ENDPOINT_URL = "https://data.source.coop"
# COG settings
GDAL_COG_OPTIONS = [
    "-of COG",
    "-co INTERLEAVE=BAND",
    "-co COMPRESS=ZSTD",
    "-co LEVEL=15",
    "-co NUM_THREADS=ALL_CPUS",
    "-co PREDICTOR=YES",
    "-co OVERVIEW_RESAMPLING=NEAREST",
    "-co BIGTIFF=YES",
    "-co OVERVIEW_COUNT=6",
]


class S3Downloader:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            config=Config(signature_version=UNSIGNED),
        )

    def s3_object_exists(self, bucket, key):
        """
        Check if an S3 object exists using unsigned requests.

        Args:
            bucket (str): S3 bucket name.
            key (str): S3 object key.
            endpoint_url (str): Custom S3 endpoint URL.

        Returns:
            bool: True if object exists, False otherwise.
        """
        try:
            logger.debug(f"Checking existence of s3://{bucket}/{key}")
            self.s3_client.head_object(Bucket=bucket, Key=key)
            logger.info(f"Object exists: s3://{bucket}/{key}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning(f"Object does not exist: s3://{bucket}/{key}")
                return False
            else:
                logger.error(f"Error checking object existence: {e}")
                raise

    def get_bucket_and_key(self, s3_url: str) -> Tuple[str, str]:
        """
        Parse an S3 URL into its bucket name and object key.

        Args:
            s3_url (str): The full S3 URL to parse.
        """
        split_str = s3_url.replace(S3_BASE_URL, "").split("/", 1)
        return split_str[0], split_str[1]

    def download_s3_file(self, s3_url, output_dir):
        """
        Download a single file from S3 using unsigned requests.

        Args:
            bucket (str): S3 bucket name.
            key (str): S3 object key.
            endpoint_url (str): Custom S3 endpoint URL.
            output_dir (str or Path): Local directory to save the object.
        """
        bucket, key = self.get_bucket_and_key(s3_url)
        key1 = key.replace(".tiff", ".vrt")
        if not self.s3_object_exists(bucket, key):
            logger.error(f"Object does not exist: s3://{bucket}/{key}")
            return False, None
        if not self.s3_object_exists(bucket, key1):
            logger.warning(f"Object does not exist: s3://{bucket}/{key1}")

        try:
            output_path = Path(output_dir) / Path(key)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(bucket, key, str(output_path))
            logger.info(f"Download complete: {output_path}")
            output_path1 = Path(output_dir) / Path(key1)
            self.s3_client.download_file(bucket, key1, str(output_path1))
            logger.info(f"Download complete: {output_path1}")
            return True, str(output_path)
        except Exception as e:
            logger.error(f"Failed to download s3://{bucket}/{key}: {e}")
            return False, None


def http_download(
    filenames: List[str], output_dir: Path, overwrite: bool = False
) -> List[str]:
    """
    Download files using HTTP protocol.

    Args:
        filenames (List[str]): List of file URLs to download.
        output_dir (Path): Directory to save downloaded files.
        overwrite (bool): If True, overwrite existing files.

    Returns:
        List[str]: List of paths to downloaded files.
    """
    import requests

    new_paths = []
    for fl in filenames:
        fl_path = Path(fl.replace(S3_BASE_URL, ""))
        final_dir = output_dir / fl_path.parent
        final_dir.mkdir(parents=True, exist_ok=True)
        url = fl.replace(S3_BASE_URL, "https://data.source.coop/")
        out_path = final_dir / fl_path.name
        if out_path.exists() and not overwrite:
            logger.info(f"File {out_path} exists, skipping download.")
            new_paths.append(str(out_path))
            continue
        logger.info(f"Downloading {url} to {final_dir}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        new_paths.append(str(out_path))
    return new_paths


def _ensure_crs(df: gpd.GeoDataFrame, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """
    Ensure the GeoDataFrame has the specified CRS.

    Args:
        df (gpd.GeoDataFrame): Input GeoDataFrame.
        crs (str): Target CRS.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame in the target CRS.
    """
    if df.crs is None or df.crs != crs:
        return df.to_crs(crs)
    return df


def get_intersecting_grids(
    input_gdf: gpd.GeoDataFrame, grid_file: str
) -> gpd.GeoDataFrame:
    """
    Intersect the input_gdf with the grid_file and return the intersecting grids.

    Args:
        input_gdf (gpd.GeoDataFrame): Input geometries.
        grid_file (str): Path to grid file.

    Returns:
        gpd.GeoDataFrame: Intersecting grid geometries.
    """
    grid_gdf = gpd.read_file(grid_file)
    input_union = input_gdf.union_all()
    possible_matches_index = list(grid_gdf.sindex.intersection(input_union.bounds))
    possible_matches = grid_gdf.iloc[possible_matches_index]
    intersecting = possible_matches[possible_matches.intersects(input_union)]
    return intersecting


def get_embedding_filenames(
    input_shape_filename: str,
    alphaearth_metadata_file: str,
    format_grid_file: Optional[str] = None,
    years: Optional[List[int]] = None,
) -> gpd.GeoDataFrame:
    """
    Get the intersecting grids from the input shape file and return the corresponding alphaearth embeddings.

    Args:
        input_shape_filename (str): Path to the input shape file. Can be .shp, .gpkg, .geojson, or .parquet.
        alphaearth_metadata_file (str): Path to the alphaearth metadata file.
        format_grid_file (Optional[str]): Path to the global grid file. If provided, filter grids.
        years (Optional[List[int]]): List of years to filter the embeddings.

    Returns:
        gpd.GeoDataFrame: Intersecting grids and their corresponding alphaearth embeddings paths.
    """
    input_file = Path(input_shape_filename)
    if not input_file.exists():
        logger.error(f"Input shape file {input_shape_filename} does not exist.")
        raise FileNotFoundError(
            f"Input shape file {input_shape_filename} does not exist."
        )

    if input_file.suffix in [".shp", ".gpkg", ".geojson"]:
        input_gdf = gpd.read_file(input_file)
    elif input_file.suffix == ".parquet":
        input_gdf = gpd.read_parquet(input_file)
    else:
        logger.error(
            f"Input shape file {input_shape_filename} has unsupported file type {input_file.suffix}. "
            "Supported types are .shp, .gpkg, .geojson, .parquet"
        )
        raise ValueError(
            f"Input shape file {input_shape_filename} has unsupported file type {input_file.suffix}. "
            "Supported types are .shp, .gpkg, .geojson, .parquet"
        )
    input_gdf = _ensure_crs(input_gdf)

    if format_grid_file is not None:
        if not Path(format_grid_file).exists():
            logger.error(f"Global grid file {format_grid_file} does not exist.")
            raise FileNotFoundError(
                f"Global grid file {format_grid_file} does not exist."
            )
        grids_20km = get_intersecting_grids(input_gdf, format_grid_file)
    else:
        grids_20km = input_gdf

    if not Path(alphaearth_metadata_file).exists():
        logger.error(
            f"AlphaEarth metadata file {alphaearth_metadata_file} does not exist."
        )
        raise FileNotFoundError(
            f"AlphaEarth metadata file {alphaearth_metadata_file} does not exist."
        )
    alphaearth_grids = get_intersecting_grids(grids_20km, alphaearth_metadata_file)
    if years:
        alphaearth_grids = alphaearth_grids[alphaearth_grids["year"].isin(years)]
    else:
        logger.info("No years specified, returning all intersecting grids.")
    return alphaearth_grids


def download_embeddings(
    filenames: List[str], output_path: str, overwrite: bool = False
) -> List[str]:
    """
    Download the embeddings for the intersecting grids from sourcecoop or HTTP and save them to the output directory.

    Args:
        filenames (List[str]): List of S3 paths to the embeddings.
        output_path (str): Path to the output directory where the embeddings will be downloaded.
        overwrite (bool): If True, download and overwrite files even if they exist.

    Returns:
        List[str]: List of paths to downloaded files.
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {len(filenames)} files to {output_dir}")
    new_paths = []
    successful_downloads = 0
    if boto3_available:
        logger.info("Using S3 for downloading files.")
        client = S3Downloader()
        for fl in filenames:
            success, new_path = client.download_s3_file(fl, output_dir)
            if success:
                new_paths.append(new_path)
                successful_downloads += 1
        logger.info(
            f"Downloaded {len(new_paths)} files to {output_dir} out of {len(filenames)}"
        )
    else:
        logger.info("Using HTTP protocol for downloading files.")
        new_paths = http_download(filenames, output_dir, overwrite=overwrite)
        logger.info(f"Downloaded {len(new_paths)} files to {output_dir}")
    return new_paths


def patch_vrt_relative_path(path_vrt: Path) -> None:
    """
    Modifies the VRT file to set the 'relativeToVRT' attribute for the source dataset to 1.

    This function reads the content of the specified VRT file, locates the SourceDataset
    element whose 'relativeToVRT' attribute is set to 0 and whose filename matches the
    generated filename, updates its 'relativeToVRT' attribute to 1, and writes the modified
    content back to the same file.

    :param path_vrt: A Path object representing the path to the VRT file to be modified.
    """
    filename = path_vrt.stem + ".tiff"
    vrt_content = path_vrt.read_text(encoding="utf-8")
    vrt_content = re.sub(
        r'<SourceDataset relativeToVRT="0">[^<]*'
        + re.escape(filename)
        + r"</SourceDataset>",
        f'<SourceDataset relativeToVRT="1">{filename}</SourceDataset>',
        vrt_content,
    )
    path_vrt.write_text(vrt_content, encoding="utf-8")


def translate_to_cog(path_vrt: Path, version):
    """Translates a VRT (Virtual Dataset) file to a Cloud Optimized GeoTIFF (COG) format using GDAL.

        This function converts a given VRT file into a COG-compatible GeoTIFF file. If the output
        file already exists, the function will skip the translation process and return the existing
        file path.

        Args:
            path_vrt (Path): The path to the input VRT file.
            version (str): Version string to append to the output filename.


    urn:    The path to the generated Cloud Optimized GeoTIFF file.    :return: The path to the generated Cloud Optimized GeoTIFF file.
    """
    path_out = path_vrt.parent / f"{path_vrt.stem}_{version}.tif"

    if path_out.exists():
        print(f"File {path_out} already exists, skipping.")
        return path_out

    gdal_cmd = " ".join(
        ["gdal_translate"] + GDAL_COG_OPTIONS + [str(path_vrt), str(path_out)]
    )
    try:
        subprocess.check_call(gdal_cmd, shell=True)
    except subprocess.CalledProcessError as e:
        raise OSError(f"Could not translate file: {e}") from e
    return path_out


def build_files_dataframe(files: list[Path]) -> pd.DataFrame:
    """
    Builds a DataFrame from a list of file paths, extracting relevant metadata from their parent directories.

    Args:
        files (list[Path]): List of file paths to include in the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing file paths and extracted metadata, including 'version', 'year', 'zone', and 's3_prefix'.
    """
    files_df = pd.DataFrame({"file_path": files})
    # Extract parent parts
    parent_parts = (
        files_df["file_path"]
        .apply(lambda x: Path(x).parent.parts[-5:])
        .apply(pd.Series)
    )
    # Join extracted parts as new columns
    files_df = files_df.join(
        parent_parts.rename(
            columns={0: "rest", 1: "version", 2: "_unused", 3: "year", 4: "zone"}
        )
    )
    files_df = files_df.drop(columns=["rest", "_unused"])
    files_df["s3_prefix"] = files_df[["version", "year", "zone"]].apply(
        lambda x: "/".join(x), axis=1
    )
    return files_df


def upload_files(files_df: pd.DataFrame, storage: WEED_storage, s3_root: str) -> None:
    """
    Uploads files to an S3-compatible storage using the specified storage utility. Each file's destination
    is determined from a DataFrame containing file paths and their respective S3 prefixes. This function
    also supports options such as progress tracking, ETag validation, and existence checks for each upload.

    :param files_df: A DataFrame where each row contains details of a file to upload. Must include columns
        for 'file_path' specifying the local path of the file and 's3_prefix' defining the relative destination
        path in the S3-compatible storage.
    :param storage: An instance of WEED_storage, which provides methods for uploading files to
        S3-compatible storage backends.
    :param s3_root: The root directory in the target S3 storage where files will be uploaded. The
        final S3 destination of each file is formed by concatenating this root with the 's3_prefix'.
    """
    for file_row in files_df.itertuples():
        s3_destination = f"{s3_root}/{file_row.s3_prefix}"
        logger.info(f"Uploading {file_row.file_path} to {s3_destination}")
        storage.upload_file_to_s3(
            file_row.file_path,
            s3_destination,
            progress_bar=True,
            etag_check=True,
            exist_check=True,
        )


# def filter_files_on_s3(storage: WEED_storage, s3_root: str, df: pd.DataFrame) -> pd.DataFrame:
#     """Get all files in the S3 bucket in a directory."""
#     list_all = storage.get_s3_content(s3_root)
#     all_files = [f["Key"] for f in list_all if f["Key"].endswith(".tif")]
#     result = [
#         {"year": f.split("/")[1], "zone": f.split("/")[2], "filename": f.split("/")[3].split("_")[0]}
#         for f in all_files
#     ]
#     df_exist= pd.DataFrame(result)
    
#     # drop existing files on S3
#     # Create a boolean mask for rows to drop
#     def should_drop(row):
#         for rw in df_exist.itertuples(index=False):
#             if int(row['year']) == int(rw.year) and rw.filename in row['path']:
#                 return True
#         return False
#     mask = df.apply(should_drop, axis=1)
#     df_filtered = df[~mask].reset_index(drop=True)
#     return df_filtered


def filter_files_on_s3(storage: WEED_storage, s3_root: str, df: pd.DataFrame) -> pd.DataFrame:
    """Get all files in the S3 bucket in a directory and filter out existing ones."""
    list_all = storage.get_s3_content(s3_root)
    all_files = [f["Key"] for f in list_all if f["Key"].endswith(".tif")]
    # Build a list of dicts as before
    result = [
        {"year": f.split("/")[1], "zone": f.split("/")[2], "filename": f.split("/")[3].split("_")[0]}
        for f in all_files
    ]

    def should_drop(row):
        for rw in result:
            if int(row['year']) == int(rw['year']) and rw['filename'] in row['path']:
                return True
        return False

    mask = df.apply(should_drop, axis=1)
    df_filtered = df[~mask].reset_index(drop=True)
    return df_filtered
