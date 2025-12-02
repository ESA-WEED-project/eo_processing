import json
import subprocess
from pathlib import Path

import openeo
import pytest
from rio_cogeo.cogeo import cog_validate
from tests.conftest import INTEGRATION_JOB_OPTIONS


INTEGRATION_TESTS = (
    {  # Dict that maps the integration tests to the number of expected assets # credits for 2x2 km area
        "extract_S1_integration.json": 176, # 7 credits
        "extract_S2_integration.json": 100, # 7 credits
        "ts_datacube_extraction_combined_integration.json": 228, # 13 credits
        "ts_datacube_extraction_S2_integration.json": 100, # 13 credits
        "generate_master_feature_cube_with_catboost_inference_integration.json": 1, # 28 credits
        "generate_S1_feature_cube_integration.json": 1, # 12 credits
        "generate_S2_feature_cube_integration.json": 1, # 15 credits
    }
)


def _is_integration_pg(pg_path: Path) -> bool:
    return pg_path.name in INTEGRATION_TESTS.keys()


def changed_process_graphs():
    """
    Get the list of changed process graphs in the current branch compared to origin/main.

    it uses the git diff command to get the list of changed files and filters them to only include
    .json files in the tests/resources directory that are marked as integration process graphs.

    New process graphs should be added to the tests/resources and added to the
    git tracking.

    Inspired by Integration testing in https://github.com/VITO-RS-Vegetation/lcfm-production.
    """
    try:
        # Get the list of changed .json files
        result = subprocess.run(
            ["git", "diff", "origin/main", "--name-only"],
            stdout=subprocess.PIPE,
            check=True,
            text=True,
        )
        changed_files = [Path(f) for f in result.stdout.splitlines()]
        print(f"Changed integration process graphs: {changed_files}")
        base_uri = Path("tests/resources").absolute().as_uri()
        json_files = [
            f for f in changed_files
            if f.suffix == ".json"
            and f.absolute().as_uri().startswith(base_uri)
            and _is_integration_pg(f)
        ]
        print(f"Changed integration process graphs: {json_files}")
        return json_files
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e}")
        return []


def _is_integration_pg(pg_path: Path) -> bool:
    return pg_path.name in INTEGRATION_TESTS.keys()


@pytest.mark.integration
@pytest.mark.parametrize(
    "pg_path, integration",
    [(pg, True) for pg in changed_process_graphs()],
    ids=[x.name for x in changed_process_graphs()],
)
def test_process_graph_integration(pg_path: Path, integration):
    """
    Tests the changed process graphs marked with integration.

    This test has been marked as integration and will only run if you run pytest with the
    --integration flag.

    Inspired by Integration testing in https://github.com/VITO-RS-Vegetation/lcfm-production.
    """
    print(f'Benchmarking: {integration}')
    # Make connection to CDSE
    con_cdse = openeo.connect("openeo.dataspace.copernicus.eu").authenticate_oidc()

    # Load job info from process graph path
    with open(pg_path, "r") as f:
        job_info = json.load(f)

    # Create job
    job = con_cdse.create_job(
        process_graph=job_info["process_graph"],
        title=f"WEED eo-processing integration test {pg_path.stem}",
        description=job_info["description"],
        job_options=INTEGRATION_JOB_OPTIONS,
    )

    # Run job & assert if it works
    job.start_and_wait()

    assert (
        job.status() == "finished"
    ), f"Job {job.job_id} failed with status {job.status()}"

    job_results = job.get_results()

    # Check the number of assets
    assert (
        len(job_results.get_assets()) == INTEGRATION_TESTS[pg_path.name]
    ), f"Job {job.job_id} returned {len(job_results.get_assets())} assets, expected {INTEGRATION_TESTS[pg_path.name]}"

    # Check if the assets are valid COGs
    for asset in job_results.get_assets():
        href = asset.href
        if href.split(".")[-1] in ["tif", "tiff"]:
            is_valid, errors, warnings = cog_validate(href, quiet=True)
            assert (
                is_valid
            ), f"File {href} is not a valid COG. Errors: {errors}, Warnings: {warnings}"
