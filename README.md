# eo_processing repository
repro for EO data processing pipelines in openEO

## WEED: Python development environment
import environment by using the yml file in the repository
  ```
  conda env create -f <path>/weed_python_environment.yml
  ```
and make sure the eo_processing is integrated via edible install
  - cd in the working folder of your repro (mostly in PyCharmsProject the name of the cloned repository) with terminal
  - run `conda activate weed`
  -	run `python -m pip install -e .`

more detailed instructions can be found here: [python_dev_environment](https://github.com/ESA-WEED-project/.github/tree/main/python_dev_environment)

## explanation on settings
By default, all required settings are automatically loaded for a chosen `provider` (`terrascope`, `creodias`, `cdse`). All default settings are set in `src/eo_processing/config/settings.py`. Settings are comprised of:
- `collection_options` set the standard EO data collections for processing
- `processing_options` that steer the methodological workflow for EO time series data extraction, VI generation & feature generation (temporal aggregation)
- `job_options` that steer the OpenEO job that runs the workflow

how to best define the `processing_options` in a call
- use pre-defined settings for `raw_extraction` (generation of raw reflectance/sigma naught time series cubes), `vi_generation` (time series cubes of the VI [plus raw data if requested]) or `feature_generation` via the task key-word in the `get_standard_processing_options` call
- use the `get_advanced_options` function and define all settings using the below list

Available `processing_options`

```
provider: str = None
        Sets the expected backend where the workflow should run ['terrascope', 'creodias'].
        This setting in turn has impact on several downstream backend-specific settings.

target_crs: int = 3035
        EPSG code for output product (e.g. 3035 for LAEA projection).

resolution: float = 10.
        spatial resolution of the output data cube in unit of the target_crs

time_interpolation: bool = False
        if missing timesteps in the S1 & S2 temporal profiles are interpolated (per pixel)
        
ts_interval: str = 'dekad'
        temporal binning (see openEO documentation for possible aggregators)

S2_BANDS: list = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]
        which reflectance bands to process of Sentinel-2. Note: requested VI's with reflectance bands not listed 
        will be not calculated & can lead to error message. 

SLC_masking_algo: str = 'mask_scl_dilation'
        Masking method for Sentinel-2 optical data ('satio', 'mask_scl_dilation')

s1_orbitdirection: str = 'DESCENDING'
        This setting ['ASCENDING', 'DESCENDING'] allows to limit the Sentinel-1 cube to only one orbit direction.

S2_scaling: list = [0, 10000, 0, 1.0]
        input / scaled value range of the Sentinel-2 datacube. needed to calculate VIs.

S1_db_rescale: bool = True
        if the Sentinel-1 data is rescaled from natural values to logarithmic before VIs generation

optical_vi_list: list = ['NDVI','AVI','CIRE','NIRv','NDMI','NDWI','BLFEI','MNDWI','NDVIMNDWI','S2WI','S2REP','IRECI']
        list of VI's to be generated on the time series datacube of Sentinel-2 (see Spectral Awesome package for all 
        possible VIs)
        
radar_vi_list: list = ['VHVVD','VHVVR','RVI']
        list of VI's to be generated on the time series datacube of Sentinel-1 (see Spectral Awesome package for all 
        possible VIs)

append : bool = True
        if the VI's are appended to the reflectance/radar time series cube OR replace them           
```

## examples
see the "notebook" sub-folder for detailed jupyter notebook examples