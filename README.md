<p align="center">
  <img alt="Logo" src="https://github.com/user-attachments/assets/ef022260-aecc-4758-af16-d0eae160ae25" style="width:50%; height:auto;" />
</p>

# eo_processing
repro for EO data processing in openEO

## WEED: Python development environment
install the Python miniconda environment -> detailed instructions here: [python_dev_environment](https://github.com/ESA-WEED-project/.github/tree/main/python_dev_environment)
<br>
and make sure the eo_processing is integrated via edible install
  - cd in the working folder of your repro (mostly in PyCharmsProject the name of the cloned repository) with terminal
  - run `conda activate weed`
  -	run `python -m pip install -e .`

## explanation on settings for S1/S2 and PlanetScope processing pipelines
By default, all required settings are automatically loaded for a chosen `provider` (`terrascope`, `creodias`, `cdse`). All default settings are set in `src/eo_processing/config/settings.py`. Settings are comprised of:
- `collection_options` set the standard EO data collections for processing. Available for Sentinel 1 & 2, manual for PlanetScope.
- `processing_options` that steer the methodological workflow for EO time series data extraction, VI generation & feature generation (temporal aggregation). Available for Sentinel (1 & 2) & PlanetScope.
- `job_options` that steer the OpenEO job that runs the workflow. Available for Sentinel 1 & 2, manual for PlanetScope.

how to best define the `processing_options` in a call for Sentinel-1 and Sentinel-2
- use pre-defined settings for `raw_extraction` (generation of raw reflectance/sigma naught time series cubes), `vi_generation` (time series cubes of the VI [plus raw data if requested]) or `feature_generation` via the task key-word in the `get_standard_processing_options` call
- use the `get_advanced_options` function and define all settings using the below list

Available `processing_options`

```
provider: str = None
        Sets the expected backend where the workflow should run ['terrascope', 'creodias', 'cdse', 'cdse-staging'].
        This setting activates certain checks and flags for processing on the backend.

target_crs: int = 3035
        EPSG code for output product (e.g. 3035 for LAEA projection).
        If set to None, the native crs of the input data is used.

resolution: float = 10.
        spatial resolution of the output data cube in unit of the target_crs. 
        If set to None, the native resolution of the input data is used.
        
ts_interval: str = 'dekad'
        temporal binning ('day', 'week', 'dekad', 'month', 'season', 'year', None) for S1/S2.
        Note: if set to None, the temporal aggregation is skipped and the raw time series data is returned.

S1_temporal_reducer : str = 'mean'
        temporal reducer for the S1 data cube in the temporal binning process.
        possible reducer ('median', 'mean', 'max', 'min', 'first', 'last', 'product', 'sd', 'sum', 'variance')

S2_temporal_reducer : str = 'median'
        temporal reducer for the S2 data cube in the temporal binning process.
        possible reducer ('median', 'mean', 'max', 'min', 'first', 'last', 'product', 'sd', 'sum', 'variance')

time_interpolation: bool = False
        if missing timesteps in the S1 & S2 temporal profiles are interpolated (per pixel)

skip_check_S1 : bool = False
        if True, the S1 data is not checked for missing timesteps (e.g. due to gaps in the orbit)
        (-> this can lead to errors in the VI calculation)

s1_orbitdirection: str = 'DESCENDING'
        This setting ['ASCENDING', 'DESCENDING'] allows to limit the Sentinel-1 cube to only one orbit direction.
        If set to None, all orbit directions are used.

skip_check_S2 : bool = False
        if True, the S2 data is not checked for missing timesteps (e.g. due to gaps in the orbit)
        (-> this can lead to errors in the VI calculation)

S2_max_cloud_cover: int = 95
        Maximum allowable cloud cover percentage for Sentinel-2 data. Acceptable values are integers
        between 0 and 100.

S2_BANDS: list = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]
        which reflectance bands to process of Sentinel-2. Note: requested VI's with reflectance bands not listed 
        will be not calculated & can lead to error message. 

s2_tileid_list: list = None
        if provided, this list contains tileIDs (eg ['31UFS']) which are used to limit the S2 data load to 
        these tileIDs. This list can be None, multiple tiles or one tile with or without a wildcard (*).

SLC_masking_algo: str = 'mask_scl_dilation'
        Masking method for Sentinel-2 optical data ('satio', 'mask_scl_dilation', None)
        Note: if set to None, no masking is applied and the S2 L2A data is used as is.

apply_cloud_mask : bool = True
        if True, the Sentinel-2 or PlanetScope data is masked for clouds (based on Sentinel-2 QA band). 
        If False, no masking is applied but the mask band is still created and added to the cube.
        Note: no effect when 'mask_scl_dilation' parameter is set to None.     

append : bool = True
        if the VI's are appended to the reflectance/radar time series cube OR replace them

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
        
openeo_chunk_size: int = 128
        internal chunk size for the openEO job. Use the standard chunk size and only chnage this parameter if you 
        know what you are doing. Note: this parameter is only relevant for S1/S2 data and not for PlanetScope.
```

how to best define the `processing_options` in a call for PlanetScope
- manual adjust minimum the following processing_options:
  - resolution
- further add the following processing_options to the dictionary:
```
planet_stac_url: str 
        URL to the PlanetScope STAC. Needs to be provided by the user of generated by
        habitat_mapping.openeo.build_planet_collection.collection in the habitat_mapping repository.

udm_stac_url: str 
        URL to the Usable Data Mask 2 STAC. Needs to be provided by the user of generated by
        habitat_mapping.openeo.build_planet_collection.collection in the habitat_mapping repository.

planet_bands: list = ["B02", "B03", "B04", "B05", "B06", "B07", "B08""]
        which reflectance bands to process of PlanetScope. Note: requested VI's with reflectance bands not listed 
        will be not calculated & can lead to error message.

UDM_masking_algo: str = 'mask_udm_dilation'
        Masking method for PlanetScope optical data ('satio', 'mask_udm_dilation', None)
        Note: if set to None, no masking is applied and the PlanetScope data is used as is.

planet_scaling: list = [0, 10000, 0, 1.0]
        input / scaled value range of the PLanetScope datacube. needed to calculate VIs.

planet_vi_list:  list = ['NDVI','AVI','CIRE','NIRv','NDWI']
        list of VI's to be generated on the time series datacube of PlanetScope (see Spectral Awesome package for all 
        possible VIs)
```

## examples
see the "notebook" sub-folder for detailed jupyter notebook examples
