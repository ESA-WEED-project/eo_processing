#%%

import unittest
import json
from unittest.mock import MagicMock
import openeo
from eo_processing.openeo.preprocessing import ts_datacube_extraction
from eo_processing.openeo.preprocessing import extract_S2_datacube, extract_S1_datacube, ts_datacube_extraction

#%%
connection = openeo.connect("openeo.dataspace.copernicus.eu").authenticate_oidc()

spatial_extent = {
            'east': 5.5,
            'south': 49.5,
            'west': 4.5,
            'north': 50.5,
            'crs': 'EPSG:4326'
        }

temporal_extent = ["2021-01-01", "2021-12-31"]

data_cube = ts_datacube_extraction(connection,
                                   spatial_extent,
                                   temporal_extent[0],
                                   temporal_extent[1])


process = data_cube.to_json()

# if needed, write JSON to file, e.g.:
with open("./ts_datacube_extraction.json", "w") as tfile:
    tfile.write(process)


#%%
class TestOpenEOWorkflows(unittest.TestCase):

    def setUp(self):
        # Mock OpenEO connection
        self.connection = MagicMock()

        # Define a bounding box example
        self.bbox = {
            'east': 5.5,
            'south': 49.5,
            'west': 4.5,
            'north': 50.5,
            'crs': 'EPSG:4326'
        }
        # Define time range
        self.start = "2021-01-01"
        self.end = "2021-12-31"

    def load_expected_process_graph(self, filename):
        """Load expected process graph from JSON file."""
        with open(filename, 'r') as f:
            return json.load(f)

    def generate_s2_process_graph(self):
        """Generate a process graph for Sentinel-2 only."""
        s2_datacube = ts_datacube_extraction(
            self.connection, self.bbox, self.start, self.end,
            S2_collection='SENTINEL2_L2A',
            processing_options={
                'target_crs': 'EPSG:32631',
                'resolution': 10,
                'S2_bands': ['B02', 'B03', 'B04']
            }
        )
        return s2_datacube.graph

    def test_s2_process_graph(self):
        """Test Sentinel-2 process graph."""
        expected_graph = self.load_expected_process_graph("expected_S2_graph.json")
        generated_graph = self.generate_s2_process_graph()
        self.assertDictEqual(expected_graph, generated_graph, "Sentinel-2 process graph does not match the expected graph.")

    def generate_combined_process_graph(self):
        """Generate a process graph for combined Sentinel-1 and Sentinel-2."""
        combined_datacube = ts_datacube_extraction(
            self.connection, self.bbox, self.start, self.end,
            S2_collection='SENTINEL2_L2A',
            S1_collection='SENTINEL1_GRD',
            processing_options={
                'target_crs': 'EPSG:32631',
                'resolution': 10,
                'S2_bands': ['B02', 'B03', 'B04'],
                's1_orbitdirection': 'DESCENDING'
            }
        )
        return combined_datacube.graph

    def test_combined_process_graph(self):
        """Test combined Sentinel-1 and Sentinel-2 process graph."""
        expected_graph = self.load_expected_process_graph("expected_combined_graph.json")
        generated_graph = self.generate_combined_process_graph()
        self.assertDictEqual(expected_graph, generated_graph, "Combined process graph does not match the expected graph.")