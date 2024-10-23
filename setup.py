#!/usr/bin/env python3

from setuptools import setup, find_packages

# Load the eo_processing version info.
#
# Note that we cannot simply import the module, since dependencies listed
# in setup() will very likely not be installed yet when setup.py run.
#
# See:
#   https://packaging.python.org/guides/single-sourcing-package-version


_version = {}
with open("src/eo_processing/_version.py") as fp:
    exec(fp.read(), _version)


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='eo_processing',
    version=_version["__version__"],
    author='Dr. Marcel Buchhorn',
    author_email='marcel.buchhorn@vito.be',
    description='EO data processing tools for openEO',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/ESA-WEED-project/eo_processing',
    python_requires=">=3.12",
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite='tests',
    include_package_data=True,
    package_dir={'': 'src'},
    package_data={'': ['resources/*', 'resources/**/*']},
    packages=find_packages('src'),
    zip_safe=True,
    install_requires=[
        "rasterio>=1.3.11",
        "openeo>=0.31.0",
        "geopandas>=1.0.1",
        "notebook>=7.2.2",
        "matplotlib>=3.9.2",
        "rioxarray>=0.17.0",
        "netCDF4>=1.7.1",
        "scikit-image>=0.24.0"
        "onnx>=1.16.2",
        "catboost>=1.2.7",
        "onnxruntime>=1.18.1",
        "geojson>=3.1.0",
        "pyarrow>=17.0.0",
        "fastparquet>=2024.5.0"
    ],
)