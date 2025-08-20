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

# Read requirements from the requirements.txt file
with open('requirements.txt') as f:
    required_packages = f.read().splitlines()

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
    tests_require=[
        "pytest>=8.3.4",
        "pytest-xdist>=3.6.1",
        "pytest-cov>=6.1.1",
        "requests-mock>=1.12.1",
        "rio-cogeo>=5.4.1"],
    test_suite='tests',
    include_package_data=True,
    package_dir={'': 'src'},
    package_data={'': ['resources/*', 'resources/**/*']},
    packages=find_packages('src'),
    zip_safe=True,
    install_requires=required_packages,
)
