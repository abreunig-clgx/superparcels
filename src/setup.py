from setuptools import setup, find_packages

setup(
    name="superparcels",
    version="0.1",
    packages=find_packages(include=['sp_geoprocessing', 'gcslib_cp']), 
    install_requires=[
        "tqdm",
        "click",
        "google-cloud",
        "google-cloud-storage",
        "google-cloud-bigquery",
        "pyarrow",
    ],
    entry_points={
        "console_scripts": [
            "sps=cli:cli",
        ],
    },
)
