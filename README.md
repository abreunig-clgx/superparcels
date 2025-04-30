# SuperParcels CLI Tool
- [SuperParcels CLI Tool](#superparcels-cli-tool)
  - [Introduction](#introduction)
  - [Requirements and Installation](#requirements-and-installation)
    - [Python Requirements](#python-requirements)
    - [Big Query Requirements](#big-query-requirements)
    - [Installation](#installation)
  - [CLI Usage](#cli-usage)
    - [CLI Entry Point: *sps*](#cli-entry-point-sps)
    - [Commands:](#commands)
      - [Calling for Help gives helpful docs on how to use CLI. The help flag can be used on any subsequent sub-commands.](#calling-for-help-gives-helpful-docs-on-how-to-use-cli-the-help-flag-can-be-used-on-any-subsequent-sub-commands)
      - [Verbose: Logs helpful messages used for debugging purposes](#verbose-logs-helpful-messages-used-for-debugging-purposes)
      - [Config](#config)
        - [Config Docs](#config-docs)
        - [Config Options](#config-options)
        - [Examples](#examples)
          - [Create config file](#create-config-file)
          - [Print Config](#print-config)
          - [Update Config](#update-config)
      - [Build](#build)
      - [spfixed](#spfixed)
        - [spfixed docs \& options:](#spfixed-docs--options)
        - [spfixed options:](#spfixed-options)
        - [Examples](#examples-1)
          - [Build superparcels with distance thresholds 30m \& 50m and use default fips from config](#build-superparcels-with-distance-thresholds-30m--50m-and-use-default-fips-from-config)
          - [Build superparcels for 06075 and write *only* to local (shapefiles), verbose](#build-superparcels-for-06075-and-write-only-to-local-shapefiles-verbose)

## Introduction
The SuperParcels CLI Tool enables users to build superparcels for any county in the US. The tool utilizes the sp_geoprocessing library which is the core functionality behind the superparcel product, and provides additional tooling to big query upload and download. 

The CLI is specific to the handling of big query, therefore, it is required to have GCP authentication to the required tables for this package to run correctly. Furthermore, metadata is hard-coded, so this tool *only* points to specific tables. Future development will allow for flexibility in this regard. 

The CLI provides two main commands: *config* and *build*. Although not required, it is highly recommended to run the config command before build. Config stores paths and metadata used in the build process.

Versioning is stored in the output tables and is taking from the project's toml file. Users can then identify build outputs by the version of this package it was run on. Releases will be published regularly, so please check often.


## Requirements and Installation 
### Python Requirements

- conda installer. Recommended installer: [miniforge](https://github.com/conda-forge/miniforge?)
- Regular Users Package Requirements (see below for Installation Instructions):

    - python >= 3.11
    - geopandas
    - scipy,
    - scikit-learn
    - google-cloud,
    - google-cloud-storage,
    - google-cloud-bigquery,
    - pyarrow,
    - db-dtypes,
    - platformdirs
- Developer User Package Requirements

    - Regular User Packages 
    - pytest
    - tomlkit
    - build

### Big Query Requirements

- Service Account key with BigQuery Editor privileges to: **clgx-gis-app-dev-06e3.superparcels**

### Installation
1. Install miniforge or similar conda installer
2. Run the following commands:
   ```
    conda create -n <env_name> python=3.11
    conda activate <env_name>
   ```
3. Download **latest** package wheel from [release](https://github.com/abreunig-clgx/superparcels/releases) page
4. Regular User Install:
   ```
    pip install path/to/superparcels-<latest-version>-py3-none-any.whl
   ```
5. Developer User Install:
   
   5.1 Ensure repo is cloned then cd to root dir 
   
   5.2 Run:
   ```
    pip install -e .[dev]
   ```
6. Test Installation by running:
   ```
    sps --help
   ```
## CLI Usage
### CLI Entry Point: *sps*
### Commands:
#### Calling for Help gives helpful docs on how to use CLI. The help flag can be used on any subsequent sub-commands.
```
sps -h
sps --help
```
#### Verbose: Logs helpful messages used for debugging purposes
```
sps --verbose
```
#### Config
Builds config json file for SuperParcel build.
```
CONFIG JSON
{
    'BUILD_DIR': PATH, 
    'INPUT_DIR': BUILD_DIR + 'inputs',
    'OUTPUT_DIR': BUILD_DIR + 'outputs',
    'ANALYSIS_DIR': BUILD_DIR + 'analysis',
    'GCP_JSON': PATH.JSON,
    'GCP_PROJECT': 'clgx-gis-app-dev-06e3', 
    'GCP_INPUT_DATASET': 'superparcels', 
    'GCP_INPUT_TABLE':
        'short_query_pu_pipeline_candidate_parcels', 
    'GCP_OUTPUT_DATASET': 'superparcels', 
    'GCS_BUCKET': 
        'gs://geospatial-projects/super_parcels',
        
    'FIPS_LIST': [fips1, fips2, etc.]}
```
##### Config Docs
```
sps config -h
```
##### Config Options

  - -js, --json-key: *Path to GCP JSON key file*.

  - -fips, --county-fips: *County FIPS to build.* Comma-seperated. No Spaces.


  - -bd, --build-dir: *Local directory for build*.
  - -see: *See config.json file*.
  - -update: *Update config.json file. Key Value pairs
                             seperated by '='. No Spaces*.
##### Examples
###### Create config file 
```
sps config -js <gcp_key.json> -fips 08031,06075 --build-dir </path/to/buildir>
```
###### Print Config
```
sps config -see
```
###### Update Config
```
sps config -update GCP_JSON=/new/gcp.json, FIPS_LIST=[55107,16001]
```
#### Build
Build SuperParcels using various subcommands

**Subcommands:**

- spfixed (stable)
- spmulti (in-development)
#### spfixed
Builds SuperParcels using a fixed epsilon --> Phase 1 Developement
##### spfixed docs & options:
```
sps build spfixed -h
```
##### spfixed options:

  - -fips: *FIPS code(s) to build SuperParcel for.
                                  Comma-seperated. No Spaces. If not provided,
                                  will build for all FIPS codes found in
                                  config.json*

  - -dt, --dist-thres: *Distance threshold list for clustering.
                                  Comma-seperated. No Spaces. Default is 200.*

  - -ss, --sample-size : *Minimum number of samples for clustering.
                                  Default is 3.*

  - -at, --area-threshold: *Minimum area threshold for super parcel
                                  creation. Default is None. NOT 
                                  IMPLEMENTED.*

  - -local, --local-upload: *Saves build to local build directory.
                                  Default is False.*

  - -bq, --bq-upload: *Uploads to BigQueryTable. Default is True.*
  
  - -bd, --build-dir: *Directory where you want the build to occur.
                                  If not provided, will use the build
                                  directory from config.json.*

  - -pb: *Path to Place Boundaries Shapefile. FUTURE
                                  IMPLEMENTATION*

##### Examples
###### Build superparcels with distance thresholds 30m & 50m and use default fips from config
```
sps build spfixed -dt 30,50
```
###### Build superparcels for 06075 and write *only* to local (shapefiles), verbose
```
sps -v build spfixed -fips 06075 -local true --bq-upload false
```