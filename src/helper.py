import os
import ast
import geopandas as gpd
from typing import List, Tuple, Union
import pandas as pd
from shapely import wkt
from platformdirs import user_config_dir
from pathlib import Path
import json
import subprocess
import click
import logging

logger = logging.getLogger('sp_cmds')



def check_paths(*args):
    for arg in args:
        if arg and not os.path.exists(arg):
            raise click.ClickException(f"File or directory does not exist: {arg}")

def build_filename(prefix, delimiter, suffix, *args):
    """
    Build a filename from a list of arguments.
    Example: build_filename('spfixed', '-', 'dbscan', 200, 3, 300000)
    Returns: 'spfixed-200-3-300000-dbscan'
    """
    args_str = delimiter.join(map(str, args))
    return f"{prefix}-{args_str}-{suffix}"

def parse_to_str_list(ctx, param, value):
    if not value:
        return None
    
    result = []
    if value[0].endswith(','):
        raise click.BadParameter("Invalid format. -- NO SPACES! eg. st1,st2,st3")
    
    
    
    for item in value[0].split(','):
        result.append(str(item))

    return result

def parse_to_int_list(ctx, param, value):
    if not value:
        return [200] # default distance
    
    
    result = []
    if value[0].endswith(','):
        raise click.BadParameter("Invalid format. -- NO SPACES! eg. st1,st2,st3")
    
    
    
    for item in value[0].split(','):
        result.append(int(item))

    return result

import click

def parse_key_value(ctx, param, value):
    """Parses key-value pairs into a dictionary.
    
    Examples:
      "key=value" -> {'key': 'value'}
      "key=[item1,item2]" -> {'key': ['item1', 'item2']}
      "key=[item1,item2],other=path" -> {'key': ['item1', 'item2'], 'other': 'path'}
    """
    if not value:
        return None

    # If multiple values are provided (as a tuple or list), join them into one string.
    if isinstance(value, (list, tuple)):
        value = ','.join(value)

    def split_outside_brackets(s):
        """Split string by commas that are not within square brackets."""
        parts = []
        current = []
        depth = 0
        for char in s:
            if char == '[':
                depth += 1
            elif char == ']':
                depth -= 1
            # If we hit a comma and we're not inside a bracketed section, split here.
            if char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        if current:
            parts.append(''.join(current))
        return parts

    result = {}
    # Split the input string into key-value pair strings.
    pairs = split_outside_brackets(value)
    
    for pair in pairs:
        if '=' not in pair:
            raise click.BadParameter(f"Invalid format '{pair}'. Use key=value")
        key, val = pair.split('=', 1)
        key = key.strip()
        val = val.strip()
        # If the value is enclosed in brackets, interpret it as a list.
        if val.startswith('[') and val.endswith(']'):
            inner = val[1:-1].strip()
            items = [item.strip() for item in inner.split(',')] if inner else []
            result[key] = items
        else:
            result[key] = val
    return result

def setup_logger():
    """
    Set up and return a configured logger.

    This function creates a logger using Python's logging module, sets its level to INFO, defines a formatter that 
    includes the timestamp, process name, log level, and message, and attaches a StreamHandler to output logs to 
    the console.

    Returns:
        logging.Logger: A configured logger instance.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # debug hard set for running within pytest debugger
    
    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger

def sql_query(path, fips_list):

    if len(fips_list) == 1:
        fips_list = f"('{fips_list[0]}')"
    else:
        fips_list = tuple(fips_list)
    query = f"""
        SELECT * FROM `{path}`
        WHERE FIPS IN {fips_list}
    """
    return query

def bigquery_to_gdf(
    json_key: str,
    sql_query: str,
    verbose: bool = True
    ):
    """
    Pulls a BigQuery table to input geodataframe

    Args:
    json_key (str): Path to the JSON key file.
    sql_query (str): SQL query to execute
    verbose (bool): If true, log messages will be printed to the console.
    """
    import geopandas as gpd
    from shapely import wkt
    from bigq.bigq import BigQ

    bq = BigQ(verbose=verbose)

    # AUTH
    try:
        bq.auth.authenticate(json_key)
    except Exception as auth_error:
        print(f"Authentication error: {auth_error}")
        return

    # QUERY
    try:
        result_df = bq.query(sql_query)
    
    except Exception as query_error:
        logger.info(f"Query execution error: {query_error}")
        logger.info(f"Query: {sql_query}")
        return

    result_df['geometry'] = result_df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(result_df, geometry='geometry', crs='EPSG:4326')

    return gdf

def gdf_to_bigquery(
    json_key: str,
    gdf: gpd.GeoDataFrame,
    table_name: str,
    write_type: str = "WRITE_APPEND",
    verbose: bool = True
):
    """
    Pushes a geodataframe to BigQuery

    Args:
    json_key (str): Path to the JSON key file.
    gdf (gpd.GeoDataFrame): Geodataframe to push.
    table_name (str): Name of the BigQuery table.
    verbose (bool): If true, log messages will be printed to the console.
    """
    from bigq.bigq import BigQ

    bq = BigQ(verbose=verbose)

    # AUTH
    try:
        bq.auth.authenticate(json_key)
    except Exception as auth_error:
        print(f"Authentication error: {auth_error}")
        return

    # PUSH
    try:
        bq.upload_gdf(
            gdf=gdf, 
            table_id=table_name,
            write_disposition=write_type)
    
    except Exception as push_error:
        logger.info(f"Push error: {push_error}")
        return

"""
def download_from_gcs(json_key, gcs_path, local_dir):
    try:
        # step 1: Test Authentication
        gcs = gcslib_cp.Gcslib(verbose=ctx.obj["VERBOSE"])
        gcs.auth.authenticate(credentials=json_key)

        gcs.cp(gcs_path, local_dir, recursive=True)
    
        return None

    except Exception as e:
        raise logger.error(f"Failed to download {gcs_path}: {e}")"
"""

def csv_conversion(csv_path, crs='EPSG:4326', dtypes=None):
        """
        Convert a CSV file to a dataframe
        """
        df = pd.read_csv(csv_path, dtype=dtypes, compression='gzip', encoding='utf-8')

        return df

def get_fips(df, filter_by='FIPS'):
    """
    Get unique FIPS codes from a dataframe
    """
    return df[filter_by].unique().tolist()

def shp_conversion(df, crs='EPSG:4326', where=None):
    if where:
        df = df.query(where) # sql-like query

    gdf = gpd.GeoDataFrame(df, geometry=df['geometry'].apply(wkt.loads), crs=crs)

    return gdf

def build_sp_args(
    candidate_gdf: gpd.GeoDataFrame,
    fips_field: str,
    dist_thres: List[Union[int, float]],
    owner_field: str,
    sample_size: int,
    area_threshold: float,
    output_dir: str
) -> List[Tuple]:
    
    
    """
    Builds a list of argument tuples for clustering superparcels by FIPS and distance thresholds.

    Parameters
    ----------
    candidate_gdf : gpd.GeoDataFrame
        The full set of candidate parcels.
    fips_field : str
        The name of the column containing FIPS codes.
    dist_thres : list of float
        A list of distance thresholds to test.
    owner_field : str
        The field used to identify ownership.
    sample_size : int
        Sample size for subsampling parcels (if applicable).
    area_threshold : float
        Minimum area to consider.
    output_dir : str
        Directory where output files should be saved.


    Returns
    -------
    List[Tuple]
        A list of argument tuples for processing.
    """

    fips_to_process = candidate_gdf[fips_field].unique()
    logger.info(f"FIPS in table: {fips_to_process}")
    sp_args = []

    for dt in dist_thres:
        for county_fips in fips_to_process:
            logger.info(f"Collecting FIPS: {county_fips} with distance {dt}")
            fips_gdf = candidate_gdf[candidate_gdf[fips_field] == county_fips]

            sp_args.append((
                fips_gdf,
                county_fips,
                owner_field,
                dt,
                sample_size,
                area_threshold,
                output_dir
            ))

    return sp_args



def get_git_commit_hash(short: bool = True) -> str:
    try:
        cmd = ["git", "rev-parse", "--short" if short else "HEAD"]
        print(os.getcwd())
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting git commit hash: {e}")
        return "unknown"


def get_config_path() -> Path:
    APP_NAME = "superparcels"
    CONFIG_FILENAME = "config.json"
    config_dir = Path(user_config_dir(APP_NAME))
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / CONFIG_FILENAME

def load_config(config_path: str = None) -> dict:
    if config_path.exists():
        with open(config_path, "r") as f:
            return json.load(f)
    return {}

def save_config(config_path: str = None, config: dict = None) -> None:
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


