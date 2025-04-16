"""
Module: sp_cmds

This module provides utility functions for working with geospatial data and managing
BigQuery, file paths, and configuration operations for the SuperParcels project. It 
includes helper functions for parsing command-line arguments (using Click), building 
filenames, working with SQL queries and geodataframes, logging setup, and managing task 
batches for multiprocessing.
"""

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
import importlib.metadata
import multiprocessing
import logging

logger = logging.getLogger('sp_cmds')


def check_paths(*args):
    """
    Check that each provided path exists.

    This function iterates over the provided paths (files or directories) and raises a 
    click.ClickException if any path does not exist.

    Args:
        *args: A variable number of path strings to check.

    Raises:
        click.ClickException: If any provided path does not exist.
    """
    for arg in args:
        if arg and not os.path.exists(arg):
            raise click.ClickException(f"File or directory does not exist: {arg}")


def build_filename(prefix, delimiter, suffix, *args):
    """
    Build a filename from a list of arguments.

    Example:
        build_filename('spfixed', '-', 'dbscan', 200, 3, 300000)
        returns: 'spfixed-200-3-300000-dbscan'

    Args:
        prefix (str): The prefix for the filename.
        delimiter (str): Delimiter used to join the arguments.
        suffix (str): The suffix to append to the filename.
        *args: Additional arguments to include in the filename.

    Returns:
        str: The constructed filename.
    """
    args_str = delimiter.join(map(str, args))
    return f"{prefix}-{args_str}-{suffix}"


def parse_to_str_list(ctx, param, value):
    """
    Parse a comma-separated string into a list of strings.

    This function serves as a Click callback to parse a command-line parameter.
    It expects a single string without spaces where items are separated by commas.
    
    Args:
        ctx (click.Context): Click context.
        param (click.Parameter): Click parameter.
        value (list or None): Input value provided to the parameter.

    Returns:
        list or None: A list of strings if input is provided, otherwise None.

    Raises:
        click.BadParameter: If the input format is invalid (e.g., trailing comma with spaces).
    """
    if not value:
        return None

    result = []
    if value[0].endswith(','):
        raise click.BadParameter("Invalid format. -- NO SPACES! eg. st1,st2,st3")

    for item in value[0].split(','):
        result.append(str(item))

    return result


def parse_to_int_list(ctx, param, value):
    """
    Parse a comma-separated string into a list of integers.

    This function serves as a Click callback to parse a command-line parameter.
    It expects a single string without spaces where items are separated by commas. If no
    value is provided, a default distance value of 200 is returned.
    
    Args:
        ctx (click.Context): Click context.
        param (click.Parameter): Click parameter.
        value (list or None): Input value provided to the parameter.

    Returns:
        list: A list of integers parsed from the input, or [200] if no input is provided.

    Raises:
        click.BadParameter: If the input format is invalid (e.g., trailing comma with spaces).
    """
    if not value:
        return [200]  # default distance

    result = []
    if value[0].endswith(','):
        raise click.BadParameter("Invalid format. -- NO SPACES! eg. st1,st2,st3")

    for item in value[0].split(','):
        result.append(int(item))

    return result


def parse_key_value(ctx, param, value):
    """
    Parse key-value pairs into a dictionary.

    The function converts string pairs into a dictionary. It also handles comma-separated 
    list values enclosed in square brackets.

    Examples:
      "key=value" -> {'key': 'value'}
      "key=[item1,item2]" -> {'key': ['item1', 'item2']}
      "key=[item1,item2],other=path" -> {'key': ['item1', 'item2'], 'other': 'path'}

    Args:
        ctx (click.Context): Click context.
        param (click.Parameter): Click parameter.
        value (str or list): The string of key-value pairs.

    Returns:
        dict or None: Parsed key-value pairs as a dictionary, or None if no value provided.

    Raises:
        click.BadParameter: If a pair is not in the key=value format.
    """
    if not value:
        return None

    # If multiple values are provided (as a tuple or list), join them into one string.
    if isinstance(value, (list, tuple)):
        value = ','.join(value)

    def split_outside_brackets(s):
        """
        Split string by commas that are not within square brackets.

        Args:
            s (str): Input string.

        Returns:
            list: List of substrings split by commas not within brackets.
        """
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
    logger.setLevel(logging.INFO)  # debug hard set for running within pytest debugger

    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


def sql_query(path, fips_list):
    """
    Construct a SQL query string to select rows with specific FIPS codes.

    Depending on the number of FIPS codes provided, the function creates a query that filters the rows
    in the table (or dataset) defined by the `path` parameter.

    Args:
        path (str): The table or dataset path.
        fips_list (list): A list of FIPS codes to filter on.

    Returns:
        str: The constructed SQL query string.
    """
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
    Pull a BigQuery table into a GeoDataFrame.

    This function authenticates with BigQuery using the provided JSON key, executes the provided SQL query,
    converts the geometry column from WKT to geometric objects, and returns a GeoDataFrame with a CRS of 'EPSG:4326'.

    Args:
        json_key (str): Path to the JSON key file.
        sql_query (str): SQL query to execute.
        verbose (bool): If True, log messages will be printed to the console.

    Returns:
        geopandas.GeoDataFrame: A geodataframe containing the queried data, or None if an error occurs.
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
    Push a GeoDataFrame to BigQuery.

    This function authenticates to BigQuery using the provided JSON key and uploads the given GeoDataFrame 
    to the specified table. The upload behavior can be controlled with the write_type parameter.

    Args:
        json_key (str): Path to the JSON key file.
        gdf (geopandas.GeoDataFrame): GeoDataFrame to push.
        table_name (str): Name of the BigQuery table.
        write_type (str): Write disposition type (e.g., "WRITE_APPEND", "WRITE_TRUNCATE").
        verbose (bool): If True, log messages will be printed to the console.
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
            write_disposition=write_type
        )
    except Exception as push_error:
        logger.info(f"Push error: {push_error}")
        return


# The following function is commented out and reserved for future use.
"""
def download_from_gcs(json_key, gcs_path, local_dir):
    try:
        # Step 1: Test Authentication
        gcs = gcslib_cp.Gcslib(verbose=ctx.obj["VERBOSE"])
        gcs.auth.authenticate(credentials=json_key)

        gcs.cp(gcs_path, local_dir, recursive=True)
    
        return None

    except Exception as e:
        raise logger.error(f"Failed to download {gcs_path}: {e}")
"""


def csv_conversion(csv_path, crs='EPSG:4326', dtypes=None):
    """
    Convert a CSV file to a pandas DataFrame.

    This function reads a CSV file (optionally compressed with gzip) into a DataFrame with the specified data types.
    
    Args:
        csv_path (str): The file path to the CSV.
        crs (str, optional): Coordinate reference system, not used in conversion but provided for consistency.
        dtypes (dict, optional): Dictionary specifying data types for the DataFrame columns.

    Returns:
        pandas.DataFrame: The DataFrame containing the CSV data.
    """
    df = pd.read_csv(csv_path, dtype=dtypes, compression='gzip', encoding='utf-8')
    return df


def get_fips(df, filter_by='FIPS'):
    """
    Retrieve unique FIPS codes from a DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame containing the data.
        filter_by (str): Column name to filter by. Default is 'FIPS'.

    Returns:
        list: A list of unique FIPS codes.
    """
    return df[filter_by].unique().tolist()


def shp_conversion(df, crs='EPSG:4326', where=None):
    """
    Convert a DataFrame with a WKT geometry column to a GeoDataFrame.

    Optionally, the DataFrame can be filtered using a SQL-like query provided in the 'where' parameter.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        crs (str, optional): Coordinate reference system for the resulting GeoDataFrame. Default is 'EPSG:4326'.
        where (str, optional): SQL-like query string to filter the DataFrame before conversion.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame with a proper geometry column.
    """
    if where:
        df = df.query(where)  # SQL-like query

    gdf = gpd.GeoDataFrame(df, geometry=df['geometry'].apply(wkt.loads), crs=crs)
    return gdf


def build_spmulti_args(
    candidate_gdf: gpd.GeoDataFrame,
    fips_field: str,
    dist_thres: List[Union[int, float]],
    owner_field: str,
    sample_size: int,
    area_threshold: float,
    timestamp: str,
    version: str,
    bq_output_dir: str,
    local_output_dir: str,
    bq_upload: bool,
    local_upload: bool,
    json_key: str,
) -> List[Tuple]:
    """
    Build a list of argument tuples for clustering superparcels using multiple distance thresholds.

    Parameters
    ----------
    candidate_gdf : geopandas.GeoDataFrame
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
    timestamp : str
        Timestamp for the output files.
    version : str
        The version of the code.
    bq_output_dir : str 
        The BigQuery table prefix for output.
    local_output_dir : str
        The local directory for output.
    bq_upload : bool
        If True, upload to BigQuery.
    local_upload : bool
        If True, save locally.
    json_key : str
        Path to the JSON key file for BigQuery authentication.

    Returns
    -------
    list of tuple
        A list of argument tuples for processing.
    """
    fips_to_process = candidate_gdf[fips_field].unique()
    logger.info(f"FIPS in table: {fips_to_process}")
    sp_args = []

    # Sort distance thresholds from largest to smallest.
    dist_thres = sorted(dist_thres, reverse=True)

    for county_fips in fips_to_process:
        logger.info(f"Collecting FIPS: {county_fips} with multi-step distances {dist_thres}")
        fips_gdf = candidate_gdf[candidate_gdf[fips_field] == county_fips]

        sp_args.append((
            fips_gdf,
            county_fips,
            owner_field,
            dist_thres,
            sample_size,
            area_threshold,
            timestamp,
            version,
            bq_output_dir,
            local_output_dir,
            bq_upload,
            local_upload,
            json_key
        ))

    return sp_args


def build_spfixed_args(
    candidate_gdf: gpd.GeoDataFrame,
    fips_field: str,
    dist_thres: List[Union[int, float]],
    owner_field: str,
    sample_size: int,
    area_threshold: float,
    timestamp: str,
    version: str,
    bq_output_dir: str,
    local_output_dir: str,
    bq_upload: bool,
    local_upload: bool,
    json_key: str,
) -> List[Tuple]:
    """
    Build a list of argument tuples for clustering superparcels using fixed distance thresholds.

    Parameters
    ----------
    candidate_gdf : geopandas.GeoDataFrame
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
    timestamp : str
        Timestamp for the output files.
    version : str
        The version of the code.
    bq_output_dir : str 
        The BigQuery table prefix for output.
    local_output_dir : str
        The local directory for output.
    bq_upload : bool
        If True, upload to BigQuery.
    local_upload : bool
        If True, save locally.
    json_key : str
        Path to the JSON key file for BigQuery authentication.

    Returns
    -------
    list of tuple
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
                timestamp,
                version,
                bq_output_dir,
                local_output_dir,
                bq_upload,
                local_upload,
                json_key
            ))

    return sp_args


def get_git_commit_hash(short: bool = True) -> str:
    """
    Retrieve the current Git commit hash.

    This function runs the 'git rev-parse' command to determine the current commit hash 
    of the repository. If 'short' is True, a shortened version of the commit hash is returned.

    Args:
        short (bool): If True, return the shortened commit hash. Otherwise, return the full hash.

    Returns:
        str: The Git commit hash or "unknown" if the command fails.
    """
    try:
        cmd = ["git", "rev-parse", "--short" if short else "HEAD"]
        print(os.getcwd())
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting git commit hash: {e}")
        return "unknown"


def get_config_path() -> Path:
    """
    Get the configuration file path in the user's config directory.

    The configuration file is stored in a directory specific to the application name "superparcels".
    The directory is created if it does not exist.

    Returns:
        pathlib.Path: The path to the configuration file.
    """
    APP_NAME = "superparcels"
    CONFIG_FILENAME = "config.json"
    config_dir = Path(user_config_dir(APP_NAME))
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / CONFIG_FILENAME


def load_config(config_path: Path = None) -> dict:
    """
    Load configuration from a JSON file.

    If the provided configuration file exists, it reads and returns the configuration as a dictionary.
    If the file does not exist, an empty dictionary is returned.

    Args:
        config_path (pathlib.Path, optional): Path to the configuration JSON file.

    Returns:
        dict: The loaded configuration dictionary.
    """
    if config_path and config_path.exists():
        with open(config_path, "r") as f:
            return json.load(f)
    return {}


def save_config(config_path: Path = None, config: dict = None) -> None:
    """
    Save a configuration dictionary to a JSON file.

    Args:
        config_path (pathlib.Path, optional): Path to the configuration JSON file.
        config (dict, optional): The configuration dictionary to save.

    Returns:
        None
    """
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


def get_version():
    """
    Retrieve the version of the 'superparcels' package.

    This function uses importlib.metadata to fetch the current version of the installed package.
    If the package is not found, an empty string is returned.

    Returns:
        str: The version of the package, or an empty string if not found.
    """
    try:
        version = importlib.metadata.version("superparcels")
    except importlib.metadata.PackageNotFoundError:
        version = ""
    return version


def create_batches(arg_tuples, batch_size):
    """
    Split a list of argument tuples into batches of a specified size.

    This generator function yields batches (sublists) of argument tuples for processing,
    each with a length equal to 'batch_size' (except possibly the last batch).

    Parameters:
        arg_tuples (list): A list of argument tuples.
        batch_size (int): The desired number of tuples in each batch.

    Yields:
        list: A batch (sublist) of argument tuples.
    """
    for i in range(0, len(arg_tuples), batch_size):
        yield arg_tuples[i:i + batch_size]


def parse_sp_args(task_tuple):
    """
    Parse a tuple of arguments for the sp fixed build function.

    This function splits the task tuple into the arguments for the build function and a metadata dictionary.

    Args:
        task_tuple (tuple): A tuple containing all task parameters.

    Returns:
        tuple: A tuple containing:
            - sp_build_args (tuple): The first six arguments for the build function.
            - meta (dict): A dictionary of metadata extracted from the task tuple.
    """
    sp_build_args = task_tuple[:6]  # Extract the first six arguments for the function

    meta = {
        'fips': task_tuple[1],
        'dt': task_tuple[3],
        'ss': task_tuple[4],
        'at': task_tuple[5],
        'timestamp': task_tuple[6],
        'version': task_tuple[7],
        'bq_output_dir': task_tuple[8],
        'local_output_dir': task_tuple[9],
        'bq_upload': task_tuple[10],
        'local_upload': task_tuple[11],
        'json_key': task_tuple[12]
    }

    return sp_build_args, meta


def process_result(result, meta, name):
    """
    Process the result of an asynchronous task.

    This callback function handles a completed task by checking the result, adding metadata,
    building a filename, and uploading or saving the result as needed.

    Parameters:
        result (geopandas.GeoDataFrame or None): The result from the processing task.
        meta (dict): Metadata containing process details such as FIPS, thresholds, timestamp, etc.
        name (str): Identifier for the task type ('spfixed' or 'spmulti').

    Returns:
        None
    """
    if result is None or len(result) == 0:
        logger.error(f"No results for {meta['fips']}. Skipping...")
        return

    # Add timestamp and version field to the result
    result['timestamp'] = meta['timestamp']
    result['version'] = meta['version']

    # Build filename based on the parameters
    if name == 'spfixed':
        if meta['at']:
            fn = build_filename('spfixed', '-', f"dt{meta['dt']}", f"ss{meta['ss']}", f"at{meta['at']}")
        else:
            fn = build_filename('spfixed', '-', f"dt{meta['dt']}", f"ss{meta['ss']}")

    #if name == 'spmulti':
    #    at = str(meta['at'])[-1]  # get last digit of area threshold
    #    formatted_dts = '_'.join(map(str, meta['dt']))
    #    fn = build_filename('spmulti', '-', f"dt{formatted_dts}", f"ss{meta['ss']}", f"at{at}")

    if name == 'spmulti_optimized':
        at = str(meta['at'])[-1]  # get last digit of area threshold
        formatted_dts = '_'.join(map(str, meta['dt']))
        fn = build_filename('spmulti_opt', '-', f"dt{formatted_dts}", f"ss{meta['ss']}", f"at{at}")

    # Upload to BigQuery if enabled
    if meta['bq_upload']:
        output_table_name = f"{meta['bq_output_dir']}.{fn}"
        logger.info(f"Uploading to BigQuery for {meta['fips']}: {output_table_name}")
        gdf_to_bigquery(
            gdf=result,
            table_name=output_table_name,
            json_key=meta['json_key'],
            write_type='WRITE_APPEND'
        )
        logger.info("Upload to BigQuery successful.")

    # Save locally if enabled
    if meta['local_upload']:
        local_fn = f"{fn}_{meta['fips']}.shp"
        output_local_name = os.path.join(meta['local_output_dir'], local_fn)
        logger.info(f"Saving to local directory for {meta['fips']}: {output_local_name}")
        result.to_file(output_local_name, driver='ESRI Shapefile')
        logger.info(f"Local upload successful: {output_local_name}")


def process_batch(func, batch, pool_size):
    """
    Process a batch of tasks asynchronously using multiprocessing.

    This function submits each task in the batch to a multiprocessing pool, processes the results 
    using a callback, and waits for all tasks in the batch to complete.

    Args:
        func (callable): The function to run for each task in the batch.
        batch (list): A list of task tuples.
        pool_size (int): Maximum number of concurrent processes.

    Returns:
        None
    """
    logger.info(f"Processing batch of size {len(batch)} with function {func.__name__}")
    async_results = []

    # Create a process pool limited to the desired number of concurrent jobs.
    with multiprocessing.Pool(processes=pool_size) as pool:
        for task in batch:
            if func.__name__ == 'build_sp_fixed':
                name = 'spfixed'
            #elif func.__name__ == 'build_sp_multi':
            #    name = 'spmulti'
            elif func.__name__ == 'build_sp_multi_optimized':
                name = 'spmulti_optimized'
            else:
                raise ValueError(f"Function {func.__name__} is not recognized.")

            build_args, meta = parse_sp_args(task)
            # Submit the task asynchronously with a callback that processes the result immediately.
            async_result = pool.apply_async(func, args=build_args,
                                            callback=lambda res, meta=meta: process_result(res, meta, name))
            async_results.append(async_result)

        for async_result in async_results:
            async_result.wait()
