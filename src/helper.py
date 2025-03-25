import os
import logging
import click
import logging

logger = logging.getLogger()



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

def sql_query(path, fips):
    query = f"""
    SELECT * FROM `{path}`
    WHERE FIPS = '{fips}'
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

    result_df['geometry'] = result_df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(result_df, geometry='geometry')

    return gdf

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