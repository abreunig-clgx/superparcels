import os
import glob
import click
import pandas as pd
import geopandas as gpd
import ast
import json
from shapely import wkt
import logging
#from sp_geoprocessing.phase2 import *
from gcslib_cp import gcslib_cp
from main_sp_fixed import build_sp_fixed

# Configure the root logger
logger = logging.getLogger(__name__)

# Callback function to parse input into a list
def parse_list(ctx, param, value):
    # Remove the leading and trailing square brackets and split by commas
    if value:
       
        result = ast.literal_eval(value)
        return [str(item) for item in result]
    else:
        return []

@click.group()
@click.pass_context
def setup(ctx):
    if ctx.obj["VERBOSE"]:
        logger.setLevel(logging.DEBUG)

@setup.command(help="Creates config file for SuperParcel Build.")
@click.option('-bd', type=click.Path(exists=False), help="Directory where you want the build to occur.")
@click.option('-csv', type=click.Path(exists=False), help="Path to HUC shapefile.")
@click.option('-fips', type=str, default=None, help="Field to use for filtering FIPS codes.")
@click.option('-js', type=click.Path(exists=False), help="Path to GCP JSON key file.")
@click.pass_context
def config(ctx, bd, csv, fips, js):
    click.echo("_________________________________________________________")
    logger.info("SETTING UP SuperParcel Build")
    click.echo("-")
    click.echo("-")

    FIPS_FIELD = fips

    def download_from_gcs(json_key, gcs_path, local_dir):
        try:
            # step 1: Test Authentication
            gcs = gcslib_cp.Gcslib(verbose=ctx.obj["VERBOSE"])
            gcs.auth.authenticate(credentials=json_key)
 
            gcs.cp(gcs_path, local_dir, recursive=True)
        
            return None

        except Exception as e:
            raise logger.error(f"Failed to download {gcs_path}: {e}")

            

    # Determine paths (local or GCS download)
    input_subdir = os.path.join(bd, "inputs")
    os.makedirs(input_subdir, exist_ok=True)
    
    logger.info('Extracting paths...')
    csv_path = download_from_gcs(js, csv, input_subdir) if csv.startswith("gs://") else csv
    logger.debug(f"CSV Path: {csv_path}")    

    def csv_conversion(csv_path, crs='EPSG:4326', dtypes=None, where=None):
        """
        Convert a CSV file to a geodataframe
        """
        df = pd.read_csv(csv_path, dtype=dtypes)

        return df

    def get_fips(df, filter_by='FIPS'):
        """
        Get unique FIPS codes from a dataframe
        """
        return df[filter_by].unique()
    
    def shp_conversion(df, crs='EPSG:4326', filter_by='FIPS', fips=None):
        # Convert the geometry column to a geodataframe
        gdf = gpd.GeoDataFrame(df, geometry=df['geometry'].apply(wkt.loads), crs=crs)

        return gdf
        
    
    df = csv_conversion(csv_path, dtypes={FIPS_FIELD: str})

    fips_list = get_fips(df, filter_by=FIPS_FIELD)
    for fips in fips_list:
        gdf = shp_conversion(df, fips=fips)
        fips_subdir = os.path.join(input_subdir, fips)
        if not os.path.exists(fips_subdir):
            os.makedirs(fips_subdir)

        gdf.to_file(os.path.join(fips_subdir, f'candidate_parcels_{fips}.shp'))



    all_gdf_paths = glob.glob(os.path.join(input_subdir, '*', '*.shp'))

    config = {}
    config["BUILD_DIR"] = bd
    # if has local directories else pull from GCS
    config["CANDIDATE_PARCELS"] = all_gdf_paths
    logger.debug(f"Candidate Parcels: {all_gdf_paths}")
    
    try:
        with open(ctx.obj["CONFIG"], "w") as config_file:
            json.dump(config, config_file, indent=4)
        logger.info(f"Configuration file created!")
    except Exception as e:
        raise logger.error(f"Failed to write config file: {e}")
    
    logger.info("SETUP COMPLETE.")
    click.echo("_________________________________________________________")
         
