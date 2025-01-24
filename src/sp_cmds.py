import os
import glob
import click
import pandas as pd
import geopandas as gpd
import ast
import json
from shapely import wkt
import logging
from gcslib_cp import gcslib_cp
from sp_geoprocessing.build import build_sp_fixed

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

@click.group(help='Setup Processes build config files, setup build directories, and prep input data for Build process.')
@click.pass_context
def setup(ctx):
    if ctx.obj["VERBOSE"]:
        logger.setLevel(logging.DEBUG)

@setup.command(help="Builds config json file for SuperParcel build.")
@click.option('-bd', type=click.Path(exists=False), help="Directory where you want the build to occur.")
@click.option('-csv', type=click.Path(exists=False), help="Path to HUC shapefile. Local or GCS path.")
@click.option('-fips', type=str, default=None, help="Field to use for filtering FIPS codes. Default is 'FIPS'.")
@click.option('-js', type=click.Path(exists=False), help="Optional: Path to GCP JSON key file.")
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

    def csv_conversion(csv_path, crs='EPSG:4326', dtypes=None):
        """
        Convert a CSV file to a dataframe
        """
        df = pd.read_csv(csv_path, dtype=dtypes)

        return df

    def get_fips(df, filter_by='FIPS'):
        """
        Get unique FIPS codes from a dataframe
        """
        return df[filter_by].unique()
    
    def shp_conversion(df, crs='EPSG:4326', where=None):
        if where:
            df = df.query(where) # sql-like query

        gdf = gpd.GeoDataFrame(df, geometry=df['geometry'].apply(wkt.loads), crs=crs)

        return gdf
        
    try:
        df = csv_conversion(csv_path, dtypes={FIPS_FIELD: str})

        fips_list = get_fips(df, filter_by=FIPS_FIELD)
        for fips in fips_list:
            gdf = shp_conversion(df, where=f"{FIPS_FIELD} == '{fips}'") 
            logger.info(f'Converting FIPS: {gdf[FIPS_FIELD].unique()}')

            fips_subdir = os.path.join(input_subdir, fips)
            if not os.path.exists(fips_subdir):
                os.makedirs(fips_subdir)

            gdf.to_file(os.path.join(fips_subdir, f'candidate_parcels_{fips}.shp'))
    except Exception as e:
        raise logger.error(f"Failed to convert CSV to Shapefile: {e}")



    all_gdf_paths = glob.glob(os.path.join(input_subdir, '*', '*.shp'), recursive=True)

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

@click.group(help='Build Processes build run the Phase 1 and Phase 2 processes for SuperParcel creation.')
@click.pass_context
def build(ctx):
    if ctx.obj["VERBOSE"]:
        logger.setLevel(logging.DEBUG)

@build.command(
    help="Builds SuperParcel Phase 1. Highly recommended to run setup first to create config file. If not, provide the necessary parameters.")
@click.option('-bd', type=click.Path(exists=False), help="Directory where you want the build to occur.")
@click.option('-fips', type=str, help="FIPS code to build SuperParcel for. If not provided, will build for all FIPS codes found in config.json")
@click.option('-cp', type=click.Path(exists=False), help="Path to County Candidate Parcel shapefile. If not provided, will use config.json")
@click.option('-key', type=str, default='OWNER', help="Field to use for clustering. Default is 'OWNER'.")
@click.option('-dt', type=int, default=200, help="Distance threshold for clustering. Default is 200.")
@click.option('-ss', type=int, default=3, help="Minimum number of samples for clustering. Default is 3.")
@click.option('-at', type=int, default=None, help="Minimum area threshold for super parcel creation. Default is None.")
@click.option('-rs', is_flag=True, help="Return single parcels. Default is False.")
@click.option('-pb', type=click.Path(exists=False), help="Path to Place Boundaries Shapefile. FUTURE IMPLEMENTATION")
@click.pass_context
def sp1(ctx, bd, fips, cp, key, dt, ss, at, rs, pb):
    click.echo("_________________________________________________________")
    logger.info("BUILDING SuperParcel Phase 1")
    click.echo("-")
    click.echo("-")

    def check_paths(*args):
        for arg in args:
            if not arg:
                raise logger.info(f'Parameter is missing.')
            else:
                if not os.path.exists(arg):
                    raise logger.error(f"File does not exist: {arg}")

    def build_filename(prefix, arg_delimeter, suffix, *args):
        """
        Build a filename from a list of arguments
        Example: build_filename('spfixed', '-', 'dbscan', 200, 3, 300_000)
        Returns: 'spfixed-200-3-300_000-dbscan'
        """

        args_str = arg_delimeter.join(map(str, args))
        return f"{prefix}-{args_str}-{suffix}"


    if pb:
        logger.info(f'Running with Place Boundaries')
        pass # implement later

    try:
        if cp: # user input single county file to run process
            if not fips:
                raise logger.error(f"No FIPS code provided.")
            # start reading immediately
            logger.info(f'Reading {os.path.basename(cp)}...')
            check_paths(cp, bd)
            output_dir = os.path.join(bd, 'outputs', f'{fips}')
            os.makedirs(output_dir, exist_ok=True)

            parcels = gpd.read_file(cp)

            if rs: # return singles
                superparcels, singles = build_sp_fixed(
                    parcels=parcels,
                    fips=fips,
                    key_field=key,
                    distance_threshold=dt,
                    sample_size=ss,
                    area_threshold=at,
                    return_singles=True
                )
                logger.info('Writing files...')
                if len(superparcels) > 0:
                    if at is not None:
                        fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}', f'at{at}')
                    else:
                        fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}')

                    superparcels.to_file(os.path.join(output_dir, f'{fn}.shp'))
                else:
                    raise logger.error(f"No super parcels created.")
                        

                if len(singles) > 0:
                    if at is not None:
                        fn = build_filename(f'singles_{fips}', '-', f'dt{dt}', f'ss{ss}', f'at{at}')
                    else:
                        fn = build_filename(f'singles_{fips}', '-', f'dt{dt}', f'ss{ss}')

                    singles.to_file(os.path.join(output_dir, f'{fn}.shp'))

                else:
                    raise logger.error(f"No single parcels created.")

            else: # no return singles
                superparcels = build_sp_fixed(
                    parcels=parcels,
                    fips=fips,
                    key_field=key,
                    distance_threshold=dt,
                    sample_size=ss,
                    area_threshold=at
                )
                logger.info('Writing files...')
                if len(superparcels) > 0:
                    logger.debug(f'Super Parcel Count: {len(superparcels)}')
                    if at is not None:
                        fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}', f'at{at}')
                    else:
                        fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}')

                    superparcels.to_file(os.path.join(output_dir, f'{fn}.shp'))
                else:
                    raise logger.error(f"No super parcels created.")
        else:
            # read from config
            config = json.load(open(ctx.obj["CONFIG"], 'r'))
            logger.debug(f"Config: {config}")
            cp_fi_list = config["CANDIDATE_PARCELS"]
            bd = config["BUILD_DIR"]

            check_paths(bd)
            
            # check if provided FIPS
            if fips:
                cp_fi_list = [fi for fi in cp_fi_list if fips in fi]

            if len(cp_fi_list) == 0:
                raise logger.error(f"No candidate parcels found in config file.")
            
            for fi in cp_fi_list:
                check_paths(fi)
                        
            logger.debug(f"Files: {cp_fi_list}")
            logger.debug(f"Build Directory: {bd}")
                
            for fi in cp_fi_list:
                logger.info(f'Processing {os.path.basename(fi)}...')
                fips = os.path.basename(fi).split('_')[2].split('.')[0]
                output_dir = os.path.join(bd, 'outputs', f'{fips}')
                os.makedirs(output_dir, exist_ok=True)

                parcels = gpd.read_file(fi)
                if rs: # return singles
                    super_parcels, singles = build_sp_fixed(
                        parcels=parcels,
                        fips=fips,
                        key_field=key,
                        distance_threshold=dt,
                        sample_size=ss,
                        at=at,
                        return_singles=True
                    )
                    logger.info('Writing files...')
                    click.echo("-")
                    click.echo("-")
                    if len(super_parcels) > 0:
                        if at is not None:
                            fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}', f'at{at}')
                        else:
                            fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}')

                        super_parcels.to_file(os.path.join(output_dir, f'{fn}.shp'))
                    else:
                        raise logger.error(f"No super parcels created.")
                
                    if len(singles) > 0:
                        if at is not None:
                            fn = build_filename(f'singles_{fips}', '-', f'dt{dt}', f'ss{ss}', f'at{at}')
                        else:
                            fn = build_filename(f'singles_{fips}', '-', f'dt{dt}', f'ss{ss}')

                        singles.to_file(os.path.join(output_dir, f'{fn}.shp'))
                    else:
                        raise logger.error(f"No single parcels created.")

                else: # no return singles
                    super_parcels = build_sp_fixed(
                        parcels=parcels,
                        fips=fips,
                        key_field=key,
                        distance_threshold=dt,
                        sample_size=ss,
                        area_threshold=at,
                        return_singles=False
                    )
                    logger.info('Writing files...')
                    click.echo("-")
                    click.echo("-")
                    if len(super_parcels) > 0:
                        if at is not None:
                            fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}', f'at{at}')
                        else:
                            fn = build_filename(f'spfixed_{fips}', '-', f'dt{dt}', f'ss{ss}')

                        super_parcels.to_file(os.path.join(output_dir, f'{fn}.shp'))
                    
                    else:
                        raise logger.error(f"No super parcels created.")
                
    except Exception as e:
        raise logger.error(f"PROCESS ERROR: {e}")
        
    logger.info("BUILD COMPLETE.")
    click.echo("_________________________________________________________")
         
