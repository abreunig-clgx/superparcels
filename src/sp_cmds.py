import os
import sys
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
from sp_geoprocessing.tools import to_int_list

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
@click.option('-csv', type=click.Path(exists=False), help="Path to Candidate Parcel csv. Local or GCS path.")
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
    config['FIPS_LIST'] = fips_list
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
    help="Builds SuperParcel Phase 1. Highly recommended to run setup first to create config file. If not, provide the necessary parameters."
)
@click.option('-bd', type=click.Path(), default=None,
              help="Directory where you want the build to occur.")
@click.option('-fips', default=None,
              help="FIPS code(s) to build SuperParcel for. If not provided, will build for all FIPS codes found in config.json",
              callback=parse_list)
@click.option('-cp', type=click.Path(), default=None,
              help="Path to County Candidate Parcel shapefile. If not provided, will use config.json")
@click.option('-key', type=str, default='OWNER',
              help="Field to use for clustering. Default is 'OWNER'.")
@click.option('-dt', default=['200'],
              help="Distance threshold list for clustering. Default is 200.", callback=to_int_list)
@click.option('-mp', type=int, default=None,
              help="Batch size for multiprocessing. Default is None.")
@click.option('-ss', type=int, default=3,
              help="Minimum number of samples for clustering. Default is 3.")
@click.option('-at', type=int, default=None,
              help="Minimum area threshold for super parcel creation. Default is None.")
@click.option('-qa', is_flag=True, default=False,
              help="Enables cProfiler. Default is False.")
@click.option('-pb', type=click.Path(), default=None,
              help="Path to Place Boundaries Shapefile. FUTURE IMPLEMENTATION")
@click.pass_context
def sp1(ctx, bd, fips, cp, key, dt, mp, ss, at, qa, pb):
    from sp_geoprocessing.tools import create_batches, mp_framework
    click.echo("_________________________________________________________")
    logger.info("BUILDING SuperParcel Phase 1")
    click.echo("-")
    click.echo("-")

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

    # Attempt to load configuration from file (if provided via ctx)
    if os.path.exists(ctx.obj["CONFIG"]):
        with open(ctx.obj["CONFIG"], "r") as config_file:
            config = json.load(config_file)
    else:
        logger.error('Cannot find config.json!!!')

    # Use user-provided build directory if available; otherwise, fall back to config.
    bd = bd or config.get("BUILD_DIR")
    if not bd:
        raise click.ClickException("Build directory must be provided as an argument (-bd) or in the config file.")
    # Create build directory if it does not exist.
    os.makedirs(bd, exist_ok=True)
    check_paths(bd)

    # Determine candidate parcel file(s)
    cp_files = []
    if cp:
        # User provided a single candidate file.
        cp_files = [cp]
        if not fips:
            raise click.ClickException("When using a single county file (-cp), please provide a FIPS code (-fips).")
    else:
        # Fall back to config candidate parcel files.
        cp_files = config.get("CANDIDATE_PARCELS", [])
        if not fips:
            try:
                fips = config.get("FIPS_LIST", [])
            except KeyError:
                raise click.ClickException("FIPS code(s) must be provided as an argument (-fips) or in the config file.")
        if not cp_files:
            raise click.ClickException("No candidate parcels provided via argument (-cp) or found in the config file.")
            
        
        # filter candidate parcel files by FIPS code(s)
        cp_files = [cp for cp in cp_files if any(fip in
                                                  os.path.basename(cp) for fip in fips)]
        if not cp_files:
            raise click.ClickException("No candidate parcel files match the provided FIPS code(s).")
        
    # Process Place Boundaries if provided (future implementation)
    if pb:
        logger.info("Running with Place Boundaries (feature not yet implemented).")
        # TODO: Implement processing with place boundaries.

    # Process each candidate file
    sp_args = []
    for fi in cp_files:
        check_paths(fi)
        matching_fips = [fip for fip in fips if fip in fi]
        if not matching_fips:
            raise click.ClickException(f"Could not determine FIPS code from file: {fi}")
        current_fips = matching_fips[0]
        logger.info(f'Collection {os.path.basename(fi)} for FIPS {current_fips}...')
        output_dir = os.path.join(bd, 'outputs', current_fips)
        os.makedirs(output_dir, exist_ok=True)
        
        # Build sp_args for each distance threshold for the current file
        for dist_thresh in dt:
            sp_args.append((fi, current_fips, key, dist_thresh, ss, at, qa, output_dir))

    
    if not mp:
        # loop through each distance threshold
        for args in sp_args:
            superparcels = build_sp_fixed(
                parcels=args[0],
                fips=args[1],
                key_field=args[2],
                distance_threshold=args[3],
                sample_size=args[4],
                area_threshold=args[5],
                qa=args[6],
            )
            logger.info('Writing files...')
            if superparcels is not None and len(superparcels) > 0:
                _fips = args[1]
                _dt = args[3]
                _ss = args[4]
                _at = args[5]

                if _at is not None:
                    fn = build_filename(f'spfixed_{_fips}', '-', f'dt{_dt}', f'ss{_ss}', f'at{_at}')
                else:
                    fn = build_filename(f'spfixed_{_fips}', '-', f'dt{_dt}', f'ss{_ss}')

                out_path = os.path.join(args[-1], f'{fn}.shp') # last arg is output directory
                superparcels.to_file(out_path)
                logger.info(f"Wrote super parcels to {out_path}")
            else:
                logger.error(f"No super parcels created for FIPS {_fips}.")

    else:
        
        batches = list(create_batches(sp_args, mp))
        for batch in batches:
            logger.info('______________________')
            batch_ids = [args[1] for args in batch]  # Using FIPS from the tuple
            dt_ids = [args[3] for args in batch]  # Using distance threshold from the tuple
            ss_ids = [args[4] for args in batch]  # Using sample size from the tuple
            at_ids = [args[5] for args in batch]  # Using area threshold from the tuple
            batch_output_dirs = [args[-1] for args in batch]  # Using output directory from the tuple
            batch = [args[:-1] for args in batch]  # Removing output directory from the tuple

            logger.info(f'Processing Batch: {batch_ids}')
            logger.info(f'Distance Thresholds: {dt_ids}')

            results = mp_framework(build_sp_fixed, batch, n_jobs=mp) # insert all args minus output directory

            for id, result in enumerate(results):
                result_output_dir = batch_output_dirs[id]
                if result is not None and len(result) > 0:
                    _fips = batch_ids[id]
                    _dt = dt_ids[id]
                    _ss = ss_ids[id]
                    _at = at_ids[id]

                    if _at is not None:
                        fn = build_filename(f'spfixed_{_fips}', '-', f'dt{_dt}', f'ss{_ss}', f'at{_at}')
                    else:
                        fn = build_filename(f'spfixed_{_fips}', '-', f'dt{_dt}', f'ss{_ss}')

                    out_path = os.path.join(result_output_dir, f'{fn}.shp')
                    result.to_file(out_path)
                    logger.info(f"Wrote super parcels to {out_path}")
                else:    
                    logger.error(f"No super parcels created for FIPS {_fips}.")
        

    logger.info("BUILD COMPLETE.")
    click.echo("_________________________________________________________")

@build.command(
    help="Build Exploratory Analysis for Distance Thresholds. IN-DEVELOPMENT"
)
@click.option('-fips', type=str, default=None,
                help="FIPS code(s) to build SuperParcel for. If not provided, will build for all FIPS codes found in config.json")
@click.option('-dd', '--data-dir', type=click.Path(), default=None,
                help="Root directory where data is located.")
@click.pass_context
def dtepa(ctx, fips, data_dir):
    from sp_geoprocessing.build import dt_exploratory_analysis
    click.echo("_________________________________________________________")
    logger.info("BUILDING Distance Threshold Explroatory Analysis")
    click.echo("-")
    click.echo("-")

    # Attempt to load configuration from file (if provided via ctx)
    if os.path.exists(ctx.obj["CONFIG"]):
        with open(ctx.obj["CONFIG"], "r") as config_file:
            config = json.load(config_file)
    else:
        logger.error('Cannot find config.json!!!')


    dt_exploratory_analysis(fips=fips, data_dir=data_dir)

         
