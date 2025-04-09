import os
import sys
import glob
import click
import pandas as pd
import geopandas as gpd
import json
import logging
from datetime import datetime, timezone
from sp_cli.helper import (
    load_config, 
    save_config, 
    parse_to_int_list, 
    parse_to_str_list, 
    parse_key_value,
    create_batches,
    process_batch
)

# Configure the root logger
logger = logging.getLogger(__name__)
logger.handlers.clear()


@click.command(help="Builds config json file for SuperParcel build.")
@click.option('-js', '--json-key', type=click.Path(exists=False), help="Path to GCP JSON key file.")
@click.option('-fips', '--county-fips', multiple=True, type=str, default=None, help="County FIPS to build. Comma-seperated. No Spaces.", callback=parse_to_str_list)
@click.option('-bd', '--build-dir', type=click.Path(exists=False), help="Local directory for build.")
@click.option('-see', is_flag=True, default=False, help="See config.json file.")
@click.option('-update', multiple=True, default=None, help="Update config.json file. Key Value pairs seperated by '='. No Spaces.", callback=parse_key_value)
@click.pass_context
def config(ctx, build_dir, json_key, county_fips, see, update):
    if see:
        try:
            config = load_config(ctx.obj["CONFIG"])
            click.echo(ctx.obj["CONFIG"])
            click.echo(config)
            sys.exit()
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {ctx.obj['CONFIG']}.")
            sys.exit()

    if update:
        try:
            config = load_config(ctx.obj["CONFIG"])
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {ctx.obj['CONFIG']}.")
            sys.exit()

        for key, value in update.items():
            if key in config:
                config[key] = value
            else:
                logger.error(f"Key {key} not found in configuration file.")
                sys.exit()

        try:
            save_config(ctx.obj["CONFIG"], config)
            logger.info(f"Configuration file updated at {ctx.obj['CONFIG']}.")
            sys.exit()
        except Exception as e:
            logger.error(f"Failed to update configuration file: {e}")
            sys.exit()


    click.echo("_________________________________________________________")
    logger.info("SETTING UP SuperParcel Build")
    click.echo("-")
    click.echo("-")    

    input_subdir = os.path.join(build_dir, "inputs")
    output_subdir = os.path.join(build_dir, "outputs")
    analysis_subdir = os.path.join(build_dir, "analysis")
    os.makedirs(input_subdir, exist_ok=True)
    os.makedirs(output_subdir, exist_ok=True)
    os.makedirs(analysis_subdir, exist_ok=True)

    config = {}
    config["BUILD_DIR"] = build_dir
    config["INPUT_DIR"] = input_subdir
    config["OUTPUT_DIR"] = output_subdir
    config["ANALYSIS_DIR"] = analysis_subdir
    config["GCP_JSON"] = json_key
    config["GCP_PROJECT"] = 'clgx-gis-app-dev-06e3'
    config["GCP_INPUT_DATASET"] = 'superparcels'
    config["GCP_INPUT_TABLE"] = 'short_query_pu_pipeline_candidate_parcels'
    config["GCP_OUTPUT_DATASET"] = 'superparcels'
    config["GCS_BUCKET"] = 'gs://geospatial-projects/super_parcels'
    config["FIPS_LIST"] = county_fips
    
    try:
        
        with open(ctx.obj["CONFIG"], "w") as config_file:
            json.dump(config, config_file, indent=4)
        logger.info(f"Configuration file created at {ctx.obj['CONFIG']}.")
        
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
    help="Builds SuperParcel Phase 1. Highly recommended to run setup first to create config file."
)
@click.option('-fips', default=None, multiple=True, type=str,
              help="FIPS code(s) to build SuperParcel for. Comma-seperated. No Spaces. If not provided, will build for all FIPS codes found in config.json",
              callback=parse_to_str_list)
@click.option('-dt', '--dist-thres', default=None, multiple=True,
              help="Distance threshold list for clustering. Comma-seperated. No Spaces. Default is 200.", callback=parse_to_int_list)
@click.option('-ss', '--sample-size', type=int, default=3,
              help="Minimum number of samples for clustering. Default is 3.")
@click.option('-at', '--area-threshold', type=int, default=None,
              help="Minimum area threshold for super parcel creation. Default is None. NOT YET IMPLEMENTED.")
@click.option('-local', '--local-upload', type=click.BOOL, default=False,
              help="Saves build to local build directory. Default is False.")
@click.option('-bq', '--bq-upload', type=click.BOOL, default=True,
                help="Uploads to BigQueryTable. Default is True.")
@click.option('-bd', '--build-dir', type=click.Path(), default=None,
              help="Directory where you want the build to occur. If not provided, will use the build directory from config.json.",
              required=False)
@click.option('-qa', is_flag=True, default=False,
              help="Enables cProfiler. Default is False. NOT YET IMPLEMENTED.")
@click.option('-pb', type=click.Path(), default=None,
              help="Path to Place Boundaries Shapefile. FUTURE IMPLEMENTATION")
@click.pass_context
def spfixed(ctx, fips, dist_thres, sample_size, area_threshold, local_upload, bq_upload, build_dir, qa, pb):
    from sp_cli.helper import (
        check_paths, 
        sql_query,
        bigquery_to_gdf,
        build_sp_args,
    )
    from sp_cli.sp_build import build_sp_fixed
    
    click.echo("_________________________________________________________")
    logger.info("BUILDING SuperParcel Fixed Epsilon Phase 1")
    click.echo("-")
    click.echo("-")

   
    # Attempt to load configuration from file (if provided via ctx)
    if os.path.exists(ctx.obj["CONFIG"]):
        with open(ctx.obj["CONFIG"], "r") as config_file:
            config = json.load(config_file)
    else:
        logger.error('Cannot find config.json!!!')
        
    timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    version = ctx.obj["VERSION"]

    logger.debug(f"Timestamp: {timestamp}")
    logger.debug(f"Version: {version}")
 


    # Use user-provided build directory if available; otherwise, fall back to config.
    bd = build_dir or config.get("BUILD_DIR")
    if not bd:
        raise click.ClickException("Build directory must be provided as an argument (-bd) or in the config file.")
    # Create build directory if it does not exist.
    os.makedirs(bd, exist_ok=True)
    check_paths(bd)

    try:
        fips = fips or config.get("FIPS_LIST", [])
        input_dir = os.makedirs(os.path.join(bd, "inputs"), exist_ok=True) or config.get("INPUT_DIR")
        local_output_dir = os.makedirs(os.path.join(bd, "outputs"), exist_ok=True) or config.get("OUTPUT_DIR")
        logger.debug(f"FIPS List: {fips}")
    except KeyError:
        raise click.ClickException("FIPS code(s) must be provided as an argument (-fips) or in the config file.")    
            
    bq_input_path = f"{config.get('GCP_PROJECT')}.{config.get('GCP_INPUT_DATASET')}.{config.get('GCP_INPUT_TABLE')}"
    bq_output_path = f"{config.get('GCP_PROJECT')}.{config.get('GCP_OUTPUT_DATASET')}"
    json_key = config.get("GCP_JSON")

    logger.debug(f"BigQuery Input Path: {bq_input_path}")
    logger.debug(f"BigQuery Output Path: {bq_output_path}")
    logger.debug(f"JSON Key: {json_key}")
    # Process Place Boundaries if provided (future implementation)
    if pb:
        logger.info("Running with Place Boundaries (feature not yet implemented).")
        # TODO: Implement processing with place boundaries.


    # CANDIDATE SQL QUERY
    query = sql_query(
        path=bq_input_path,
        fips_list=fips
    )
    logger.debug(f"SQL Query: {query}")

    try:
        candidate_gdf = bigquery_to_gdf(
            json_key=json_key,
            sql_query=query
        )

        if local_upload: # write input to input_dir
            input_path = os.path.join(input_dir, 'candidate_input.shp')
            candidate_gdf.to_file(input_path, driver='ESRI Shapefile')
    except Exception as e:
        raise logger.error(f"Failed to pull data from BigQuery: {e}")


    # get KEY OWNER & FIPS PUID FIELD
    owner_field = next((col for col in candidate_gdf.columns if 'owner' in col.lower()), None)
    fips_field = next((col for col in candidate_gdf.columns if 'fips' in col.lower()), None)
    puid_field = next((col for col in candidate_gdf.columns if 'puid' in col.lower()), None)
    logger.debug(f"Owner Field: {owner_field}")
    logger.debug(f"FIPS Field: {fips_field}")
    logger.debug(f"PUID Field: {puid_field}")
    logger.debug(f'PUID dtype: {candidate_gdf[puid_field].dtype}')

    logger.debug(f"Candidate GeoDataFrame Columns: {candidate_gdf.columns}")
    logger.debug(f"Candidate GeoDataFrame: {candidate_gdf.head(2)}")
    logger.debug(f"Candidate GeoDataFrame CRS: {candidate_gdf.crs}")
    logger.debug(f"Candidate GeoDataFrame Length: {len(candidate_gdf)}")
    
    if owner_field is None:
        raise logger.error("No owner field found in the candidate GeoDataFrame.")
    
    if fips_field is None:
        raise logger.error("No FIPS field found in the candidate GeoDataFrame.")
    
    logger.debug(f"Owner Field: {owner_field}")
    logger.debug(f"FIPS Field: {fips_field}")

    # list of tuples for each arg combination
    sp_args = build_sp_args(
        candidate_gdf=candidate_gdf,
        fips_field=fips_field, # arg 1
        owner_field=owner_field, # arg 2
        dist_thres=dist_thres, # arg 3
        sample_size=sample_size, # arg 4
        area_threshold=area_threshold, # arg 5
        timestamp=timestamp, # arg 6
        version=version, # arg 7
        bq_output_dir=bq_output_path, # arg 8
        local_output_dir=local_output_dir, # arg 9
        bq_upload=bq_upload, # arg 10
        local_upload=local_upload, # arg 11
        json_key=json_key # arg 12
    )

    logger.debug(f"SP Args Example Tuple: {sp_args[0]}")
    logger.info(f"Number of SuperParcel Iterations: {len(sp_args)}")

    batch_size = min(len(sp_args), 10)  # Set batch size to 10 or the number of args, whichever is smaller
    logger.info(f'Running {batch_size} concurrent processes')
  
    # RUN SUPERPARCEL BUILD
    click.echo("-")
    click.echo("-")
    logger.info(f'STARTING SUPERPARCEL BUILD')
    click.echo("-")
    click.echo("-")
    process_batch(build_sp_fixed, sp_args, pool_size=batch_size)

    
    logger.info("BUILD COMPLETE.")
    click.echo("_________________________________________________________")

@click.command(
    help="Build Exploratory Analysis for Distance Thresholds. IN-DEVELOPMENT"
)
@click.option('-fips', default=None, multiple=True, type=str,
              help="FIPS code(s) to build SuperParcel for. Comma-seperated. No Spaces. If not provided, FIPS codes will be pulled from config.json", 
              callback=parse_to_str_list)
@click.option('-dt', '--dist-thres', default=None, multiple=True,
              help="Distance threshold list for comparative analysis. Comma-seperated. No Spaces. Default is None.", callback=parse_to_int_list, required=True)
@click.option('-pull', '--pull-data', type=click.Choice(['bigquery', 'local']),
              help="Choices to pull input data from: 'bigquery' or 'local'")
@click.pass_context
def dt_analysis(ctx, fips, dist_thres, pull_data):
    from sp_cli.helper import (
        check_paths, 
        sql_query,
        bigquery_to_gdf
    )
    from sp_geoprocessing.analysis import dt_owner_counts, dt_overlap, dt_area_ratio
    
    click.echo("-")
    click.echo("-")
    logger.info("BUILDING Distance Threshold Comparison Analysis")
    click.echo("-")
    click.echo("-")
    click.echo("_________________________________________________________")

    # Attempt to load configuration from file (if provided via ctx)
    if os.path.exists(ctx.obj["CONFIG"]):
        with open(ctx.obj["CONFIG"], "r") as config_file:
            config = json.load(config_file)
    else:
        logger.error('Cannot find config.json!!!')

    
    shp_dir = config.get("OUTPUT_DIR")
    fips_list = config.get("FIPS_LIST")
    output_dir = config.get("ANALYSIS_DIR")

    version = ctx.obj["VERSION"]
    json_key = config.get("GCP_JSON")

    if pull_data == 'local':
        sys.exit("Local data pull not yet implemented.")


    if pull_data == 'bigquery':
        fips = fips or config.get("FIPS_LIST", [])
        bq_input_dataset = f"{config.get('GCP_PROJECT')}.{config.get('GCP_INPUT_DATASET')}"
        bq_table_prefix = 'spfixed-ss3-dt'

        all_gdfs = gpd.GeoDataFrame()
        for dt in dist_thres:
            bq_input_path = bq_input_dataset + '.' + bq_table_prefix + str(dt)

            # CANDIDATE SQL QUERY
            query = sql_query(
                path=bq_input_path,
                fips_list=fips
            )
            logger.debug(f"SQL Query: {query}")

            try:
                gdf = bigquery_to_gdf(
                    json_key=json_key,
                    sql_query=query
                )
                gdf['dt'] = dt

                all_gdfs = pd.concat([all_gdfs, gdf], ignore_index=True)
            except Exception as e:
                raise logger.error(f"Failed to pull data from BigQuery: {e}")
        
    all_owner_counts = pd.DataFrame()
    all_dt_overlaps = pd.DataFrame()
    all_dt_area_ratios = pd.DataFrame()
    for fips in all_gdfs['fips'].unique():
        filter_gdf = all_gdfs[all_gdfs['fips'] == fips] 
        logger.info(f'Processing FIPS: {fips}...')
        

        # run owner counts for each distance threshold
        logger.info('Running owner counts...')
        owner_counts = dt_owner_counts(
            gdf=filter_gdf, 
            group_field='owner'
        )
        all_owner_counts = pd.concat([all_owner_counts, owner_counts], axis=0)
        
        # run overlap analysis for each distance threshold
        logger.info('Running overlap analysis...')
        dt_overlaps = dt_overlap(
            gdf=filter_gdf,
            sp_id_field='sp_id',
            owner_field='owner'
        )
        all_dt_overlaps = pd.concat([all_dt_overlaps, dt_overlaps], axis=0)

        # run area ratio analysis
        logger.info('Running area ratio analysis...')
        dt_area_ratio_df = dt_area_ratio(
            gdf=filter_gdf,
            area_field='area_ratio'
        )
        #logger.debug(f'Area Ratio DataFrame: {dt_area_ratio_df.head(1)}')
        all_dt_area_ratios = pd.concat([all_dt_area_ratios, dt_area_ratio_df], ignore_index=True)

    logger.info('Writing files...')
    owner_count_out_path = os.path.join(output_dir, 'owner_count_analysis.csv')
    dt_overlap_out_path = os.path.join(output_dir, 'dt_overlap_analysis.csv')
    dt_area_ratio_out_path = os.path.join(output_dir, 'dt_area_ratio_analysis.csv')
    all_owner_counts.to_csv(owner_count_out_path)
    all_dt_overlaps.to_csv(dt_overlap_out_path)
    all_dt_area_ratios.to_csv(dt_area_ratio_out_path)

    click.echo('-')
    click.echo('-')
    click.echo('DT ANALYSIS COMPLETE.')
    click.echo('-')
    click.echo('-')
    click.echo("_________________________________________________________")



         
