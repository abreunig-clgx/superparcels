import os
import sys
import glob
import click
import pandas as pd
import geopandas as gpd
import json
import logging
from datetime import datetime, timezone
from build import build_sp_fixed
from helper import parse_to_int_list, parse_to_str_list

# Configure the root logger
logger = logging.getLogger(__name__)

@click.command(help="Builds config json file for SuperParcel build.")
@click.option('-js', '--json-key', type=click.Path(exists=False), help="Path to GCP JSON key file.", required=True)
@click.option('-fips', '--county-fips', multiple=True, type=str, default=None, help="County FIPS to build. Comma-seperated. No Spaces.", required=True, callback=parse_to_str_list)
@click.option('-bd', '--build-dir', type=click.Path(exists=False), help="Local directory for build.", required=True)
@click.pass_context
def config(ctx, build_dir, json_key, county_fips):
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
    config["GCP_INPUT_DATASET"] = 'clgx_gis_dev'
    config["GCP_INPUT_TABLE"] = 'parcel_view_1'
    config["GCP_OUTPUT_DATASET"] = 'boundary_poc'
    config["GCP_OUTPUT_TABLE"] = 'sp'
    config["GCS_BUCKET"] = 'gs://geospatial-projects/super_parcels'
    config["FIPS_LIST"] = county_fips
    
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
@click.option('-local', '--local-upload', is_flag=True, default=False,
              help="Uploads to local directory instead of GCS. Default is False.")
@click.option('-bq', '--bq-upload', is_flag=True, default=True,
                help="Uploads to BigQueryTable. Default is True.")
@click.option('-bd', '--build-dir', type=click.Path(), default=None,
              help="Directory where you want the build to occur. If not provided, will use the build directory from config.json.",
              required=False)
@click.option('-qa', is_flag=True, default=False,
              help="Enables cProfiler. Default is False. NOT YET IMPLEMENTED.")
@click.option('-pb', type=click.Path(), default=None,
              help="Path to Place Boundaries Shapefile. FUTURE IMPLEMENTATION")
@click.pass_context
def sp1(ctx, fips, dist_thres, sample_size, area_threshold, local_upload, bq_upload, build_dir, qa, pb):
    from helper import (
        check_paths, 
        build_filename,
        sql_query,
        bigquery_to_gdf,
        gdf_to_bigquery,
        build_sp_args,
        get_git_commit_hash

    )
    from build import build_sp_fixed
    from sp_geoprocessing.tools import create_batches, mp_framework
    click.echo("_________________________________________________________")
    logger.info("BUILDING SuperParcel Phase 1")
    click.echo("-")
    click.echo("-")

    # Attempt to load configuration from file (if provided via ctx)
    if os.path.exists(ctx.obj["CONFIG"]):
        with open(ctx.obj["CONFIG"], "r") as config_file:
            config = json.load(config_file)
    else:
        logger.error('Cannot find config.json!!!')
        
    timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    commit_hash = get_git_commit_hash()

    # Use user-provided build directory if available; otherwise, fall back to config.
    bd = build_dir or config.get("BUILD_DIR")
    if not bd:
        raise click.ClickException("Build directory must be provided as an argument (-bd) or in the config file.")
    # Create build directory if it does not exist.
    os.makedirs(bd, exist_ok=True)
    check_paths(bd)

    try:
        fips = fips or config.get("FIPS_LIST", [])
    except KeyError:
        raise click.ClickException("FIPS code(s) must be provided as an argument (-fips) or in the config file.")    
            
    bq_input_path = f"{config.get('GCP_PROJECT')}.{config.get('GCP_INPUT_DATASET')}.{config.get('GCP_INPUT_TABLE')}"
    bq_output_path = f"{config.get('GCP_PROJECT')}.{config.get('GCP_OUTPUT_DATASET')}"
    json_key = config.get("GCP_JSON")

    # Process Place Boundaries if provided (future implementation)
    if pb:
        logger.info("Running with Place Boundaries (feature not yet implemented).")
        # TODO: Implement processing with place boundaries.

    print(type(fips))
    # CANDIDATE SQL QUERY
    query = sql_query(
        path=bq_input_path,
        fips_list=fips
    )
    logger.info(f"SQL Query: {query}")
    try:
        candidate_gdf = bigquery_to_gdf(
            json_key=json_key,
            sql_query=query
        )
    except Exception as e:
        raise logger.error(f"Failed to pull data from BigQuery: {e}")


    # get KEY OWNER FIELD
    owner_field = next((col for col in candidate_gdf.columns if 'owner' in col.lower()), None)
    fips_field = next((col for col in candidate_gdf.columns if 'fips' in col.lower()), None)

   
    sp_args = build_sp_args(
        candidate_gdf=candidate_gdf,
        fips_field=fips_field,
        owner_field=owner_field,
        dist_thres=dist_thres,
        sample_size=sample_size,
        area_threshold=area_threshold,
        output_dir=bq_output_path
    )

    if len(dist_thres) == 1:
        batch_size = min(len(fips), 15) # 15 is the max number of jobs that can be run in parallel
    else:
        batch_size = len(dist_thres) # group all distance thresholds together ([fip1-dt1, fip2-dt1, fip3-dt1], [fip1-dt2, fip2-dt2, fip3-dt2], etc.)
    batches = list(create_batches(sp_args, batch_size))

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
        logger.info(f'Sample Size: {ss_ids}')
        logger.info(f'Area Threshold: {at_ids}')
        logger.info(f'Output Directories: {batch_output_dirs}')

        results = mp_framework(build_sp_fixed, batch, n_jobs=batch_size) # insert all args minus output directory

        for i, result in enumerate(results):
            if result is None or len(result) == 0:
                logger.error(f"No results for batch {i}. Skipping...")
                continue

            # Check if the output directory exists
            output_dir = batch_output_dirs[i]
            _fips = batch_ids[i]
            _dt = dt_ids[i]
            _ss = ss_ids[i]
            _at = at_ids[i]

            # add timstamp field to result
            result['timestamp'] = timestamp
            

            logger.info(f'{result.head(1)}')

        
        for id, result in enumerate(results):
            result_output_dir = batch_output_dirs[id]
            if result is not None and len(result) > 0:
                _fips = batch_ids[id]
                _dt = dt_ids[id]
                _ss = ss_ids[id]    
                _at = at_ids[id]
                
                result['timestamp'] = timestamp

                if _at is not None:
                    fn = build_filename(f'spfixed_{commit_hash}', '-', f'dt{_dt}', f'ss{_ss}', f'at{_at}')
                else:
                    fn = build_filename(f'spfixed_{commit_hash}', '-', f'dt{_dt}', f'ss{_ss}')


                output_table_name = f'{output_dir}.{fn}'
                logger.info(f"Output table name for FIPS {_fips}: {output_table_name}")
                
                gdf_to_bigquery(
                    gdf=result,
                    table_name=output_table_name,
                    json_key=json_key,
                    write_type='WRITE_APPEND'
                )
                logger.info(f'Upload to BigQuery successful.')
            else:    
                logger.error(f"No super parcels created for FIPS {_fips}.")
    

    logger.info("BUILD COMPLETE.")
    click.echo("_________________________________________________________")

@click.command(
    help="Build Exploratory Analysis for Distance Thresholds. IN-DEVELOPMENT"
)
@click.pass_context
def dt_analysis(ctx):
    from sp_geoprocessing.analysis import dt_owner_counts, dt_overlap
    
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

    all_owner_counts = pd.DataFrame()
    all_dt_overlaps = pd.DataFrame()
    for fips in fips_list:
        logger.info(f'Processing FIPS: {fips}...')
        try:
            all_dts = glob.glob(os.path.join(shp_dir, fips, '*.shp'), recursive=True)
        except:
            raise ValueError('No shapefiles found in the specified directory')
            


        # extract distance thresholds from filenames
        dt_names = []
        for dt in all_dts:
            dt_name = os.path.basename(dt).split('-')[-1].split('.')[0].split('dt')[-1]
            # sort by distance threshold
            dt_names.append(int(dt_name))
        dt_names.sort()


        # run owner counts for each distance threshold
        logger.info('Running owner counts...')
        owner_counts = dt_owner_counts(
            data_dir=shp_dir, 
            fips=fips,
            dt_values=dt_names, 
            group_field='owner'
        )
        all_owner_counts = pd.concat([all_owner_counts, owner_counts], axis=0)
        
        # run overlap analysis for each distance threshold
        dt_overlaps = dt_overlap(
            data_dir=shp_dir,
            fips=fips,
            dt_values=dt_names,
            sp_id_field='sp_id',
            owner_field='owner'
        )
        all_dt_overlaps = pd.concat([all_dt_overlaps, dt_overlaps], axis=0)

    logger.info('Writing files...')
    owner_count_out_path = os.path.join(shp_dir, 'owner_count_analysis.csv')
    dt_overlap_out_path = os.path.join(shp_dir, 'dt_overlap_analysis.csv')
    all_owner_counts.to_csv(owner_count_out_path)
    all_dt_overlaps.to_csv(dt_overlap_out_path)

    click.echo('-')
    click.echo('-')
    click.echo('DT ANALYSIS COMPLETE.')
    click.echo('-')
    click.echo('-')
    click.echo("_________________________________________________________")



         
