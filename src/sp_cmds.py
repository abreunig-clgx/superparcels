import os
import sys
import glob
import click
import pandas as pd
import geopandas as gpd
import json
import logging
from datetime import datetime
from build import build_sp_fixed
from helper import parse_to_int_list, parse_to_str_list

# Configure the root logger
logger = logging.getLogger(__name__)

@click.command(help="Builds config json file for SuperParcel build.")
@click.option('-bd', '--build-dir', type=click.Path(exists=False), help="Directory where you want the build to occur.", required=True)
@click.option('-js', '--json-key', type=click.Path(exists=False), help="Path to GCP JSON key file.", required=True)
@click.option('-p', '--project', type=str, help="GCP Project ID.", required=True)
@click.option('-d', '--dataset', type=str, help="BigQuery Dataset ID.", required=True)
@click.option('-t', '--table', type=str, help="BigQuery Table ID.", required=True)
@click.option('-s', '--storage', type=str, help="GCS Storage Bucket.", required=True)
@click.option('-fips', '--county-fips', multiple=True, type=str, default=None, help="County FIPS to build. Comma-seperated. No Spaces.", required=True, callback=parse_to_str_list)
@click.pass_context
def config(ctx, build_dir, json_key, project, dataset, table, storage, county_fips):
    click.echo("_________________________________________________________")
    logger.info("SETTING UP SuperParcel Build")
    click.echo("-")
    click.echo("-")    

    from helper import bigquery_to_gdf, sql_query 

    input_subdir = os.path.join(build_dir, "inputs")
    output_subdir = os.path.join(build_dir, "outputs")
    analysis_subdir = os.path.join(build_dir, "analysis")
    os.makedirs(input_subdir, exist_ok=True)
    os.makedirs(output_subdir, exist_ok=True)
    os.makedirs(analysis_subdir, exist_ok=True)
    logger.info(f"Created build directories: {input_subdir}, {output_subdir}, {analysis_subdir}")

    config = {}
    config["BUILD_DIR"] = build_dir
    config["INPUT_DIR"] = input_subdir
    config["OUTPUT_DIR"] = output_subdir
    config["ANALYSIS_DIR"] = analysis_subdir
    config["GCP_JSON"] = json_key
    config["GCP_PROJECT"] = project
    config["GCP_DATASET"] = dataset
    config["GCP_TABLE"] = table
    config["GCS_BUCKET"] = storage
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
@click.option('-bd', type=click.Path(), default=None,
              help="Directory where you want the build to occur. If not provided, will use the build directory from config.json.",
              required=False)
@click.option('-fips', default=None, multiple=True, type=str,
              help="FIPS code(s) to build SuperParcel for. If not provided, will build for all FIPS codes found in config.json",
              callback=parse_to_str_list)
@click.option('-dt', default=None, multiple=True,
              help="Distance threshold list for clustering. Default is 200.", callback=parse_to_int_list)
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
def sp1(ctx, bd, fips, dt, mp, ss, at, qa, pb):
    from helper import (
        check_paths, 
        build_filename,
        sql_query,
        bigquery_to_gdf
    )
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
        
    timestamp = datetime.now().strftime("%Y_%m_%d")
    # Use user-provided build directory if available; otherwise, fall back to config.
    bd = bd or config.get("BUILD_DIR")
    if not bd:
        raise click.ClickException("Build directory must be provided as an argument (-bd) or in the config file.")
    # Create build directory if it does not exist.
    os.makedirs(bd, exist_ok=True)
    check_paths(bd)

    try:
        fips = fips or config.get("FIPS_LIST", [])
    except KeyError:
        raise click.ClickException("FIPS code(s) must be provided as an argument (-fips) or in the config file.")    
            
    bq_path = f"{config.get('GCP_PROJECT')}.{config.get('GCP_DATASET')}.{config.get('GCP_TABLE')}"
    json_key = config.get("GCP_JSON")

    # Process Place Boundaries if provided (future implementation)
    if pb:
        logger.info("Running with Place Boundaries (feature not yet implemented).")
        # TODO: Implement processing with place boundaries.

    # Process each candidate file
    sp_args = []
    for county_fips in fips:
        logger.info(f"Processing FIPS: {county_fips}")
        county_query = sql_query(
            fips=county_fips,
            path=bq_path
        )

        gdf = bigquery_to_gdf(
            json_key=json_key,
            sql_query=county_query
        )

    
        output_dir = os.path.join(bd, 'outputs', timestamp, county_fips)
        os.makedirs(output_dir, exist_ok=True)
        
        # Build sp_args for each distance threshold for the current file
        for dist_thresh in dt:
            sp_args.append((gdf, county_fips, key, dist_thresh, ss, at, qa, output_dir))

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



         
