import click
import logging
from main_sp_fixed import build_sp_fixed

# Configure the root logger
logger = logging.getLogger(__name__)

@click.group(help="Setup Process for SuperParcel Build.")
@click.pass_context
def setup(ctx):
    if ctx.obj["VERBOSE"]:
        logger.setLevel(logging.DEBUG)

@setup.command(help="Creates config file for SuperParcel Build.")
@click.option('-bd', type=click.Path(exists=False), help="Directory where you want the build to occur.")
@click.option('-csv', type=click.Path(exists=False), help="Path to HUC shapefile.")
@click.option('-js', type=click.Path(exists=False), help="Path to GCP JSON key file.")
@click.pass_context
def cna(ctx, bd, csv, js):
    click.echo("_________________________________________________________")
    logger.info("SETTING UP SuperParcel Build")
    click.echo("-")
    click.echo("-")

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
    csv_path = download_from_gcs(js, csv, input_subdir) if huc.startswith("gs://") else huc
    pb_path = download_from_gcs(js, pb, input_subdir) if pb.startswith("gs://") else pb
    cts_path = download_from_gcs(js, cts, input_subdir) if cts.startswith("gs://") else cts


    # find zipfiles and extract in input_subdir
    zip_files = glob.glob(os.path.join(input_subdir, "*.zip"))
    if len(zip_files) != 0:
        for zip_file in zip_files:
            with ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(input_subdir)
            os.remove(zip_file)

    
    # Create configuration dictionary
    def get_paths(dir, input_type):
        """ Single HUC Shp, multiple preburn shps, single wfagg shp"""
        input_exts = {
            "HUC": {
                'regex': '*HUC*',
                'num_files': 'single', 
                'ext': ".shp"
            }, 
            "PREBURN": {
                'regex': '*Preburn*',
                'num_files': 'multiple', 
                'ext': ".shp"
            },
            "COUNTIES": {
                'regex': '*County*',
                'num_files': 'single', 
                'ext': ".shp"
            }
        }
        
        pattern = input_exts[input_type]['regex']
        num_files = input_exts[input_type]['num_files']
        extension = input_exts[input_type]['ext']
        path = glob.glob(os.path.join(dir, f"{pattern}{extension}"), recursive=True)

        if num_files == 'single':
            try:
                if len(path) == 1:
                    return path[0]
            except IndexError as e:
                raise logger.error(f"Failed to find {input_type} in {dir} with extension {extension} --- {e}")
        else:
            return path

    config = {}
    config["BUILD_DIR"] = bd
    # if has local directories else pull from GCS
    config["HUC"]       = get_paths(huc_path, "HUC") if huc_path else get_paths(input_subdir, "HUC")
    config["PREBURN"]   = get_paths(pb_path, "PREBURN") if pb_path else get_paths(input_subdir, "PREBURN")
    config["COUNTIES"]  = get_paths(cts_path, "COUNTIES")

    logger.debug(f"HUC Path: {huc_path}")
    logger.debug(f"Preburn Path: {pb_path}")
    logger.debug(f"Counties Path: {cts_path}")
    
    try:
        with open(ctx.obj["CONFIG"], "w") as config_file:
            json.dump(config, config_file, indent=4)
        logger.info(f"Configuration file created!")
    except Exception as e:
        raise logger.error(f"Failed to write config file: {e}")
    
    logger.info("SETUP COMPLETE.")
    click.echo("_________________________________________________________")
         
