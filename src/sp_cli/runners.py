import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.fileio import MatchFiles

from shapely.geometry import base
import pandas as pd
import geopandas as gpd
from shapely import wkt
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import json
import os
import subprocess
from sp_cli.sp_build import build_sp_multi_optimized
from sp_cli.helper import format_timestamp, format_version, build_filename
import logging

import os
import threading

logger = logging.getLogger(__name__)

class WriteFipsGeoParquet(beam.DoFn):
    def __init__(self, output_prefix):
        self.output_prefix = output_prefix  # e.g., geospatial-projects/super_parcels/geoparquet/staging

    def process(self, element):
        fips, records = element
        df = pd.DataFrame(records)

        df['geometry'] = df['geometry'].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')

        buf = BytesIO()
        gdf.to_parquet(buf, engine='pyarrow', compression='zstd')
        buf.seek(0)

        path = f"{self.output_prefix}/{fips}.parquet"
        with FileSystems.create(path) as f:
            f.write(buf.read())

        yield f"Wrote: {path}"

class RunSuperparcelCLI(beam.DoFn):
    def __init__(self, cli_options):
        self.output_dir = cli_options.get('output_dir')
        self.dist_thres = cli_options.get('dist_thres')
        self.area_threshold = cli_options.get('area_threshold')
        self.timestamp = cli_options.get('timestamp')
        self.version = cli_options.get('version')
      
    def process(self, filepath):
        fips = os.path.basename(filepath).split('.')[0]
        logger.info(f'\U0001F680 Starting {fips} in PID: {os.getpid()} | Thread: {threading.current_thread().name}')
        
        output_name = os.path.basename(filepath).replace('.parquet', '_sp.parquet')
        at_fn = str(self.area_threshold)[-1]  # get last digit of area threshold
        formatted_dts = '_'.join(map(str, self.dist_thres))
        build_type_subdir = build_filename('spmulti', '-', f"dt{formatted_dts}", f"ss3", f"at{at_fn}")
        fn_output_path = os.path.join(
            self.output_dir, 
            f'v{format_version(self.version)}', 
            format_timestamp(self.timestamp), 
            build_type_subdir,
            output_name)
        
        try:
            input_gdf = gpd.read_parquet(filepath)

            owner_field = next((col for col in input_gdf.columns if 'owner' in col.lower()), None)
            fips_field = next((col for col in input_gdf.columns if 'fips' in col.lower()), None)

            if owner_field is None or fips_field is None:
                raise logger.error(f"Owner or FIPS field not found in {filepath}")
            if input_gdf.empty:
                raise logger.error(f"Input GeoDataFrame is empty for {filepath}")
   
            result = build_sp_multi_optimized(
                parcels=input_gdf,
                fips=fips,
                key_field=owner_field,
                distance_thresholds=self.dist_thres,
                area_threshold=self.area_threshold,
            )
            result['timestamp'] = self.timestamp
            result['version'] = self.version

            def generate_bq_schema(df):
                type_mapping = {
                    'object': 'STRING',
                    'float64': 'FLOAT',
                    'int64': 'INTEGER',
                    'bool': 'BOOLEAN',
                    'datetime64[ns]': 'TIMESTAMP'
                }

                fields = []
                for col, dtype in df.dtypes.items():
                    dtype_str = str(dtype)
                    bq_type = type_mapping.get(dtype_str, 'STRING')
                    fields.append({
                        "name": col,
                        "type": bq_type,
                        "mode": "NULLABLE"
                    })

                return {"fields": fields}

            # After you finish the SuperParcel build
            schema = generate_bq_schema(result)

            # Save schema JSON
            schema_output_path = output_path.replace(".parquet", "_schema.json")
            with FileSystems.create(schema_output_path) as f:
                f.write(json.dumps(schema).encode('utf-8'))


            result.to_parquet(fn_output_path, engine='pyarrow', compression='zstd')
            logger.info(f"✅ Success for {fips}:\n{fn_output_path}")

        except subprocess.CalledProcessError as e:
            yield f"❌ Failure for {filepath}:\n{e.stderr}"


class ReadParquetAndConvertToDict(beam.DoFn):
    def process(self, file_path):
        with FileSystems.open(file_path) as f:
            buf = BytesIO(f.read())
            table = pq.read_table(buf)
            df = table.to_pandas()


            if 'geometry' in df.columns:
                df['geometry'] = df['geometry'].apply(
                    lambda geom: geom.wkt if isinstance(geom, base.BaseGeometry) else None
                )
            # Convert each row to a dict (one per output)
            for record in json.loads(df.to_json(orient="records")):
                yield record

    

def parquet2bigq_runner(input_glob, bq_output_table, pipeline_options):
    
    options = PipelineOptions(
            runner='DirectRunner',
            project='clgx-gis-app-dev-06e3',
            region='us-central1',
            temp_location='gs://geospatial-projects/super_parcels/geoparquet/temp',
            staging_location='gs://geospatial-projects/super_parcels/geoparquet/staging',
            job_name='parquet2bigq_runner',
            save_main_session=True,
            service_account_email='dataflow-service-account@clgx-gis-app-dev-06e3.iam.gserviceaccount.com'
        )
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Match output Parquets" >> beam.io.fileio.MatchFiles(input_glob)
            | "Get File Paths" >> beam.Map(lambda metadata: metadata.path)
            | "Read Parquet and Convert" >> beam.ParDo(ReadParquetAndConvertToDict())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=bq_output_table,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


def superparcel_runner(input_glob, pipeline_options, cli_options):
    
    options = PipelineOptions(
        runner='DirectRunner',  
        project='clgx-gis-app-dev-06e3',
        region='us-central1',
        temp_location='gs://geospatial-projects/super_parcels/geoparquet/temp',
        staging_location='gs://geospatial-projects/super_parcels/geoparquet/staging',
        job_name='superparcel_runner',
        save_main_session=True,
        service_account_email='dataflow-service-account@clgx-gis-app-dev-06e3.iam.gserviceaccount.com'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Match Parquet Files" >> MatchFiles(input_glob)
            | "Extract File Paths" >> beam.Map(lambda file_metadata: file_metadata.path)
            | "Run CLI Per FIPS" >> beam.ParDo(RunSuperparcelCLI(cli_options))
            | "Log Output" >> beam.Map(print)
        )


# bucket: geospatial-projects/super_parcels/geoparquet
def bigq2parquet_runner(pipeline_options, output_prefix):
    options = PipelineOptions(
        runner='DirectRunner',  
        project='clgx-gis-app-dev-06e3',
        region='us-central1',
        temp_location='gs://geospatial-projects/super_parcels/geoparquet/temp',
        staging_location='gs://geospatial-projects/super_parcels/geoparquet/staging',
        job_name='bigq2parquet_runner',
        save_main_session=True,
        service_account_email='dataflow-service-account@clgx-gis-app-dev-06e3.iam.gserviceaccount.com'
    )

#INPUT BIGQ TABLE: clgx-gis-app-dev-06e3.superparcels.short_query_pu_pipeline_candidate_parcels_w_ss
# OUTPUT PREFIX: 'gs://geospatial-projects/super_parcels/geoparquet/staging/candidates'
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from BigQuery" >> ReadFromBigQuery(
                query="""
                    SELECT fips, puid, owner, geometry
                    FROM `clgx-gis-app-dev-06e3.superparcels.short_query_pu_pipeline_candidate_parcels_w_ss`
                    WHERE fips IN ('06075', '08031', '16001')
                """,
                use_standard_sql=True
            )
            | "Key by FIPS" >> beam.Map(lambda row: (row['fips'], row))
            | "Group by FIPS" >> beam.GroupByKey()
            | "Write to GeoParquet" >> beam.ParDo(WriteFipsGeoParquet(
                output_prefix=output_prefix
            )
            )
            | "Log" >> beam.Map(print)
        )





