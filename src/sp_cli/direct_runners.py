import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.filesystems import FileSystems

import argparse
import pandas as pd
import geopandas as gpd
from shapely import wkt
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import json
import os
import subprocess

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
    def __init__(self, cli_command):
        self.cli_command = cli_command

    def process(self, filepath):
   
        try:
            result = subprocess.run(self.cli_command, shell=True, check=True, capture_output=True, text=True)
            yield f"✅ Success for {filepath}:\n{result.stdout}"
        except subprocess.CalledProcessError as e:
            yield f"❌ Failure for {filepath}:\n{e.stderr}"

# bucket: geospatial-projects/super_parcels/geoparquet
def dr_runner_bigq2parquet():
    options = PipelineOptions(
        runner='DirectRunner',  
        project='clgx-gis-app-dev-06e3',
        region='us-central1',
        temp_location='gs://geospatial-projects/super_parcels/geoparquet/temp',
        staging_location='gs://geospatial-projects/super_parcels/geoparquet/staging',
        job_name='dr_runner_bigq2parquet',
        save_main_session=True,
        service_account_email='dataflow-service-account@clgx-gis-app-dev-06e3.iam.gserviceaccount.com'
    )

#INPUT BIGQ TABLE: clgx-gis-app-dev-06e3.superparcels.short_query_pu_pipeline_candidate_parcels_w_ss
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
                output_prefix='gs://geospatial-projects/super_parcels/geoparquet/staging/fips'
            )
            )
            | "Log" >> beam.Map(print)
        )

def dr_runner_superparcels(input_glob):
    
    options = PipelineOptions(
        runner='DirectRunner',  
        project='clgx-gis-app-dev-06e3',
        region='us-central1',
        temp_location='gs://geospatial-projects/super_parcels/geoparquet/temp',
        staging_location='gs://geospatial-projects/super_parcels/geoparquet/staging',
        job_name='dr_runner_superparcels',
        save_main_session=True,
        service_account_email='dataflow-service-account@clgx-gis-app-dev-06e3.iam.gserviceaccount.com'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Match Parquet Files" >> beam.io.fileio.MatchFiles(input_glob)
            | "Extract File Paths" >> beam.Map(lambda file_metadata: file_metadata.path)
            | "Run CLI Per FIPS" >> beam.ParDo(RunSuperparcelCLI())
            | "Log Output" >> beam.Map(print)
        )




