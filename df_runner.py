import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import subprocess
import os

class RunSuperparcelCLI(beam.DoFn):
    def __init__(self, gcp_key_path, root_build_dir="/app/tmp"):
        self.gcp_key_path = gcp_key_path
        self.root_build_dir = root_build_dir

    def process(self, county_fips):
        build_dir = f"{self.root_build_dir}/{county_fips}"
        os.makedirs(build_dir, exist_ok=True)

        subprocess.run([
            "sps", "config",
            "--json-key", self.gcp_key_path,
            "--county-fips", county_fips,
            "--build-dir", build_dir
        ], check=True)

        subprocess.run([
            "superparcel", "build",
            "--county-fips", county_fips,
            "--build-dir", build_dir
        ], check=True)

        yield f"Done with {county_fips}"

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='clgx-gis-app-dev-06e3',
        region='us-central1',
        temp_location='gs://geospatial-team/abreunig/temp',
        sdk_container_image='gcr.io/clgx-gis-app-dev-06e3/superparcel-df',
        experiments=['use_runner_v2'],
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "County FIPS list" >> beam.Create(['06075', '16001'])
            | "Run CLI" >> beam.ParDo(RunSuperparcelCLI(
                gcp_key_path="/app/keys/clgx-gis-app-dev-06e3-abdbc02fa88b.json"
            ))
        )

if __name__ == "__main__":
    run()
