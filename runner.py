import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import subprocess
import os

with beam.Pipeline() as pipe:
    (
        pipe
        | "Run CLI" >> beam.ParDo(
            lambda fips: subprocess.run([
                "sps", "build", "spmulti-optimal"
                
            ], check=True)
        )
    )