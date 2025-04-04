# Start from Apache Beam's official Python SDK image
FROM apache/beam_python3.11_sdk:latest

# Set working directory
WORKDIR /app

# Copy your codebase into the container
COPY . /app

# Install your CLI package
RUN pip install -e .

# Optional: install system dependencies for geospatial libraries
# RUN apt-get update && apt-get install -y gdal-bin libgdal-dev

# Add your GCP service account key (replace with your actual path)
COPY src/clgx-gis-app-dev-06e3-a94b39eeec37.json /app/keys/clgx-gis-app-dev-06e3-a94b39eeec37.json

# Set environment variable for GCP auth
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/keys/clgx-gis-app-dev-06e3-a94b39eeec37.json"

# Create build root dir (optional but helpful)
RUN mkdir -p /app/tmp

# Beam expects this script to launch the pipeline
ENTRYPOINT ["python", "-m", "df_runner"]
