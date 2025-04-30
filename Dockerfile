FROM apache/beam_python3.11_sdk:latest

WORKDIR /app

COPY . /app

RUN pip install -e .

# Optional: install native geospatial libs
# RUN apt-get update && apt-get install -y gdal-bin libgdal-dev

COPY src/keys/df_key/clgx-gis-app-dev-06e3-abdbc02fa88b.json /app/keys/clgx-gis-app-dev-06e3-abdbc02fa88b.json

ENV GOOGLE_APPLICATION_CREDENTIALS="/app/keys/clgx-gis-app-dev-06e3-abdbc02fa88b.json"

RUN mkdir -p /app/tmp

ENTRYPOINT ["bash"]
