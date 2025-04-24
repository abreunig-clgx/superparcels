FROM apache/beam_python3.11_sdk:latest

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip && \
    pip install geopandas shapely pyarrow pandas

ENTRYPOINT ["python", "df_runner_bigq2parquet.py"]

