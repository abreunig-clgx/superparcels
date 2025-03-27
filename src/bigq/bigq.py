import os
import logging
from datetime import datetime
from typing import Optional, Union, Dict
from google.cloud import bigquery
from google.oauth2 import service_account

class Logging:
    def __init__(self, verbose: bool = True):
        """Initialize the logger."""
        self.verbose = verbose
        self.logger = self._get_logger()
        self.start_time = None

    def _get_logger(self) -> logging.Logger:
        """Create or retrieve a logger instance."""
        logger_name = 'BigQLogger'
        logger = logging.getLogger(logger_name)
        if not logger.hasHandlers():
            logger.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        return logger

    def info(self, msg: str):
        """Log an info message."""
        if self.verbose:
            self.logger.info(msg)

    def error(self, msg: str):
        """Log an error message."""
        if self.verbose:
            self.logger.error(msg)

    def time(self, msg: str, event_time: Optional[datetime] = None):
        """Log the event time with a specified message."""
        event_time = event_time or datetime.now()
        self.info(f"{msg}: {event_time.strftime('%A, %B %d, %Y %I:%M:%S %p')}")

    def start(self):
        """Record and log the start time."""
        self.start_time = datetime.now()
        self.time("Started")

    def finish(self):
        """Record and log the finish time and elapsed time."""
        end_time = datetime.now()
        if self.start_time:
            elapsed = end_time - self.start_time
            self.time(f"Finished (Elapsed Time: {elapsed.total_seconds()} seconds)", end_time)
        else:
            self.info("Finished, but start time was not set.")

class BigQ:
    def __init__(self, verbose: Optional[bool] = True):
        """
        Initialize the BigQ class for interacting with BigQuery.
        
        Parameters
        ----------
        verbose : Optional[bool]
            Whether to print verbose output.
        """
        self.authenticated = False
        self.client: Optional[bigquery.Client] = None
        self.verbose = verbose
        self.logger = Logging(verbose)
        self.auth = self.Auth(self)

    def query(self, query: str):
        """
        Execute a query on BigQuery and return the results as a DataFrame.
        
        Parameters
        ----------
        query : str
            The SQL query to execute.
            
        Returns
        -------
        pandas.DataFrame
            The query results.
            
        Raises
        ------
        RuntimeError
            If the BigQuery client is not authenticated.
        Exception
            If the query execution fails.
        """
        if not self.authenticated or self.client is None:
            raise RuntimeError("BigQuery client is not authenticated. Please authenticate first.")

        try:
            self.logger.info("Executing query...")
            query_job = self.client.query(query)
            result_df = query_job.result().to_dataframe()
            self.logger.info("Query executed successfully.")
            return result_df
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            raise

    def upload_gdf(self, gdf, table_id: str, write_disposition: str = "WRITE_TRUNCATE", autodetect: bool = True):
        """
        Uploads a GeoDataFrame to BigQuery.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            The GeoDataFrame to upload. Its 'geometry' column will be converted to WKT strings if not already.
        table_id : str
            The destination BigQuery table ID in the format 'project.dataset.table'.
        write_disposition : str, optional
            The write disposition (default is "WRITE_TRUNCATE" to overwrite the table).
        autodetect : bool, optional
            Whether to autodetect the table schema (default is True).

        Raises
        ------
        RuntimeError
            If the BigQuery client is not authenticated.
        Exception
            If the upload fails.
        """
        if not self.authenticated or self.client is None:
            raise RuntimeError("BigQuery client is not authenticated. Please authenticate first.")

        try:
            self.logger.info("Preparing GeoDataFrame for upload...")

            # Convert geometry column to WKT if it's not already a string.
            if "geometry" in gdf.columns:
                gdf = gdf.copy()  # Avoid modifying the original
                gdf["geometry"] = gdf["geometry"].apply(
                    lambda geom: geom.wkt if not isinstance(geom, str) else geom
                )

            self.logger.info("Uploading GeoDataFrame to BigQuery...")
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=autodetect
            )
            job = self.client.load_table_from_dataframe(gdf, table_id, job_config=job_config)
            job.result()  # Wait for the load job to complete
            self.logger.info(f"Uploaded {job.output_rows} rows to {table_id}")
        except Exception as e:
            self.logger.error(f"Failed to upload GeoDataFrame to BigQuery: {e}")
            raise


    class Auth:
        def __init__(self, parent: 'BigQ'):
            """
            Initialize the Auth class.
            
            Parameters
            ----------
            parent : BigQ
                The parent BigQ instance.
            """
            self.parent = parent
            self.credentials = None

        def authenticate(self, credentials: Union[str, Dict]) -> bigquery.Client:
            """
            Authenticate with BigQuery using a JSON file path or a credentials dictionary.
            
            Parameters
            ----------
            credentials : Union[str, Dict]
                The path to the JSON file or a dictionary containing the service account key.
                
            Returns
            -------
            bigquery.Client
                The authenticated BigQuery client.
                
            Raises
            ------
            FileNotFoundError, TypeError, Exception
                If credentials are invalid or authentication fails.
            """
            try:
                if isinstance(credentials, str):
                    if os.path.exists(credentials):
                        self.parent.logger.info(f"Loading credentials from file: {credentials}")
                        self.credentials = service_account.Credentials.from_service_account_file(credentials)
                    else:
                        raise FileNotFoundError(f"Credential file not found: {credentials}")
                elif isinstance(credentials, dict):
                    self.parent.logger.info("Loading credentials from a dictionary.")
                    self.credentials = service_account.Credentials.from_service_account_info(credentials)
                else:
                    raise TypeError("Credentials must be a file path (str) or a dictionary.")

                # Create the BigQuery client using the credentials.
                project_id = getattr(self.credentials, "project_id", None)
                client = bigquery.Client(credentials=self.credentials, project=project_id)
                self.parent.client = client
                self.parent.authenticated = True
                self.parent.logger.info("Authentication successful.")
                return client
            except Exception as e:
                self.parent.logger.error(f"Authentication failed: {e}")
                raise
