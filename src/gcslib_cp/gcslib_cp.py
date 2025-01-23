import os
import fnmatch
import logging
import requests
from datetime import datetime
from typing import Any, List, Optional, Dict, Union
from google.cloud import storage
from google.oauth2 import service_account

class Gcslib:
    """Google Cloud Storage Library
    """
    def __init__(
        self,
        verbose: Optional[bool] = True,
    ):
        """Initialize the Gcslib class.
        
        Parameters
        ----------
        verbose : Optional[bool]
            Whether to print verbose output.

        """
        self.authenticated = False
        self.client = None
        
        # Logging
        self.verbose = verbose
        self.session = requests.Session()
    
        # Class
        self.logger = self.Logging(verbose)
        self.auth = self.Auth(self)
        
    def ls(self, path: str = None, details: bool = False, recursive: bool = True) -> Union[List[str], List[Dict[str, Any]]]:
        """List contents of a Google Cloud Storage bucket.

        This function retrieves and lists the contents of a specified Google Cloud Storage (GCS) path. It can display either basic file names or detailed metadata for each blob (file/object) in the bucket. The listing can be recursive, allowing for a full directory tree to be returned.

        Parameters
        ----------
        path : str, optional
            The GCS path to list the contents of (bucket path). The path can start with `gs://` or just the bucket name and prefix.
            Example: `'gs://my-bucket/folder/'` or `'my-bucket/folder/'`. Must be provided.

        details : bool, optional, default=False
            If `True`, detailed metadata for each blob (file/object) is returned. 
            If `False`, only the blob names (file paths) are returned.

        recursive : bool, optional, default=True
            If `True`, the function will list all files in subdirectories. 
            If `False`, only the files in the immediate directory are listed.

        Returns
        -------
        Union[List[str]
            A list of strings representing file paths.
            
        List[Dict[str, Any]]]
            A list of dictionaries containing detailed information for each blob. The structure of the dictionary (when `details=True`) is as follows:
            - 'blob': The blob object itself, representing a file or other object stored in Google Cloud Storage.
            - 'name': The name (path) of the blob within the bucket.
            - 'name2': The name (path) of the blob within the bucket.
            - 'size_converted': The size of the blob in human-readable format (e.g., KB, MB).
            - 'size': The size of the blob in bytes.
            - 'content_type': The MIME type of the blob, indicating the format of the content.
            - 'time_created_converted': The creation time of the blob in a human-readable format.
            - 'time_created': The exact timestamp when the blob was created.
            - 'time_updated_converted': The last update time of the blob in a human-readable format.
            - 'time_updated': The exact timestamp when the blob was last modified.
            - 'md5_hash': The MD5 hash of the blob's content, used for data integrity verification.
            - 'crc32c': The CRC32C checksum of the blob's content, another method for verifying data integrity.
            - 'etag': The HTTP entity tag for the blob, used for cache validation and optimistic concurrency control.
            - 'generation': The generation number of the blob, which changes each time the content is overwritten.
            - 'metageneration': The metageneration number, which changes each time the metadata is updated.
            - 'storage_class': The storage class of the blob (e.g., STANDARD, NEARLINE, COLDLINE), indicating its durability and availability.
            - 'temporary_hold': A boolean indicating if the blob is under temporary hold.
            - 'event_based_hold': A boolean indicating if the blob is under event-based hold.
            - 'retention_expiration_time': The timestamp when the retention period expires (if retention policy is applied).
            - 'metadata': A dictionary of custom metadata associated with the blob.
            - 'kms_key_name': The name of the Cloud KMS (Key Management Service) key used to encrypt the blob, if applicable.
            - 'public_url': The public URL to access the blob if it is publicly accessible.
            - 'self_link': A link to the blob's resource in Google Cloud Storage.
            - 'owner': The owner of the blob, represented as an entity object containing user/project information.

        Raises
        ------
        Exception
            If the user is not authenticated or if an error occurs while listing the contents.
        
        ValueError
            If the `path` is not specified.

        Examples
        --------
        >>> ls('gs://my-bucket/folder/')
        ['file1.txt', 'file2.txt', 'subfolder/file3.txt']

        >>> ls('my-bucket/folder/', details=True)
        [
            {
                'name': 'file1.txt',
                'size': 1024,
                'content_type': 'text/plain',
                'time_created': datetime.datetime(2023, 1, 1, 12, 0),
                'md5_hash': 'd41d8cd98f00b204e9800998ecf8427e',
                'crc32c': 'e3b0c442',
                # ... (other metadata)
            },
            ...
        ]
        """
        result                                      = []                                                # Return list
        
        if not self.authenticated or self.client is None:
            raise Exception("You must authenticate first before listing contents.")
        
        if not path:
            raise ValueError("The variable path must be specified.")
        
        try:
            path                                    = path[5:] if path.startswith("gs://") else path    # Remove gs:// from path
            bucket_name, _, prefix                  = path.partition('/')                               # Split path
            prefix                                  = prefix or None                                    # If there's no prefix, set it to None
            bucket                                  = self.client.bucket(bucket_name)                   # Get bucket
            blobs                                   = list(bucket.list_blobs(prefix=prefix))            # Get blobs
            _blobs                                  = self._handle_recursive(blobs=blobs, path=prefix) if not recursive and len(blobs) > 1 else blobs # Process recursive


            if details:
                result = [
                    {
                        'blob':                         blob,                                           # The blob object itself, representing a file or other object stored in Google Cloud Storage.
                        'name':                         blob.name,                                      # The name (path) of the blob within the bucket.
                        'name2':                        f"gs://{bucket_name}/{blob.name}",              # The name (path) of the blob within the bucket.
                        'size_converted':               self._convert_size(blob.size),                  # The size of the blob in human-readable format (e.g., KB, MB).
                        'size':                         blob.size,                                      # The size of the blob in bytes.
                        'content_type':                 blob.content_type,                              # The MIME type of the blob, indicating the format of the content.
                        'time_created_converted':       self._format_datetime(blob.time_created),       # The creation time of the blob in a human-readable format.
                        'time_created':                 blob.time_created,                              # The exact timestamp when the blob was created.
                        'time_updated_converted':       self._format_datetime(blob.updated),            # The last update time of the blob in a human-readable format.
                        'time_updated':                 blob.updated,                                   # The exact timestamp when the blob was last modified.
                        'md5_hash':                     blob.md5_hash,                                  # The MD5 hash of the blob's content, used for data integrity verification.
                        'crc32c':                       blob.crc32c,                                    # The CRC32C checksum of the blob's content, another method for verifying data integrity.
                        'etag':                         blob.etag,                                      # The HTTP entity tag for the blob, used for cache validation and optimistic concurrency control.
                        'generation':                   blob.generation,                                # The generation number of the blob, which changes each time the content is overwritten.
                        'metageneration':               blob.metageneration,                            # The metageneration number, which changes each time the metadata is updated.
                        'storage_class':                blob.storage_class,                             # The storage class of the blob (e.g., STANDARD, NEARLINE, COLDLINE), indicating its durability and availability.
                        'temporary_hold':               blob.temporary_hold,                            # A boolean indicating if the blob is under temporary hold.
                        'event_based_hold':             blob.event_based_hold,                          # A boolean indicating if the blob is under event-based hold.
                        'retention_expiration_time':    blob.retention_expiration_time,                 # The timestamp when the retention period expires (if retention policy is applied).
                        'metadata':                     blob.metadata,                                  # A dictionary of custom metadata associated with the blob.
                        'kms_key_name':                 blob.kms_key_name,                              # The name of the Cloud KMS (Key Management Service) key used to encrypt the blob, if applicable.
                        'public_url':                   blob.public_url,                                # The public URL to access the blob if it is publicly accessible.
                        'self_link':                    blob.self_link,                                 # A link to the blob's resource in Google Cloud Storage.
                        'owner':                        blob.owner                                      # The owner of the blob, represented as an entity object containing user/project information.
                    }
                    for blob in _blobs
                ]

                # Logging details
                for blob_info in result:
                    self.logger.info(f'''Processed blob: {blob_info}''')
                self.logger.info(f'''Total size: {self._convert_size(sum(blob['size'] for blob in result))}''')
            else:
                result                              = [blob.name for blob in _blobs]                    # Get blob name

            if not len(_blobs):
                self.logger.error(f'''Failed to find blob(s) "{path}"''')
        except Exception as e:
            self.logger.error(f'''Error listing contents of "{path}": {str(e)}''')
            raise e
        finally:
            return result

    def md(self, path: str) -> Dict[str, Any]:
        """Create a new directory (prefix) in a Google Cloud Storage bucket.

        This function creates an empty directory (or prefix) in a specified Google Cloud Storage bucket. If the `gs://` scheme is not included in the path, it will be added. The path is ensured to end with a forward slash ("/") to denote a folder. The function splits the provided path to extract the bucket name and prefix, then creates the empty directory by uploading an empty string to the target path.

        Parameters
        ----------
        path : str
            The full path to the directory to be created in the GCS bucket. This path can be with or without the `gs://` prefix, but it must specify a valid bucket and directory name.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the following keys:
            - 'success' (bool): Whether the directory creation was successful.
            - 'message' (str): A confirmation message or error description.
            - 'path' (str): The created directory path.

        Raises
        ------
        ValueError
            If the path is not specified or if there is an error with the path format.
            
        Exception
            If the user is not authenticated or an error occurs while creating the directory.

        Examples
        --------
        >>> md('gs://my-bucket/new-directory/')
        {
            'success': True,
            'message': 'Created path gs://my-bucket/new-directory/"',
            'path': 'gs://my-bucket/new-directory/'
        }
        
        >>> md('my-bucket/new-directory/')
        {
            'success': True,
            'message': 'Created path gs://my-bucket/new-directory/"',
            'path': 'gs://my-bucket/new-directory/'
        }
        """
        result                                  = {
            "success": False,                                                                       # If request was successful
            "message": "",                                                                          # Description of what occured
            "path": path                                                                            # Path
        }
    
        if not self.authenticated or self.client is None:
            raise Exception("You must authenticate first before creating a path.")
            
        if not path:
            raise ValueError("path (bucket path) must be specified.")
        
        try:
            path                                = path if path.endswith('/') else f'{path}/'        # Ensure folder name is given
            path                                = path[5:] if path.startswith('gs://') else path    # Strip 'gs://' if present and split path into bucket_name and prefix
            bucket_name, _, prefix              = path.partition('/')                               # Split path
            prefix                              = prefix or None                                    # If there's no prefix, set it to None
            bucket                              = self.client.bucket(bucket_name)                   # Get source bucket
            blob                                = bucket.blob(prefix)                               # Get blob connection
            logging_target_path                 = f"gs://{path}"                                    # Creating logging name
            
            # Create empty directory
            blob.upload_from_string('')

            message = f'''Created path {logging_target_path}"'''
            self.logger.info(message)
            result.update({"success": True, "path": logging_target_path, "message": message})
        except ValueError as ve:
            message = f'''Error with path format: {str(ve)}'''
            self.logger.error(message)
            result.update({"message": message})
            raise ve
        except Exception as e:
            message = f'''Error creating path "{path}": {str(e)}'''
            self.logger.error(message)
            result.update({"message": message})
            raise e
        finally:
            return result

    def mv(self, path: str, target_path: str, recursive: bool = True) -> Dict[str, Any]:
        """Moves, downloads, or uploads files and directories between local and Google Cloud Storage (GCS).

        This function handles the moveing of files or directories between GCS and local paths. It can perform three types of operations:
        1. Moves files/directories from one GCS path to another.
        2. Download files from GCS to a local directory.
        3. Upload files from a local directory to GCS.

        Parameters
        ----------
        path : str
            The source path of the file or directory to move. Can be a GCS path (e.g., `'gs://my-bucket/folder/'`) or a local path. Must be specified.

        target_path : str
            The destination path where the file or directory should be copied. Can be a GCS path or a local path. Must be specified.

        recursive : bool, optional, default=True
            If `True`, the function will move the entire directory tree, including all subdirectories and files.
            If `False`, only the immediate contents of the specified directory will be copied.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the results of the move process. The structure is as follows:
            - 'success' (bool): Indicates whether the move process was successful.
            - 'type' (str): Specifies the type of operation performed (either "move", "download", or "upload").
            - 'message' (str): A summary of the outcome (e.g., which files were copied or any errors encountered).
            - 'path' (str): The source path that was processed.
            - 'target_path' (str): The destination path that was processed.
            - 'recursive' (bool): Indicates whether the operation was performed recursively.
            - 'details' (list[Dict]): A list of dictionaries, each containing:
                - 'success' (bool): Whether the move of a specific file was successful.
                - 'path' (str): The source path of the file.
                - 'target_path' (str): The destination path of the file.
                - 'message' (str): A message providing details on the move operation.

        Raises
        ------
        Exception
            If the user is not authenticated or if an error occurs during the move process.

        ValueError
            If `path` or `target_path` are not specified, or if neither the source nor destination is a GCS path.

        Examples
        --------
        Move files between GCS paths:
        >>> mv('gs://my-bucket/folder/', 'gs://my-other-bucket/folder/')
        {
            'success': True,
            'type': 'move',
            'message': 'Move completed from "gs://my-bucket/folder/" to "gs://my-other-bucket/folder/"',
            'path': 'gs://my-bucket/folder/',
            'target_path': 'gs://my-other-bucket/folder/',
            'recursive': True,
            'details': [
                {
                    'success': True,
                    'path': 'gs://my-bucket/folder/file1.txt',
                    'target_path': 'gs://my-other-bucket/folder/file1.txt',
                    'message': 'File copied from "gs://my-bucket/folder/file1.txt" to "gs://my-other-bucket/folder/file1.txt"'
                },
                ...
            ]
        }

        Download files from GCS to local:
        >>> mv('gs://my-bucket/folder/', '/local/folder/')
        {
            'success': True,
            'type': 'download',
            'message': 'Download completed from "gs://my-bucket/folder/" to "/local/folder/"',
            'path': 'gs://my-bucket/folder/',
            'target_path': '/local/folder/',
            'recursive': True,
            'details': [
                {
                    'success': True,
                    'path': 'gs://my-bucket/folder/file1.txt',
                    'target_path': '/local/folder/file1.txt',
                    'message': 'File downloaded from "gs://my-bucket/folder/file1.txt" to "/local/folder/file1.txt"'
                },
                ...
            ]
        }

        Upload files from local to GCS:
        >>> mv('/local/folder/', 'gs://my-bucket/folder/')
        {
            'success': True,
            'type': 'upload',
            'message': 'Upload completed from "/local/folder/" to "gs://my-bucket/folder/"',
            'path': '/local/folder/',
            'target_path': 'gs://my-bucket/folder/',
            'recursive': True,
            'details': [
                {
                    'success': True,
                    'path': '/local/folder/file1.txt',
                    'target_path': 'gs://my-bucket/folder/file1.txt',
                    'message': 'File uploaded from "/local/folder/file1.txt" to "gs://my-bucket/folder/file1.txt"'
                },
                ...
            ]
        }
        """
        result = {
            "success": False,                                                                                       # If request was successful
            "type": "",                                                                                             # Type of process (download, copy, or upload)
            "message": "",                                                                                          # Description of what occured
            "path": path,                                                                                           # Path
            "target_path": target_path,                                                                             # Target Path
            "recursive": recursive,                                                                                 # If Recusrive
            "details": []                                                                                           # Details of the process
        }
        
        if not self.authenticated or self.client is None:
            raise Exception("You must authenticate first before copying files or directories.")

        if not path or not target_path:
            raise ValueError("Source and destination paths must be specified.")
        
        try:
            # Handle GCS-to-GCS copy (copy)
            if path.startswith('gs://') and target_path.startswith('gs://'):
                result.update({
                    "type": "copy",
                    "details": self._copy_gcs_to_gcs(path, target_path, recursive, delete=True)
                })
            # Handle GCS download to local
            elif path.startswith('gs://'):
                result.update({
                    "type": "download",
                    "details": self._download_from_gcs(path, target_path, recursive)
                })
            # Handle local upload to GCS 
            elif target_path.startswith('gs://'):
                result.update({
                    "type": "upload",
                    "details": self._upload_to_gcs(path, target_path, recursive)
                })
            else:
                raise ValueError("Either the source or the destination must be a GCS path.")
            
        except ValueError as ve:
            message = f'''Error with path format: {str(ve)}'''
            self.logger.error(message)
            result.update({"message": message})
            raise ve
        except Exception as e:
            result["message"] = f'''Error during "{result['type']}" operation: {str(e)}'''
            self.logger.error(result["message"])
            raise e
        finally:
            success = bool(result['details']) and all(file["success"] for file in result['details'])
            message = f'''{result['type'].capitalize()} completed successfully from "{path}" to "{target_path}"''' if success else f'''{result['type'].capitalize()} failed from "{path}" to "{target_path}"'''
            result.update({"success": success, "message": message})
            return result

    def cp(self, path: str, target_path: str, recursive: bool = True) -> Dict[str, Any]:
        """Copies, downloads, or uploads files and directories between local and Google Cloud Storage (GCS).

        This function handles the copying of files or directories between GCS and local paths. It can perform three types of operations:
        1. Copy files/directories from one GCS path to another.
        2. Download files from GCS to a local directory.
        3. Upload files from a local directory to GCS.

        Parameters
        ----------
        path : str
            The source path of the file or directory to copy. Can be a GCS path (e.g., `'gs://my-bucket/folder/'`) or a local path. Must be specified.

        target_path : str
            The destination path where the file or directory should be copied. Can be a GCS path or a local path. Must be specified.

        recursive : bool, optional, default=True
            If `True`, the function will copy the entire directory tree, including all subdirectories and files.
            If `False`, only the immediate contents of the specified directory will be copied.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the results of the copy process. The structure is as follows:
            - 'success' (bool): Indicates whether the copy process was successful.
            - 'type' (str): Specifies the type of operation performed (either "copy", "download", or "upload").
            - 'message' (str): A summary of the outcome (e.g., which files were copied or any errors encountered).
            - 'path' (str): The source path that was processed.
            - 'target_path' (str): The destination path that was processed.
            - 'recursive' (bool): Indicates whether the operation was performed recursively.
            - 'details' (list[Dict]): A list of dictionaries, each containing:
                - 'success' (bool): Whether the copy of a specific file was successful.
                - 'path' (str): The source path of the file.
                - 'target_path' (str): The destination path of the file.
                - 'message' (str): A message providing details on the copy operation.

        Raises
        ------
        Exception
            If the user is not authenticated or if an error occurs during the copy process.

        ValueError
            If `path` or `target_path` are not specified, or if neither the source nor destination is a GCS path.

        Examples
        --------
        Copy files between GCS paths:
        >>> cp('gs://my-bucket/folder/', 'gs://my-other-bucket/folder/')
        {
            'success': True,
            'type': 'copy',
            'message': 'Copy completed from "gs://my-bucket/folder/" to "gs://my-other-bucket/folder/"',
            'path': 'gs://my-bucket/folder/',
            'target_path': 'gs://my-other-bucket/folder/',
            'recursive': True,
            'details': [
                {
                    'success': True,
                    'path': 'gs://my-bucket/folder/file1.txt',
                    'target_path': 'gs://my-other-bucket/folder/file1.txt',
                    'message': 'File copied from "gs://my-bucket/folder/file1.txt" to "gs://my-other-bucket/folder/file1.txt"'
                },
                ...
            ]
        }

        Download files from GCS to local:
        >>> cp('gs://my-bucket/folder/', '/local/folder/')
        {
            'success': True,
            'type': 'download',
            'message': 'Download completed from "gs://my-bucket/folder/" to "/local/folder/"',
            'path': 'gs://my-bucket/folder/',
            'target_path': '/local/folder/',
            'recursive': True,
            'details': [
                {
                    'success': True,
                    'path': 'gs://my-bucket/folder/file1.txt',
                    'target_path': '/local/folder/file1.txt',
                    'message': 'File downloaded from "gs://my-bucket/folder/file1.txt" to "/local/folder/file1.txt"'
                },
                ...
            ]
        }

        Upload files from local to GCS:
        >>> cp('/local/folder/', 'gs://my-bucket/folder/')
        {
            'success': True,
            'type': 'upload',
            'message': 'Upload completed from "/local/folder/" to "gs://my-bucket/folder/"',
            'path': '/local/folder/',
            'target_path': 'gs://my-bucket/folder/',
            'recursive': True,
            'details': [
                {
                    'success': True,
                    'path': '/local/folder/file1.txt',
                    'target_path': 'gs://my-bucket/folder/file1.txt',
                    'message': 'File uploaded from "/local/folder/file1.txt" to "gs://my-bucket/folder/file1.txt"'
                },
                ...
            ]
        }
        """
        result = {
            "success": False,                                                                                       # If request was successful
            "type": "",                                                                                             # Download, copy, or upload
            "message": "",                                                                                          # Description of what occured
            "path": path,                                                                                           # Path
            "target_path": target_path,                                                                             # Target Path
            "recursive": recursive,                                                                                 # If Recusrive
            "details": []                                                                                           # Details of the process
        }
        
        if not self.authenticated or self.client is None:
            raise Exception("You must authenticate first before copying files or directories.")

        if not path or not target_path:
            raise ValueError("Source and destination paths must be specified.")
        
        try:
            # Handle GCS-to-GCS copy (copy)
            if path.startswith('gs://') and target_path.startswith('gs://'):
                result.update({
                    "type": "copy",
                    "details": self._copy_gcs_to_gcs(path, target_path, recursive)
                })
            # Handle GCS download to local
            elif path.startswith('gs://'):
                result.update({
                    "type": "download",
                    "details": self._download_from_gcs(path, target_path, recursive)
                })
            # Handle local upload to GCS 
            elif target_path.startswith('gs://'):
                result.update({
                    "type": "upload",
                    "details": self._upload_to_gcs(path, target_path, recursive)
                })
            else:
                raise ValueError("Either the source or the destination must be a GCS path.")

        except ValueError as ve:
            message = f'''Error with path format: {str(ve)}'''
            self.logger.error(message)
            result.update({"message": message})
            raise ve
        except Exception as e:
            result["message"] = f'''Error during "{result['type']}" operation: {str(e)}'''
            self.logger.error(result["message"])
            raise e
        finally:
            success = bool(result['details']) and all(file["success"] for file in result['details'])
            message = f'''{result['type'].capitalize()} completed successfully from "{path}" to "{target_path}"''' if success else f'''{result['type'].capitalize()} failed from "{path}" to "{target_path}"'''
            result.update({"success": success, "message": message})
            return result

    def rn(self, path: str, target_path: str) -> Dict[str, Any]:
        """Rename a file or folder in Google Cloud Storage (GCS).

        This method handles renaming both individual files and entire folders in a GCS bucket. It ensures that the source and target paths are within the same bucket and adjusts the paths accordingly. The function supports both single-file renaming and folder renaming, including cases where folders contain multiple files.

        Parameters
        ----------
        path : str
            The source path of the file or folder to rename. This can be a full GCS path (e.g., "gs://bucket_name/folder/file") or a relative GCS path (e.g., "bucket_name/folder/file"). If renaming a folder, the path should reflect the folder prefix (with or without a trailing slash).
            
        target_path : str
            The target path where the file or folder should be renamed to. Like `path`, this can be a full or relative GCS path. If renaming a folder, the target path should reflect the new folder prefix.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the following keys:
            - "success" (bool): True if the rename operation was successful for all files/folders, False otherwise.
            - "message" (str): A summary of the operation, including errors if any occurred.
            - "path" (str): The original source path provided.
            - "target_path" (str): The target path provided.
            - "details" (list): A list of dictionaries containing details for each file or folder processed. Each entry includes:
                - "success" (bool): True if the rename was successful for the specific file or folder, False otherwise.
                - "path" (str): The original source path of the file or folder.
                - "target_path" (str): The new target path of the file or folder.
                - "message" (str): A message indicating the result of the renaming operation for the specific file or folder.

        Raises
        ------
        ValueError
            Raised if `path` or `target_path` are not specified, or if the source and target paths are in different buckets.
        
        Exception
            Raised if an error occurs during the rename operation, such as a failure to authenticate or GCS-related errors.

        Examples
        --------
        >>> gcs.rn(path="gs://bucket_name/folder/file.txt", target_path="gs://bucket_name/folder/new_file.txt")
        {
            'success': True,
            'message': 'Renamed "gs://bucket_name/folder/file.txt" to "gs://bucket_name/folder/new_file.txt"'
            'path': 'gs://bucket_name/folder/file.txt',
            'target_path': 'gs://bucket_name/folder/new_file.txt',
            'details': [
                {
                    'success': True,
                    'message': 'Renamed "gs://bucket_name/folder/file.txt" to "gs://bucket_name/folder/new_file.txt"'
                    'path': 'gs://bucket_name/folder/file.txt',
                    'target_path': 'gs://bucket_name/folder/new_file.txt'
                }
            ]
        }
        >>> gcs.rn(path="gs://bucket_name/folder/", target_path="gs://bucket_name/new_folder/")
        {
            'success': True,
            'message': 'Renamed "gs://bucket_name/folder/" to "gs://bucket_name/new_folder/"'
            'path': 'gs://bucket_name/folder/',
            'target_path': 'gs://bucket_name/new_folder/',
            'details': [
                {
                    'success': True,
                    'message': 'Renamed "gs://bucket_name/folder/file.txt" to "gs://bucket_name/new_folder/file.txt"'
                    'path': 'gs://bucket_name/folder/file.txt',
                    'target_path': 'gs://bucket_name/new_folder/file.txt'
                },
                {
                    'success': True,
                    'message': 'Renamed "gs://bucket_name/folder/file2.txt" to "gs://bucket_name/new_folder/file2.txt"'
                    'path': 'gs://bucket_name/folder/file2.txt',
                    'target_path': 'gs://bucket_name/new_folder/file2.txt'
                },
            ]
        }
        """
        result                                          = { 
            "success": False,                                                                           # If request was successful
            "message": "",                                                                              # Description of what occured
            "path": path,                                                                               # Path
            "target_path": target_path,                                                                 # Target Path
            "details": []                                                                               # Details of the process
        }        
        
        if not self.authenticated or self.client is None:
            raise Exception("You must authenticate first before renaming files or directories.")
            
        if not path or not target_path:
            raise ValueError("Variables path and target_path paths must be specified.")
        
        try:
            path                                        = path if path.startswith('gs://') else f'gs://{path}' # Remove gs:// from source path
            target_path                                 = target_path if target_path.startswith('gs://') else f'gs://{target_path}' # Remove gs:// from target_path path
            source_bucket_name, source_prefix           = path[5:].split('/', 1)                        # Extract source bucket names and prefixes
            target_bucket_name, target_prefix           = target_path[5:].split('/', 1)                 # Extract target bucket names and prefixes

            if source_bucket_name != target_bucket_name:
                raise ValueError("Source and target must be in the same bucket for renaming.")

            source_bucket                               = self.client.bucket(source_bucket_name)        # Get bucket
            blob                                        = source_bucket.blob(source_prefix)             # Get blob
            
            if blob.exists():
                # Handle files
                try:
                    source_bucket.copy_blob(blob, source_bucket, target_prefix)                         # Copy blob with new name
                    blob.delete()                                                                       # Delete old blob

                    message                             = f'''Renamed "{path}" to "{target_path}"'''
                    result["details"]                   = {"success": True, "path": path, "target_path": target_path, "message": message}
                except Exception as _e:
                    message                             = f'''Could not rename "{path}": {str(_e)}'''
                    result["details"]                   = {"success": False, "path": path, "target_path": target_path, "message": message}
            else:
                # Handle folders
                source_blobs                            = list(source_bucket.list_blobs(prefix=source_prefix)) # Get blobs
                
                for blob in source_blobs:
                    relative_path                       = blob.name[len(source_prefix):]                # Get relative path
                    source_path_name                    = f"{source_prefix}{relative_path}"             # Details source name
                    target_path_name                    = f"{target_prefix}{relative_path}"             # Details target name
                    logging_source_blob_name            = os.path.join(f"gs://{source_bucket_name}", source_path_name).replace("\\", "/") # Create logging name
                    logging_target_blob_name            = os.path.join(f"gs://{target_bucket_name}", target_path_name).replace("\\", "/") # Create logging name
                    try:
                        source_bucket.copy_blob(blob, source_bucket, source_path_name)                  # Copy blob with new name
                        blob.delete()                                                                   # Delete old blob
                        
                        message                         = f'''Renamed "{logging_source_blob_name}" to "{logging_target_blob_name}"'''
                        result["details"].append({"success": True, "path": logging_source_blob_name, "target_path": logging_target_blob_name, "message": message})
                    except Exception as _e:
                        message                         = f'''Could not rename "{logging_source_blob_name}": {str(_e)}'''
                        result["details"].append({"success": False, "path": logging_source_blob_name, "target_path": logging_target_blob_name, "message": message})
                        result.update({"message": message})

            message = f'''Renamed "{path}" to "{target_path}"'''
            self.logger.info(message)
            result.update({"path": path, "target_path": target_path})
        except Exception as e:
            message = f'''Error renaming "{path}" to "{target_path}": {str(e)}'''
            self.logger.error(message)
            result.update({"message": message})
        finally:
            success = bool(result['details']) and all(file["success"] for file in result['details'])
            message = f'''Rename completed successfully for "{path}" to "{target_path}"''' if success else f'''Rename failed from "{path}" to "{target_path}"'''
            result.update({"success": success, "message": message})
            return result

    def rm(self, target_path: str, recursive: bool = True) -> Dict[str, Any]:
        """Deletes files or directories in a Google Cloud Storage bucket.

        This function deletes blobs (files/objects) in a specified Google Cloud Storage (GCS) path. It can handle both individual files and directories. When `recursive` is set to `True`, the function deletes all files in the directory and its subdirectories. Wildcards can also be used to match multiple files.

        Parameters
        ----------
        target_path : str
            The GCS path of the file or directory to delete. The path can start with `gs://` or just the bucket name and prefix. 
            Example: `'gs://my-bucket/folder/'` or `'my-bucket/folder/'`. Must be provided.

        recursive : bool, optional, default=True
            If `True`, the function will delete all files in the directory and its subdirectories. 
            If `False`, only the immediate files in the specified directory will be deleted, and subdirectories will be ignored.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the results of the deletion process. The structure is as follows:
            - 'success' (bool): Indicates whether the deletion process was successful.
            - 'message' (str): A summary of the outcome (e.g., which files were deleted, or any errors encountered).
            - 'target_path' (str): The target path that was processed.
            - 'recursive' (bool): Indicates whether the deletion was performed recursively.
            - 'details' (list[Dict]): A list of dictionaries, each containing:
                - 'success' (bool): Whether the deletion of a specific file was successful.
                - 'target_path' (str): The GCS path of the file.
                - 'message' (str): A message providing details on the deletion.

        Raises
        ------
        Exception
            If the user is not authenticated or if an error occurs during the deletion process.

        ValueError
            If `target_path` is not specified.

        Examples
        --------
        >>> rm('gs://my-bucket/folder/file1.txt')
        {
            'success': True,
            'message': 'Deleted gs://my-bucket/folder/file1.txt',
            'target_path': 'gs://my-bucket/folder/file1.txt',
            'recursive': False,
            'details': [
                {
                    'success': True,
                    'target_path': 'gs://my-bucket/folder/file1.txt',
                    'message': 'Deleted "gs://my-bucket/folder/file1.txt"'
                }
            ]
        }

        >>> rm('gs://my-bucket/folder/', recursive=True)
        {
            'success': True,
            'message': 'Deleted gs://my-bucket/folder/',
            'target_path': 'gs://my-bucket/folder/',
            'recursive': True,
            'details': [
                {
                    'success': True,
                    'target_path': 'gs://my-bucket/folder/file1.txt',
                    'message': 'Deleted "gs://my-bucket/folder/file1.txt"'
                },
                {
                    'success': True,
                    'target_path': 'gs://my-bucket/folder/file2.txt',
                    'message': 'Deleted "gs://my-bucket/folder/file2.txt"'
                }
            ]
        }
        """
        result = {
            "success": False,                                                                                   # If request was successful
            "message": "",                                                                                      # What process occured
            "target_path": target_path,                                                                         # target_path
            "recursive": recursive,                                                                             # recursive
            "details": []                                                                                       # results
        }
        
        if not self.authenticated or self.client is None:
            raise Exception("You must authenticate first before renaming files or directories.")
        
        if not target_path:
            raise ValueError("Target_path paths must be specified.")
        
        try:
            target_path                                 = target_path[5:] if target_path.startswith('gs://') else target_path # Strip 'gs://' if present and split path into bucket_name and prefix
            target_bucket_name, _, target_prefix        = target_path.partition('/')                            # Split path
            target_bucket                               = self.client.bucket(target_bucket_name)                # Get target bucket
            wildcard_exists, wildcard_types             = self._check_wildcards(target_prefix)                  # Determine if wildcards are used
            processed_target_prefix                     = target_path[5:target_path.rfind('/') + 1].split('/', 1)[1] if wildcard_exists else target_path[5:].split('/', 1)[1] # Update retreval prefix if wildcard is used
            blobs                                       = list(target_bucket.list_blobs(prefix=processed_target_prefix)) # Get all available blobs
            _blobs                                      = self._handle_recursive(blobs=blobs, path=processed_target_prefix) if not recursive and len(blobs) > 1 else blobs # Process recursive
            _blobs                                      = self._handle_wildcards(prefix=processed_target_prefix, blobs=_blobs, wildcard_exists=wildcard_exists, wildcard_types=wildcard_types) # Handle wildcards
            logging_blob_count                          = len(_blobs)                                           # Processed blob count
            logging_target_path                         = f"gs://{target_bucket_name}/{target_prefix}"          # Creating logging name
            
            # Logging Values
            if logging_blob_count == 0:
                self.logger.error(f'''Failed to find blob(s) "{logging_target_path}"''')
                return result

            # Iterate over blobs
            for target_blob in _blobs:
                logging_destination_blob_name           = f"gs://{target_bucket_name}/{target_blob.name}"       # Details destination name

                try:
                    # Delete blob
                    # target_blob.delete()
                    
                    message = f'''Deleted "{logging_destination_blob_name}"'''
                    result["details"].append({"success": True, "target_path": logging_destination_blob_name, "message": message})
                    self.logger.info(message)
                except Exception as _e:
                    message = f'''Failed to delete "{logging_destination_blob_name}"'''
                    result["details"].append({"success": False, "target_path": logging_destination_blob_name, "message": f"{message}: {_e}"})
                    self.logger.info(message)
                
        except Exception as e:
            message = f'''Error processing blob(s) "{target_path}": {str(e)}'''
            self.logger.error(message)
        finally:
            success = bool(result['details']) and all(file["success"] for file in result['details'])
            message = f'''Deleted {logging_target_path}''' if success else f'''Failed to delete {logging_target_path}'''
            result.update({
                "success": success,
                "message": message,
            })
            return result

    class Auth:
        def __init__(self, parent):
            """Constructor method
            """
            self.parent = parent
            
            self.credentials = None
            
        def authenticate(self, credentials: Union[str, Dict]):
            """
            Authenticate with Google Cloud Storage using a JSON file path or a credentials dictionary.
            Parameters
            ----------
            credentials : Union[str, Dict]
                The path to the JSON file or a dictionary containing the service account key.
            Returns
            -------
            storage.Client
                The authenticated Google Cloud Storage client.
            """
            try:
                if isinstance(credentials, str):
                    # Load credentials from a JSON file
                    if os.path.exists(credentials):
                        self.parent.logger.info(f"Loading credentials from file: {credentials}")
                        self.credentials = service_account.Credentials.from_service_account_file(credentials)
                    else:
                        raise FileNotFoundError(f"Credential file not found: {credentials}")
                elif isinstance(credentials, dict):
                    # Load credentials from a dictionary
                    self.parent.logger.info("Loading credentials from a dictionary.")
                    self.credentials = service_account.Credentials.from_service_account_info(credentials)
                else:
                    raise TypeError("Credentials must be a file path (str) or a dictionary.")
                
                # Authenticate the client with the provided credentials
                client = storage.Client(credentials=self.credentials)
                self.parent.client = client
                self.parent.authenticated = True
                self.parent.logger.info("Authentication successful.")
                return client
            
            except Exception as e:
                self.parent.logger.error(f"Authentication failed: {e}")
                raise e

    class Logging:
        def __init__(self, verbose: bool):
            """Constructor method
            """
            self.verbose = verbose
            self.logger = self._get_logger()
            self.start_time = None

        def _get_logger(self) -> logging.Logger:
            """Get or create a logger instance.
            
            Returns
            -------
            logging.Logger
                Logger instance.
            """
            logger_name = 'GCSlib'
            logger = logging.getLogger(logger_name)
            if not logger.hasHandlers():
                logger.setLevel(logging.INFO)
                formatter = logging.Formatter('%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)
            return logger

        def info(self, msg: str):
            """Log an info message.
            
            Parameters
            ----------
            msg : str
                The message to log.
            """
            if self.verbose and self.logger:
                self.logger.info(msg)

        def error(self, msg: str):
            """Log an error message.
            
            Parameters
            ----------
            msg : str
                The message to log.
            """
            if self.verbose and self.logger:
                self.logger.error(msg)

        def time(self, msg: str, event_time: datetime = None):
            """Logs an event time with a specified message.
            
            Parameters
            ----------
            msg : str
                The message to log.
            event_time : datetime
                Time of the event (default is current time).
            """
            event_time = event_time or datetime.now()
            self.info(f"{msg}: {event_time.strftime('%A, %B %d, %Y %I:%M:%S %p')}")

        def start(self):
            """Records the start time and logs it.
            """
            self.start_time = datetime.now()
            self.time("Started")

        def finish(self):
            """Records the finish time, calculates elapsed time, and logs it.
            """
            end_time = datetime.now()
            elapsed = end_time - self.start_time
            elapsed_seconds = elapsed.total_seconds()
            self.time(f"Finished (Elapsed Time: {elapsed_seconds} seconds)", end_time)

    def _handle_wildcards(self, prefix, blobs: list, wildcard_exists: bool, wildcard_types: list) -> list:
        """Processes the list of blobs based on the presence of wildcards and whether recursive search is enabled.
        
        Parameters
        ----------
        prefix : str
        blobs : list
            - The list of blobs to process.
        wildcard_exists : bool
            - True if wildcards are present in the path, False otherwise.
        wildcard_types : list of str
            - List of wildcard characters present in the path.
        recursive : bool
            - True if recursive search is enabled, False otherwise.
        
        Returns
        -------
        list
            - The filtered list of blobs that match the wildcard patterns.
        """
        if not wildcard_exists:
            return blobs
        
        filtered_blobs = []
        
        for blob in blobs:
            blob_name = blob.name

            # Handle '**' wildcard (recursive matching)
            if '**' in wildcard_types:
                if '/' not in blob_name[len(prefix):]:
                    filtered_blobs.append(blob)
            
            # Handle '*' wildcard
            elif '*' in wildcard_types:
                pattern = prefix.replace('*', '*')
                if fnmatch.fnmatch(blob_name, pattern):
                    filtered_blobs.append(blob)
            
            # Handle '?' wildcard
            elif '?' in wildcard_types:
                pattern = prefix.replace('?', '?')
                if fnmatch.fnmatch(blob_name, pattern):
                    filtered_blobs.append(blob)
            
            # Handle character class '[]'
            elif '[]' in wildcard_types:
                if fnmatch.fnmatch(blob_name, prefix):
                    filtered_blobs.append(blob)
            
            # Handle brace expansion '{}'
            elif '{}' in wildcard_types:
                brace_content = prefix[prefix.find('{') + 1:prefix.find('}')]
                options = brace_content.split(',')
                for option in options:
                    pattern = prefix.replace(f'{{{brace_content}}}', option)
                    if fnmatch.fnmatch(blob_name, pattern):
                        filtered_blobs.append(blob)
                        break

        return filtered_blobs

    def _process_local(self, paths) -> list:
        """"""
        return paths

    def _handle_recursive(self, blobs: list, path: str) -> list:
        """
        Filters the blobs to only include those directly under the given path.

        Parameters
        ----------
        blobs : list
            List of Blob objects from Google Cloud Storage.
        path : str
            The path path to filter blobs.

        Returns
        -------
        list
            Filtered list of Blob objects.
        """

        if not path.endswith('/'):
            path += '/'

        direct_contents = set()
        
        for blob in blobs:
            if blob.name.startswith(path):
                relative_path = blob.name[len(path):]
                # Only consider direct children
                if '/' not in relative_path.strip('/'):
                    base_path = path + relative_path.split('/')[0]
                    direct_contents.add(base_path)

        filtered_blobs = [blob for blob in blobs if blob.name in direct_contents]
        return filtered_blobs

    def _handle_recursive_local(self, paths: list, path: str) -> list:
        """
        Filters the paths to only include those directly under the given path.

        Parameters
        ----------
        paths : list
            List of file or folder paths (strings).
        path : str
            The path path to filter the contents.

        Returns
        -------
        list
            Filtered list of paths (strings).
        """

        if not path.endswith('/'):
            path += '/'

        direct_contents = set()

        for path in paths:
            if path.startswith(path):
                relative_path = path[len(path):]
                # Only consider direct children
                if '/' not in relative_path.strip('/'):
                    base_path = path + relative_path.split('/')[0]
                    direct_contents.add(base_path)

        # Filter out paths that match direct contents
        filtered_paths = [path for path in paths if path in direct_contents]
        return filtered_paths

    def _convert_size(self, size_bytes: int) -> str:
        """Convert a size in bytes to a more human-readable format.

        Parameters
        ----------
        ize_bytes : int
            The size to convert, in bytes.

        Returns
        -------
            str: The size in a more human-readable format.

        Example
        -------
        >>> convert_size(10267)
        '10.03 KB'
        """
        if size_bytes == 0:
            return "0B"
        
        size_names = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = 0
        size = size_bytes

        while size >= 1024 and i < len(size_names) - 1:
            size /= 1024.0
            i += 1

        return f"{size:.2f} {size_names[i]}"
    
    def _check_wildcards(self, path: str) -> tuple:
        """Checks if the given path contains any wildcard characters and identifies the type of wildcard(s).

        Parameters
        ----------
        path : str
            - The path string to check.

        Returns
        -------
        tuple(bool, list(str)) (wildcard_exists, wildcard_types)
            - wildcard_exists (bool): True if any wildcard character is found, False otherwise.
            - wildcard_types (list of str): A list of the wildcard characters found in the path.
        """
        wildcard_types = []
        
        if '**' in path:
            wildcard_types.append('**')
        if '*' in path:
            wildcard_types.append('*')
        if '?' in path:
            wildcard_types.append('?')
        if '[' in path and ']' in path:
            wildcard_types.append('[]')
        if '{' in path and '}' in path:
            wildcard_types.append('{}')
        
        wildcard_exists = bool(wildcard_types)
        
        return wildcard_exists, wildcard_types

    def _format_datetime(self, dt: datetime) -> str:
        """
        Formats a datetime object to a more readable string.

        Parameters
        ----------
        dt : datetime
            The datetime object to format.

        Returns
        -------
        str
            The formatted datetime string, e.g., "2024-08-27 14:47:00 UTC".

        Example
        -------
        >>> _format_datetime(datetime.datetime(2024, 8, 27, 14, 47, 0, 156000, tzinfo=datetime.timezone.utc))
        '2024-08-27 14:47:00 UTC'
        """
        if dt is None:
            return "N/A"  # Return a placeholder if the datetime is None

        # Format the datetime object to a string: "YYYY-MM-DD HH:MM:SS UTC"
        formatted_datetime = dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        return formatted_datetime
    
    def _md_silent(self, path: str) -> tuple:
        """Create a new directory (prefix) in a Google Cloud Storage bucket.

        Parameters
        ----------
        path : str
            The name of the directory to create.

        Returns
        -------
        Dict[str, Any]
            A confirmation message indicating the directory was created.

        Raises
        ------
        Exception
            If an error occurs while creating the directory.

        Examples
        --------
        >>> md(directory='my-bucket/new-directory/')
        """
        try:
            if not self.authenticated or self.client is None:
                raise Exception("You must authenticate first before creating a directory.")
            
            if not path:
                raise ValueError("Directory (bucket path) must be specified.")

            # Handle cases where 'gs://' is missing
            if path.startswith('gs://'):
                path = path[5:]
            
            if '/' not in path:
                raise ValueError("Invalid path format. It should be in the form 'bucket-name/directory/'")

            bucket_name, prefix = path.split('/', 1)

            if not prefix.endswith('/'):
                prefix += '/'

            bucket = self.client.bucket(bucket_name)
            
            # Create an empty blob with a trailing slash to simulate a path
            blob = bucket.blob(prefix)
            blob.upload_from_string('')

            return True, ""
        
        except Exception as e:
            return False, e

    def _copy_gcs_to_gcs(self, source: str, destination: str, recursive: bool, delete: False) -> List[Dict[str, Any]]:
        """Copy blobs from one Google Cloud Storage (GCS) location to another, with optional recursive and wildcard support.

        This method copies files from a source GCS bucket and prefix to a destination GCS bucket and prefix. It supports wildcard patterns in the source path to select multiple blobs and handles recursive copying of blobs if requested.

        Parameters
        ----------
        source : str
            The source GCS path in the format `'gs://bucket-name/prefix'`. This is the location from which blobs will be copied. Wildcards can be used in the prefix to select multiple blobs.
        
        destination : str
            The destination GCS path in the format `'gs://bucket-name/prefix'`. This is the location where the blobs will be copied.
        
        recursive : bool
            If `True`, all files and subdirectories under the source prefix will be copied. If `False`, only files directly under the top-level source prefix (non-recursively) will be copied.

        delete : bool
            If `True` after moved the blobs will be deleted. If `False` the blobs will not be removed.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries detailing the results of the copy operation for each blob. Each dictionary contains:
            - 'success' (bool): Indicates whether the blob was copied successfully.
            - 'path' (str): The source path of the blob.
            - 'target_path' (str): The destination path of the blob.
            - 'message' (str): A message summarizing the outcome of the copy operation.

        Raises
        ------
        Exception
            If an error occurs during the blob copy process, the exception will be caught, and a detailed error message will be logged.

        Notes
        -----
        - The method handles wildcard patterns (e.g., `*`, `**`) in the source path to match multiple blobs.
        - Supports recursive copying when `recursive` is `True`. If `recursive` is `False`, only files directly under the source prefix will be processed.
        - Each blob is copied to the destination bucket with its relative path preserved.

        Example
        -------
        Copying blobs between GCS buckets:
        >>> _copy_gcs_to_gcs('gs://source-bucket/folder/', 'gs://destination-bucket/new-folder/', recursive=True)
        [
            {
                'success': True,
                'path': 'gs://source-bucket/folder/file1.txt',
                'target_path': 'gs://destination-bucket/new-folder/file1.txt',
                'message': 'Copied file from "gs://source-bucket/folder/file1.txt" to "gs://destination-bucket/new-folder/file1.txt"'
            },
            ...
        ]
        """
        details                                 = []
        source_bucket_name, source_prefix       = source[5:].split('/', 1)                                  # Get source bucket and prefix
        destination_bucket_name, destination_prefix   = destination[5:].split('/', 1)                       # Get destination bucket and prefix
        source_bucket                           = self.client.bucket(source_bucket_name)                    # Get source bucket
        destination_bucket                      = self.client.bucket(destination_bucket_name)               # Get destination bucket
        processed_source_prefix                 = source_prefix                                             # Set retreval prefix

        # Identify Wildcards
        wildcard_exists, wildcard_types         = self._check_wildcards(source_prefix)                      # Determine if wildcards are used
        if wildcard_exists:                                                                                 # Update retreval prefix if wildcard is used
            last_slash_index                    = source.rfind('/')
            _, processed_source_prefix          = source[:last_slash_index + 1][5:].split('/', 1)

        # Get blobs
        blobs                                   = list(source_bucket.list_blobs(prefix=processed_source_prefix))
                
        # Process recursive
        _blobs = blobs
        if not recursive and len(blobs) > 1:
            _blobs = self._handle_recursive(blobs=blobs, path=processed_source_prefix)                      # Handle Recursive blobs
                    
        # Additional blob processing
        _blobs = self._handle_wildcards(                                                                    # Process blobs
            prefix=processed_source_prefix, blobs=_blobs, wildcard_exists=wildcard_exists, wildcard_types=wildcard_types
        )
        logging_blob_count                      = len(_blobs)                                               # Processed blob count
                
        # Logging Values
        if logging_blob_count == 0:
            self.logger.error(f'''Failed to find blob(s) "{source}"''')
            return details
        else: 
            if logging_blob_count > 1:
                self.logger.info(f'''Downloading path "{source}" to "{destination}"''')
                
        # Iterate over blobs
        for source_blob in _blobs:
            logging_source_blob_name            = source_blob.name                                          # Details source name
            logging_destination_blob_name       = ""                                                        # Details destination name

            try:
                relative_path                   = source_blob.name[len(source_prefix):].lstrip('/')         # Create source relative path
                
                # Handle blob names
                if not source_blob.name.endswith('/'):
                    destination_blob_name       = os.path.join(destination_prefix, relative_path).replace("\\", "/").rstrip('/')
                else:
                    destination_blob_name       = os.path.join(destination_prefix, relative_path).replace("\\", "/")
                
                # Logging Values
                logging_source_blob_name        = os.path.join(f"gs://{source_bucket_name}", source_blob.name).replace("\\", "/")
                logging_destination_blob_name   = os.path.join(f"gs://{destination_bucket_name}/{destination_prefix}", relative_path).replace("\\", "/")
                
                # Copy file
                blob_copy = source_bucket.copy_blob(
                    source_blob, destination_bucket, destination_blob_name
                )
                if delete:
                    source_blob.delete()
                
                message = f'''Copied file from "{logging_source_blob_name}" to "{logging_destination_blob_name}"'''
                details.append({"success": True, "path": logging_source_blob_name, "target_path": destination_blob_name, "message": message})
                self.logger.info(message)
            except Exception as e:
                message = f'''Failed to copy "{source_blob}": {str(e)}'''
                details.append({"success": False, "path": logging_source_blob_name, "target_path": destination_blob_name, "message": message})
                self.logger.error(message)
                
        return details

    def _download_from_gcs(self, source: str, destination: str, recursive: bool) -> List[Dict[str, Any]]:
        """Download blobs from a Google Cloud Storage (GCS) bucket to a local directory, with optional recursive and wildcard support.

        This method downloads files from a specified GCS bucket and prefix to a local directory. It supports recursive downloads of all files under a directory and can handle wildcard patterns in the source path for selecting specific files.

        Parameters
        ----------
        source : str
            The GCS path to the source files in the format `'gs://bucket-name/prefix'`. Wildcards can be used in the prefix to select specific files.
        
        destination : str
            The local directory path where the files will be downloaded. The method creates the necessary directory structure if it doesn't exist.
        
        recursive : bool
            If `True`, all files and subdirectories under the source prefix will be downloaded recursively. If `False`, only files directly under the top-level source prefix will be downloaded.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries detailing the results of the download operation for each blob. Each dictionary contains:
            - 'success' (bool): Indicates whether the blob was downloaded successfully.
            - 'path' (str): The source GCS path of the blob.
            - 'target_path' (str): The local destination path of the downloaded file.
            - 'message' (str): A message summarizing the outcome of the download operation.

        Raises
        ------
        Exception
            If an error occurs during the download process, the exception will be caught, and a detailed error message will be logged.

        Notes
        -----
        - Supports wildcard patterns (e.g., `*`, `**`) in the source path to select multiple blobs.
        - When `recursive` is `True`, the method downloads all files under the source prefix. If `recursive` is `False`, only the top-level files are downloaded.
        - Automatically creates any necessary directories on the local filesystem to match the structure of the GCS source prefix.
        - Files are downloaded to the `destination` directory, with their relative path preserved from the GCS source.

        Example
        -------
        Downloading files from a GCS bucket:
        >>> _download_from_gcs('gs://my-bucket/folder/', '/local/directory/', recursive=True)
        [
            {
                'success': True,
                'path': 'gs://my-bucket/folder/file1.txt',
                'target_path': '/local/directory/file1.txt',
                'message': 'Downloaded file from "gs://my-bucket/folder/file1.txt" to "/local/directory/file1.txt"'
            },
            ...
        ]
        """
        details                                 = []
        source_bucket_name, source_prefix       = source[5:].split('/', 1)                                  # Get source bucket and prefix
        source_bucket                           = self.client.bucket(source_bucket_name)                    # Get source bucket
        processed_source_prefix                 = source_prefix

        # Identify Wildcards
        wildcard_exists, wildcard_types         = self._check_wildcards(source_prefix)                      # Determine if wildcards are used
        if wildcard_exists:                                                                                 # Update retreval prefix if wildcard is used
            last_slash_index                    = source.rfind('/')
            _, processed_source_prefix          = source[:last_slash_index + 1][5:].split('/', 1)

        # Get blobs
        blobs                                   = list(source_bucket.list_blobs(prefix=processed_source_prefix))
                
        # Process recursive
        _blobs = blobs
        if not recursive and len(blobs) > 1:
            _blobs = self._handle_recursive(blobs=blobs, path=processed_source_prefix)                      # Handle Recursive blobs
                    
        # Additional blob processing
        _blobs = self._handle_wildcards(                                                                    # Process blobs
            prefix=processed_source_prefix, blobs=_blobs, wildcard_exists=wildcard_exists, wildcard_types=wildcard_types
        )
        logging_blob_count                      = len(_blobs)                                               # Processed blob count
                
        # Logging Values
        if logging_blob_count == 0:
            self.logger.error(f'''Failed to find blob(s) "{source}"''')
            return details
        else: 
            if logging_blob_count > 1:
                self.logger.info(f'''Downloading path "{source}" to "{destination}"''')
        
        # Iterate over blobs
        for source_blob in _blobs:
            logging_source_blob_name            = source_blob.name                                          # Details source name
            logging_destination_path_name       = ""                                                        # Details destination name
            message                             = ""                                                        # Details message
                                
            try:
                relative_path                   = source_blob.name[len(source_prefix):].lstrip('/')         # Create source relative path
                
                # Handle blob names
                if logging_blob_count == 1 and not source_blob.name.endswith('/'):
                    destination_local_path = destination.replace("\\", "/")
                else:
                    destination_local_path = os.path.join(destination, relative_path).replace("\\", "/")
                                        
                # Logging Values
                logging_source_blob_name        = os.path.join(f"gs://{source_bucket_name}", source_blob.name).replace("\\", "/")
                logging_destination_path_name   = destination_local_path
                
                # Check if directory exists
                if not os.path.exists(os.path.dirname(destination_local_path)):
                    os.makedirs(os.path.dirname(destination_local_path))

                # Download the file
                source_blob.download_to_filename(destination_local_path)
                
                message = f'''Downloaded file from "{logging_source_blob_name}" to "{logging_destination_path_name}"'''
                details.append({"success": True, "path": logging_source_blob_name, "target_path": logging_destination_path_name, "message": message})
                self.logger.info(message)
            except Exception as e:
                message = f'''Failed to download "{source_blob}": {str(e)}'''
                details.append({"success": False, "path": logging_source_blob_name, "target_path": logging_destination_path_name, "message": message})
                self.logger.error(message)

        return details

    def _upload_to_gcs(self, source: str, destination: str, recursive: bool) -> List[Dict[str, Any]]:
        """Upload files or directories from a local file system to a Google Cloud Storage (GCS) bucket.

        This method uploads files or directories from a local path to a specified GCS bucket and prefix. It supports recursive uploads for directories and handles both files and folders. Wildcard patterns can be used in the destination path.

        Parameters
        ----------
        source : str
            The local file or directory path to upload. If a directory is specified, its contents are uploaded based on the `recursive` flag.
        
        destination : str
            The GCS destination path in the format `'gs://bucket-name/prefix'`. If wildcards are used in the prefix, the method will handle them accordingly.
        
        recursive : bool
            If `True`, all files and subdirectories under the source directory are uploaded recursively. If `False`, only the top-level directory or file is uploaded.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries detailing the result of the upload operation for each file or folder. Each dictionary contains:
            - 'success' (bool): Indicates whether the upload was successful.
            - 'path' (str): The local source path of the file or folder.
            - 'target_path' (str): The GCS destination path of the uploaded file or folder.
            - 'message' (str): A message summarizing the outcome of the upload operation.

        Raises
        ------
        Exception
            If an error occurs during the upload process, it is caught, and a detailed error message is logged.

        Notes
        -----
        - Supports uploading individual files and directories.
        - Wildcard patterns (e.g., `*`, `**`) in the destination path are supported.
        - If `recursive` is set to `True`, the method uploads all files and subdirectories within the source directory.
        - Automatically creates necessary directories in the GCS bucket to match the local file structure.
        - If the destination path is a folder (or if multiple files are uploaded), the method ensures the correct structure by appending the file or folder name to the destination prefix.
        - Handles cases where the destination path includes folders, ensuring that they are created in the GCS bucket if they don't already exist.

        Example
        -------
        Uploading files from a local directory to a GCS bucket:
        >>> _upload_to_gcs('/local/directory/', 'gs://my-bucket/folder/', recursive=True)
        [
            {
                'success': True,
                'path': '/local/directory/file1.txt',
                'target_path': 'gs://my-bucket/folder/file1.txt',
                'message': 'Uploaded file from "/local/directory/file1.txt" to "gs://my-bucket/folder/file1.txt"'
            },
            ...
        ]
        """
        details                                 = []
        destination_bucket_name, destination_prefix = destination[5:].split('/', 1)                         # Get source bucket and prefix
        destination_bucket                      = self.client.bucket(destination_bucket_name)               # Get destination bucket
                
        # Identify Wildcards
        wildcard_exists, wildcard_types         = self._check_for_wildcards(destination_prefix)             # Determine if wildcards are used
        if wildcard_exists:                                                                                 # Update retreval prefix if wildcard is used
            last_slash_index                    = destination.rfind('/')
            _, processed_destination_prefix     = destination[:last_slash_index + 1][5:].split('/', 1)
        
        # Get paths
        if os.path.isfile(source):
            paths =  [source.replace("\\", "/")]
        elif os.path.isdir(source):
            paths = []
            for root, dirs, filenames in os.walk(source):
                paths.append(root.replace("\\", "/"))
                for filename in filenames:
                    paths.append(os.path.join(root, filename).replace("\\", "/"))
                    
        # Process recursive
        processed_paths = paths
        if not recursive and len(paths) > 1:
            processed_paths = self._handle_recursive_local(paths=paths, path=processed_destination_prefix)  # Handle Recursive blobs
            
        # Additional blob processing
        processed_paths = self._process_local(                                                              # Process blobs
            paths=processed_paths
        )
        logging_blob_count                      = len(processed_paths)                                      # Processed blob count
        
        # Logging Values
        if logging_blob_count == 0:
            self.logger.error(f'''Failed to find blob(s) "{source}"''')
            return details
        else: 
            if logging_blob_count > 1:
                self.logger.info(f'''Downloading path "{source}" to "{destination}"''')
          
        for source_path in processed_paths:
            logging_source_path_name            = source_path                                               # Details source name
            logging_destination_blob_name       = ""                                                        # Details destination name
            message                             = ""                                                        # Details message
            
            try:
                relative_path                   = source_path[len(source):].lstrip('/')                     # Create source relative path
            
                # Handle blob names
                if os.path.isfile(source_path):
                    destination_path_name       = os.path.join(destination_prefix, relative_path).replace("\\", "/").rstrip('/')
                    logging_source_path_name        = source_path.replace("\\", "/")
                    logging_destination_blob_name   = os.path.join(f"gs://{destination_bucket_name}/{destination_prefix}", relative_path).replace("\\", "/")
                    
                    self.logger.info(f"File creation: {destination_path_name}")
                    
                    # Upload files
                    blob = destination_bucket.blob(destination_path_name)
                    blob.upload_from_filename(source_path)
                    
                    message = f'''Uploaded file from "{logging_source_path_name}" to "{logging_destination_blob_name}"'''
                    details.append({"success": True, "path": logging_source_path_name, "target_path": logging_destination_blob_name, "message": message})
                    self.logger.info(message)
                elif os.path.isdir(source):
                    destination_path_name       = os.path.join(f"{destination_bucket_name}/{destination_prefix}", relative_path).replace("\\", "/")
                    
                    # Logging Values
                    logging_source_path_name        = source_path.replace("\\", "/")
                    logging_destination_blob_name   = os.path.join(f"gs://{destination_bucket_name}/{destination_prefix}", relative_path).replace("\\", "/")
                    
                    self.logger.info(f"Folder creation: {destination_path_name}")
                    
                    # Create folder
                    _success, _response = self._md_silent(destination_path_name)
                    
                    # Handle create folder logic
                    if _success:
                        message = f'''Uploaded folder from "{logging_source_path_name}" to "{logging_destination_blob_name}"'''
                        details.append({"success": True, "path": logging_source_path_name, "target_path": logging_destination_blob_name, "message": message})
                        self.logger.info(message)
                    else:
                        message = f'''Failed to upload folder from "{logging_source_path_name}" to "{logging_destination_blob_name}": {_response}'''
                        details.append({"success": False, "path": logging_source_path_name, "target_path": logging_destination_blob_name, "message": message})
                        self.logger.info(message)
            except Exception as e:
                message = f'''Failed to upload "{source_path}": {str(e)}'''
                details.append({"success": False, "source": logging_source_path_name, "target_path": logging_destination_blob_name, "message": message})
                self.logger.error(message)

        return details