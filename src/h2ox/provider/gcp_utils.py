from typing import Dict

import io

from google.cloud import storage

def download_blob(url: str) -> io.BytesIO:
    """Download a blob as bytes
    Args:
        url (str): the url to download
    Returns:
        io.BytesIO: the content as bytes
    """
    storage_client = storage.Client()
    
    bucket_id = url.split('/')[0]
    file_path = '/'.join(url.split('/')[1:])
    
    bucket = storage_client.bucket(bucket_id)
    blob = bucket.blob(file_path)
    f = io.BytesIO(blob.download_as_bytes())
    return f

def upload_blob(source_directory: str, target_directory: str):
    """Function to save file to a bucket.
    Args:
        target_directory (str): Destination file path.
        source_directory (str): Source file path
    Returns:
        None: Returns nothing.
    Examples:
        >>> target_directory = 'target/path/to/file/.pkl'
        >>> source_directory = 'source/path/to/file/.pkl'
        >>> save_file_to_bucket(target_directory)
    """

    client = storage.Client()
    
    bucket_id = target_directory.split('/')[0]
    file_path = '/'.join(target_directory.split('/')[1:])

    bucket = client.get_bucket(bucket_id)

    # get blob
    blob = bucket.blob(file_path)

    # upload data
    blob.upload_from_filename(source_directory)

    return target_directory

def download_cloud_json(bucket_name: str, filename: str, **kwargs) -> Dict:
    """
    Function to load the json data for the WorldFloods bucket using the filename
    corresponding to the image file name. The filename corresponds to the full
    path following the bucket name through intermediate directories to the final
    json file name.
    Args:
      bucket_name (str): the name of the Google Cloud Storage (GCP) bucket.
      filename (str): the full path following the bucket_name to the json file.
    Returns:
      The unpacked json data formatted to a dictionary.
    """
    # initialize client
    client = storage.Client(**kwargs)
    # get bucket
    bucket = client.get_bucket(bucket_name)
    # get blob
    blob = bucket.blob(filename)
    # check if it exists
    # TODO: wrap this within a context
    return json.loads(blob.download_as_string(client=None))