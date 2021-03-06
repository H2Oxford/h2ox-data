import os

if os.environ['PROVIDER']=='GCP':
    
    from gcsfs import GCSFileSystem
    from h2ox.provider.gcp_utils import download_blob, upload_blob, download_cloud_json, cloud_file_exists, download_blob_to_filename
    
    # logic for credentials if not on VPN?
    fs = GCSFileSystem()
    
    mapper = fs.get_mapper
    
    prefix = 'gs://'
    
else:
    
    raise NotImplementedError("Only GCP implemented")