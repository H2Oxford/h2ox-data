import os

if os.environ['PROVIDER']=='GCP':
    
    from gcsfs import GCSFileSystem
    
    fs = GCSFileSystem(token=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    
    mapper = fs.get_mapper
    
    prefix = 'gs://'
    
else:
    
    raise NotImplementedError("Only GCP implemented")