import logging
logging.basicConfig(level=logging.INFO)

import netCDF4 as nc
import gcsfs
import numpy as np
import zarr

def write_worker_mp(filename, variable, gcs_path, slices, time_offset, worker_idx):
    
    logger = logging.getLogger(f'worker_{worker_idx}')
    
    # open the dataset and the zarr
    ds_worker = nc.Dataset(filename)
    store = gcsfs.GCSMap(root=gcs_path)
    z = zarr.open(store)
    
    # write each slice
    for ii_s,subslice in enumerate(slices):
        if ii_s % 100 ==0:
            logger.info(f'ii_s:{ii_s}')
        time_slice = subslice[1]
        offset_slice = slice(subslice[1].start+time_offset, subslice[1].stop+time_offset)
        lat_slice = subslice[2]
        lon_slice = subslice[3]
        z[variable][lon_slice, lat_slice, offset_slice] = np.transpose(
            np.squeeze(
                ds_worker[variable][time_slice, lat_slice, lon_slice]
            ),
            [2,1,0]
        )
        
        
    return 1