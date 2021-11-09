from loguru import logger
from typing import Tuple, List
import os
from joblib import delayed
from joblib import Parallel
from tqdm import tqdm
from datetime import datetime

import zarr
import numpy as np
import xarray as xr
import dask

from h2ox.provider import mapper, prefix
from h2ox.data import ERA5_VARIABLES, FORECAST_VARIABLES
from h2ox.data.utils import tqdm_joblib

def transfer_worker(
    z_src: zarr.core.Array,
    z_dst: zarr.core.Array,
    slice_tuple: Tuple[slice]
):
    
    #array reorders to lat,lon,time
    lat_slice = slice_tuple[0] 
    lon_slice = slice_tuple[1]
    time_slice = slice_tuple[2]
    
    arr = z_src[lon_slice,lat_slice,time_slice]
    
    z_dst[lat_slice,lon_slice,time_slice] = arr.transpose(1,0,2).astype(np.float32) 
    
    return 1

def transfer_era5(
    source: str,
    target: str,
    min_dt: datetime,
    max_dt: datetime,
    zero_dt: datetime,
    keys: List[str],
    n_workers: int,
):
    
    start_dt_idx = int((min_dt-zero_dt).total_seconds()/60/60)
    end_dt_idx = int((max_dt-zero_dt).total_seconds()/60/60)
    
    logger.info('Running data transfer')
    
    zx_src = xr.open_zarr(mapper(prefix+os.path.join(source)))
        
    slices = dask.array.core.slices_from_chunks(tuple(zx_src.chunks.values()))
    
    logger.info(f'Got {len(slices)} slices, transfering {len(keys)} keys.')
    
    # filter for time steps
    slices = [s for s in slices if s[2].stop>start_dt_idx and s[2].start<end_dt_idx]
    
    # filter for land coverage
    mask = np.load(os.path.join(os.getcwd(),'bin','./era5_land_mask.npz'))['mask']
    
    slices = [s for s in slices if (mask[s[1],s[0]]==False).sum()>0]
    
    logger.info(f'Filtered slices leaving {len(slices)} slices')
    
    
    
    for kk in keys:
        
        jobs=[]
        
        z_src = zarr.open(mapper(prefix+os.path.join(source,kk)))
        z_dst = zarr.open(mapper(prefix+os.path.join(target,kk)))
    
        for slice_tuple in slices:
            jobs.append(
                delayed(transfer_worker)(
                    z_src,
                    z_dst,
                    slice_tuple,
                )
            )
            
        with tqdm_joblib(
            tqdm(desc=f"Transfering {kk} from {source} to {target} with {n_workers} workers", total=len(jobs)),
        ):
            Parallel(n_jobs=n_workers, verbose=0, prefer="threads")(jobs)

    return 1
    
if __name__=="__main__":
    
    transfer_era5(
        source = 'oxeo-era5/lk-test-build',
        target = 'oxeo-era5/build',
        min_dt = datetime(2010,1,1,0,0),
        max_dt = datetime(2021,1,1,0,0),
        zero_dt = datetime(1981,1,1,0,0),
        keys = ['tp'],
        n_workers= 30,
    )