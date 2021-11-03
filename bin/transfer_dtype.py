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
    key_source: str,
    key_dest: str,
    slice_tuple: Tuple[slice]
):
    
    z_src = zarr.open(mapper(prefix+key_source))
    z_dst = zarr.open(mapper(prefix+key_dest))
    
    arr = z_src[slice_tuple[0],slice_tuple[1],slice_tuple[2]]
    
    z_dst[slice_tuple[0],slice_tuple[1],slice_tuple[2]] = arr.astype(np.float32) 
    
    return 1

def transfer(
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
    
    slices = [s for s in slices if s[2].stop<start_dt_idx or s[2].start>end_dt_idx]
    
    logger.info(f'Filtered slices leaving {len(slices)} slices')
    
    for kk in keys:
        
        jobs=[]
    
        for slice_tuple in slices:
            jobs.append(
                delayed(transfer_worker)(
                    os.path.join(source,kk),
                    os.path.join(target,kk),
                    slice_tuple,
                )
            )
            
        with tqdm_joblib(
            tqdm(desc=f"Transfering {kk} from {source} to {target} with {n_workers} workers", total=len(jobs)),
        ):
            Parallel(n_jobs=n_workers, verbose=0, prefer="threads")(jobs)

        return 1
    
if __name__=="__main__":
    
    transfer(
        source = 'oxeo-era5/lk-test-build',
        target = 'oxeo-era5/build',
        min_dt = datetime(2010,1,1,0,0),
        max_dt = datetime(2020,12,31,23,0),
        zero_dt = datetime(1981,1,1,0,0),
        keys = list(ERA5_VARIABLES.keys()),
        n_workers= 4,
    )