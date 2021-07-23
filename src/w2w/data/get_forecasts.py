import logging
logging.basicConfig(level=logging.INFO)
from itertools import product
import os

import netCDF4 as nc
import gcsfs
import numpy as np
import zarr
import multiprocessing as mp
import dask.array
import xarray as xr

from workers import ecmwf_caller, write_worker_ecmwf


def loop_forecasts(yrmonths, n_workers, gcs_root):
    
    #pool = mp.Pool(n_workers)
    
    logger = logging.getLogger('ecmwf_main_loop')
    
    # loop the year-months
    for year, month in yrmonths:
        
        
        fname = os.path.join(os.getcwd(),'data',f'{year}_{month:02d}.grib')
        
        # download the forecasts
        logger.info(f'Calling downloader: {fname}')
        if not int(year)==2010 and not int(month)==1:
            ecmwf_caller(fname)
        
        ###  get slices
        # open dataset
        ds = xr.open_dataset(fname, chunks = {'longitude':10,'latitude':10,'steps':61,'time':1461}, engine='cfgrib')
        
        # get slices to write
        slices = dask.array.core.slices_from_chunks(dask.array.empty_like(ds.to_array()).chunks) # null, time, lat, lon
        
        # filter slices
        slices = [s for s in slices if s[3].stop!=361]
        logger.info(f'Got {len(slices)} slices')
        
        # prep for multiprocessing
        chunk_worker = len(slices)//n_workers+1
        slices_rechunked = [slices[chunk_worker*ii:chunk_worker*(ii+1)] for ii in range(n_workers)]

        # prep the pool for writing
        
        logger.info('Calling multiprocess upload')
        
        args = [
            (
            fname,  
            gcs_root, 
            slices_rechunked[ii], 
            ii
            ) for ii in range(n_workers)
        ]
        
        # try single-threaded
        for arg_worker in args:
            
            write_worker_ecmwf(*arg_worker)

        #results = pool.starmap(write_worker_ecmwf, args)
        
        logger.info(f'removing {fname}')
        # os.remove(fname)

    return 1

def download_only(yrmonths):
    logger = logging.getLogger('ecmwf_main_loop')
    
    # loop the year-months
    for year, month in yrmonths:
        
        
        fname = os.path.join(os.getcwd(),'data',f'{year}_{month:02d}.grib')
        
        # download the forecasts
        logger.info(f'Calling downloader: {fname}')
        try:
            ecmwf_caller(fname)
        except Exception as e:
            logger.info(f'ERROR! {fname}')
            logger.info(e)


if __name__=="__main__":
    
    yrmonths = [t for t in product(list(range(2017,2018)),list(range(1,13)))]
    
    #gcs_root = 'oxeo-forecasts/ecmwf-tigge-15day'
    #loop_forecasts(yrmonths,16,gcs_root)
    
    download_only(yrmonths)
    
    