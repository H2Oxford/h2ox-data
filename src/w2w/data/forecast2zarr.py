import os, glob
from datetime import datetime as dt
import logging
import xarray as xr
import dask.array
logging.basicConfig(level=logging.INFO)

import numpy as np

import multiprocessing as mp
from multiprocessing.shared_memory import SharedMemory

from workers import sharedmem_worker


def forecasts2zarr(grib_dir, start_dt, end_dt, gcs_root, variables, n_workers):
    
    logger=logging.getLogger('FORECASTS2ZARR')
    
    # get the fnames
    
    fs = sorted(glob.glob(os.path.join(grib_dir, '*.grib')))
    
    # convert to records
    records = []
    for f in fs:
        year, month = os.path.splitext(os.path.split(f)[1])[0].split('_')    
        records.append({
            'year':int(year),
            'month':int(month),
            'dt_obj':dt(year=int(year), month=int(month), day=1),
            'f':f,
        })
    
    # filter records
    run_records = [r for r in records if r['dt_obj']>=start_dt and r['dt_obj']<end_dt]
    
    logger.info(f'Got {len(run_records)} to run.')
    
    pool = mp.Pool(n_workers)
    
    # main loop
    for r in run_records:
        
        ds_dask =  xr.open_dataset(r['f'], chunks = {'longitude':10,'latitude':10,'steps':61,'time':1461}, engine='cfgrib')
        ds =  xr.open_dataset(r['f'], engine='cfgrib')
        
        for variable in variables:
            
            logger.info(f'Getting data {r["f"]} - {variable}')
            
            data = ds[variable].values
            
            # transpose to correct shape
            data = np.transpose(data, [3,2,0,1])
            
            logger.info('assigning to sharedemem')
            # create the shared memory space
            shm = SharedMemory(create=True, size=data.nbytes)

            # create the shared array
            data_shm = np.ndarray(data.shape, dtype=data.dtype, buffer=shm.buf)

            # write the data into shared mem
            data_shm[:] = data[:]

            shm_spec = {
                'name':shm.name,
                'shape':data.shape,
                'dtype':data.dtype
            }
            
            # load the slices
            slices = dask.array.core.slices_from_chunks(dask.array.empty_like( ds_dask.to_array()).chunks) # null, time, lat, lon
            
            # eliminate the slices which hit the boundary and only the first variable
            slices = [s for s in slices if s[3].stop!=361 and s[0].start==0]
            
            logger.info(f'Got {len(slices)} slices')
            # prep for multiprocessing
            chunk_worker = len(slices)//n_workers+1
            slices_rechunked = [slices[chunk_worker*ii:chunk_worker*(ii+1)] for ii in range(n_workers)]
            
            time_offset = int((dt.strptime(f'{year}-{month}-01', '%Y-%m-%d') - dt.strptime('2010-01-01','%Y-%m-%d')).days)
    
            args = [
                (
                shm_spec, 
                variable, 
                gcs_root, 
                slices_rechunked[ii], 
                time_offset,
                ii
                ) for ii in range(n_workers)
            ]
        
            logger.info('Calling MP Pool with sharedmem')
            #single threaded first
            #for arg in args:
            #    sharedmem_worker(*arg)
        
            pool.starmap(sharedmem_worker, args)
            
            del data_shm  # Unnecessary; merely emphasizing the array is no longer used
            shm.close()
            shm.unlink()  # Free and release the shared memory block at the very end

            



if __name__=="__main__":
    
    forecasts2zarr(
        grib_dir=os.path.join(os.getcwd(),'data'), 
        start_dt=dt(year=2012, month=1, day=1), 
        end_dt=dt(year=2021, month=1, day=1), 
        gcs_root='oxeo-forecasts/ecmwf-tigge-15day',
        variables=['t2m','tp'],
        n_workers=60
    )