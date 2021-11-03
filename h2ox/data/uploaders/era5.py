
def nc_to_zarr(gcs_root, f, n_workers, logger):
    
    year, month, var = os.path.splitext(os.path.split(f)[1])[0].split('_')
    
    logger.info(f'PUSH: {year} {month} {var}')
    
    # get the ds object
    ds = xr.open_dataset(f,chunks={'longitude':10, 'latitude':10, 'time':35064})
    
    # get slices to write
    slices = dask.array.core.slices_from_chunks(dask.array.empty_like(ds.to_array()).chunks) # null, time, lat, lon
    
    # eliminate the slices which hit the boundary
    slices = [s for s in slices if s[2].stop!=1801]
    logger.info(f'N_slices: {len(slices)}')
    
    # prep for multiprocessing
    chunk_worker = len(slices)//n_workers+1
    slices_rechunked = [slices[chunk_worker*ii:chunk_worker*(ii+1)] for ii in range(n_workers)]
    
    # prep the pool for writing
    pool = mp.Pool(n_workers)
    
    time_offset = int((dt.strptime(f'{year}-{month}-01', '%Y-%m-%d') - dt.strptime('1981-01-01','%Y-%m-%d')).total_seconds()/3600)
    
    args = [
        (
        f, 
        var, 
        gcs_root, 
        slices_rechunked[ii], 
        time_offset,
        ii
        ) for ii in range(n_workers)
    ]

    results = pool.starmap(write_worker_mp, args)
    
    return np.prod(results)

def push_month(gcs_root, f, n_workers):
    
    year, month, var = os.path.splitext(os.path.split(f)[1])[0].split('_')
    
    logger = logging.getLogger(f'PUSH-{year}-{month}-{var}')
    
    # get the ds object
    ds = xr.open_dataset(f,chunks={'longitude':10, 'latitude':10, 'time':35064})
    
    # get slices to write
    slices = dask.array.core.slices_from_chunks(dask.array.empty_like(ds.to_array()).chunks) # null, time, lat, lon
    
    # eliminate the slices which hit the boundary
    slices = [s for s in slices if s[2].stop!=1801]
    logger.info(f'Pushing {len(slices)}')
    
    # prep for multiprocessing
    chunk_worker = len(slices)//n_workers+1
    slices_rechunked = [slices[chunk_worker*ii:chunk_worker*(ii+1)] for ii in range(n_workers)]
    
    # prep the pool for writing
    pool = mp.Pool(n_workers)
    
    time_offset = int((dt.strptime(f'{year}-{month}-01', '%Y-%m-%d') - dt.strptime('1981-01-01','%Y-%m-%d')).total_seconds()/3600)
    
    args = [
        (
        f, 
        var, 
        gcs_root, 
        slices_rechunked[ii], 
        time_offset,
        ii
        ) for ii in range(n_workers)
    ]

    results = pool.starmap(write_worker_mp, args)
    
    return np.prod(results)


def write_worker_ecmwf(filename, gcs_path, slices, worker_idx):
    
    logger = logging.getLogger(f'worker_{worker_idx}')
    
    year, month = os.path.splitext(os.path.split(filename)[1])[0].split('_')
    
    time_offset = (dt(year=int(year), month=int(month), day=1) - dt(year=2010,month=1,day=1)).days
    
    ds_worker = xr.open_dataset(filename,engine='cfgrib')
    
    store = gcsfs.GCSMap(root=gcs_path)
    z = zarr.open(store)
    
    variables = {
        0:'t2m',
        1:'tp',
    }
    
    for ii_s, s in enumerate(slices):
        
        variable = variables[s[0].start]
        
        if ii_s % 10==0:
            logger.info(f'ii_s:{ii_s}')
        
        time_slice = s[1]
        offset_slice = slice(s[1].start+time_offset, s[1].stop+time_offset)
        step_slice = s[2]
        lat_slice = s[3]
        lon_slice = s[4]
        z[variable][lon_slice, lat_slice, offset_slice, step_slice] = np.transpose(
            np.squeeze(
                ds_worker[variable][time_slice, step_slice, lat_slice, lon_slice]
            ),
            [3,2,0,1]
        )
        
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