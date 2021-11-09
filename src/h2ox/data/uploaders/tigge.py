

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
            
def sharedmem_worker(shm_spec, variable, gcs_path, slices, time_offset, worker_idx):
    
    logger = logging.getLogger(f'worker_{worker_idx}')
    
    # open the dataset from sharedmemory
    existing_shm = shared_memory.SharedMemory(name=shm_spec['name'])
    data = np.ndarray(shm_spec['shape'], dtype=shm_spec['dtype'], buffer=existing_shm.buf)
    
    # open the zarr
    store = gcsfs.GCSMap(root=gcs_path)
    z = zarr.open(store)
    
    # write each slice
    for ii_s,s in enumerate(slices):
        if ii_s % 100 ==0:
            logger.info(f'ii_s:{ii_s}')
            
        time_slice = s[1]
        offset_slice = slice(s[1].start+time_offset, s[1].stop+time_offset)
        step_slice = s[2]
        lat_slice = s[3]
        lon_slice = s[4]
        z[variable][lon_slice, lat_slice, offset_slice, step_slice] = data[lon_slice, lat_slice, time_slice, step_slice]
        
        
    #del data  # Unnecessary; merely emphasizing the array is no longer used
    existing_shm.close()
        
    return 1
            
            
            
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

            