

FORECAST_VARIABLES = {
    't2m':'2m_temperature',
    'tp':'total_precipitation',
}


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
            
            
            
def ecmwf_caller(fname):
    
    year, month = os.path.splitext(os.path.split(fname)[1])[0].split('_')
    
    if int(month)==2:
        if year in [2008,2012,2016, 2020]:
            last_day = 29
        else:
            last_day = 28
    elif int(month) in [1,3,5,7,8,10,12]:
        last_day = 31
    else:
        last_day=30
    
    print (f'getting {fname}: {year} {month}')
    
    server = ECMWFDataServer()

    server.retrieve({
        "class": "ti",
        "dataset": "tigge",
        "date": f"{year}-{month}-01/to/{year}-{month}-{last_day:02d}",
        "expver": "prod",
        "grid": "0.5/0.5",
        "levtype": "sfc",
        "origin": "ecmf",
        "param": "167/228228",
        "step": "0/6/12/18/24/30/36/42/48/54/60/66/72/78/84/90/96/102/108/114/120/126/132/138/144/150/156/162/168/174/180/186/192/198/204/210/216/222/228/234/240/246/252/258/264/270/276/282/288/294/300/306/312/318/324/330/336/342/348/354/360",
        "time": "00:00:00",
        "type": "cf",
        "target": fname,
    })
    
    return 1