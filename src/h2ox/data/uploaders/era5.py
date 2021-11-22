import xarray as xr
from datetime import datetime
from loguru import logger

def era5_ingest_local_worker(local_path, z_dst, slices, variable, zero_dt, ii_worker):
    
    ds = xr.open_dataset(local_path)
    
    # from ds get min dt
    start_dt64 = ds.time.min().values
    
    start_dt = datetime.strptime(start_dt64.astype(str)[0:19],'%Y-%m-%dT%H:%M:%S')
    
    start_dt_idx = int((start_dt-zero_dt).total_seconds()/60/60)
    end_dt_idx = start_dt_idx + ds.time.shape[0]
    
    time_slice_dst = slice(start_dt_idx, end_dt_idx, None)
    
    data_variables = [kk for kk,vv in ds.variables.items() if type(vv)==xr.core.variable.Variable]
    
    for variable in data_variables:
        logger.info(f'WORKER:{ii_worker}, ingesting {variable}')
    
        for ii_s,s in enumerate(slices):
            
            if ii_s % 10==0:
                logger.info(f'WORKER:{ii_worker}, slice:{ii_s}/{len(slices)}')

            lat_slice = s[0]
            lon_slice = s[1]

            data = ds[variable].isel(latitude=lat_slice, longitude = lon_slice).load()

            z_dst[variable][lat_slice, lon_slice, time_slice_dst] = data.values.transpose(1,2,0).astype(np.float32)
        
