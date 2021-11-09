from loguru import logger
from typing import Tuple
import os

import zarr
import pandas as pd
import numpy as np
import dask.array
import xarray as xr

from h2ox.provider import mapper, prefix
from h2ox.data import ERA5_VARIABLES, FORECAST_VARIABLES


def build_forecast_archive(
    start_date: str,
    end_date: str,
    chunks: Tuple[int,int,int,int],
    storage_root: str,
) -> int:
    
    dt_range = pd.date_range(start_date, end=end_date, freq='1d')
    steps = pd.date_range('2010-01-01', periods=61,freq='6H') - pd.Timestamp('2010-01-01')
    
    # first, build the coordinates
    coords = {
        'longitude': np.arange(0,360,0.5),
        'latitude': np.arange(90,-90,-0.5),
        'time': dt_range,
        'step': steps,
    }
    
    # next, build the variables
    # use a dask array hold the dummy dimensions
    dummies = dask.array.zeros((720, 360, dt_range.shape[0], steps.shape[0]), chunks=chunks, dtype=np.float32)  # lon, lat, day, steps
    
    # mock-up the dataset
    ds = xr.Dataset(
        {kk: (tuple(coords.keys()), dummies) for kk in FORECAST_VARIABLES.keys()},
        coords = coords
    )
    
    # Now we write the metadata without computing any array values
    ds.to_zarr(mapper(prefix+storage_root), compute=False, consolidated=True)
    
    return 1
    
    
def build_era5_archive(
    start_date: str,
    end_date: str,
    chunks: Tuple[int,int,int],
    storage_root: str,
) -> int:
    
    dt_range = pd.date_range(start_date, end=end_date,freq='1H')
    
    # first, build the coordinates
    coords = {
        'latitude': np.arange(90,-90,-0.1),
        'longitude': np.arange(0,360,0.1),
        'time': dt_range
    }
    
    # next, build the variables
    # use a dask array hold the dummy dimensions
    dummies = dask.array.zeros((1800,3600,dt_range.shape[0]), chunks=chunks, dtype=np.float32)
    
    # mock-up the dataset
    ds = xr.Dataset(
        {kk: (tuple(coords.keys()), dummies) for kk in ERA5_VARIABLES.keys()},
        coords = coords
    )
    
    # Now we write the metadata without computing any array values
    ds.to_zarr(mapper(prefix+storage_root), compute=False, consolidated=True)
    
    return 1

if __name__=="__main__":
    
    logger.info('building forecast archive')
    
    """
    build_forecast_archive(
        start_date = '2010-01-01 00:00:00',
        end_date = '2024-12-31 23:00:00',
        chunks = (10, 10, 1461, 61), # lon, lat, day, steps
        storage_root = os.environ['FORECAST_ARCHIVE_ROOT']
    )
    """
    
    logger.info('building era5 archive')
    
    build_era5_archive(
        start_date = '1981-01-01 00:00:00',
        end_date = '2024-12-31 23:00:00',
        chunks = (10, 10, 1461*24), # lat, lon, day * 24 hrs
        storage_root = os.environ['ERA5_ARCHIVE_ROOT']
    )
    
    logger.info('Done!')