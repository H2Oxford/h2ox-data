from typing import Tuple
import glob, os
from datetime import datetime as dt
import multiprocessing as mp
import logging

logging.basicConfig(level=logging.INFO)

import zarr
import xarray as xr
import gcsfs
import pandas as pd
import dask.array
import numpy as np

from workers import write_worker_mp

VARIABLES = {
    "t2m": "2m_temperature",
    "u10": "10m_u_component_of_wind",
    "v10": "10m_v_component_of_wind",
    "tp": "total_precipitation",
    "potev": "potential_evaporation",
}

FORECAST_VARIABLES = {
    "t2m": "2m_temperature",
    "tp": "total_precipitation",
}


def build_forecast_archive(
    start_date: str, end_date: str, chunks: Tuple[int, int, int, int], gcs_root: str,
) -> int:

    dt_range = pd.date_range("2010-01-01", end="2024-12-31", freq="1d")
    steps = pd.date_range("2010-01-01", periods=61, freq="6H") - pd.Timestamp(
        "2010-01-01"
    )

    # first, build the coordinates
    coords = {
        "longitude": np.arange(0, 360, 0.5),
        "latitude": np.arange(90, -90, -0.5),
        "time": dt_range,
        "step": steps,
    }

    # next, build the variables
    # use a dask array hold the dummy dimensions
    dummies = dask.array.zeros(
        (720, 360, dt_range.shape[0], steps.shape[0]), chunks=chunks
    )  # lon, lat, day, steps

    # mock-up the dataset
    ds = xr.Dataset(
        {kk: (tuple(coords.keys()), dummies) for kk in FORECAST_VARIABLES.keys()},
        coords=coords,
    )

    # set up the Google Cloud Storage store
    store = gcsfs.GCSMap(root=gcs_root)

    # Now we write the metadata without computing any array values
    ds.to_zarr(store, compute=False, consolidated=True)

    return 1


def build_zarr_archive(
    start_date: str, end_date: str, chunks: Tuple[int, int, int], gcs_root: str,
) -> int:

    dt_range = pd.date_range(start_date, end=end_date, freq="1H")

    # first, build the coordinates
    coords = {
        "longitude": np.arange(0, 360, 0.1),
        "latitude": np.arange(90, -90, -0.1),
        "time": dt_range,
    }

    # next, build the variables
    # use a dask array hold the dummy dimensions
    dummies = dask.array.zeros((3600, 1800, dt_range.shape[0]), chunks=chunks)

    # mock-up the dataset
    ds = xr.Dataset(
        {kk: (tuple(coords.keys()), dummies) for kk in VARIABLES.keys()}, coords=coords
    )

    # set up the Google Cloud Storage store
    store = gcsfs.GCSMap(root=gcs_root)

    # Now we write the metadata without computing any array values
    ds.to_zarr(store, compute=False, consolidated=True)

    return 1


def push_month(gcs_root, f, n_workers):

    year, month, var = os.path.splitext(os.path.split(f)[1])[0].split("_")

    logger = logging.getLogger(f"PUSH-{year}-{month}-{var}")

    # get the ds object
    ds = xr.open_dataset(f, chunks={"longitude": 10, "latitude": 10, "time": 35064})

    # get slices to write
    slices = dask.array.core.slices_from_chunks(
        dask.array.empty_like(ds.to_array()).chunks
    )  # null, time, lat, lon

    # eliminate the slices which hit the boundary
    slices = [s for s in slices if s[2].stop != 1801]
    logger.info(f"Pushing {len(slices)}")

    # prep for multiprocessing
    chunk_worker = len(slices) // n_workers + 1
    slices_rechunked = [
        slices[chunk_worker * ii : chunk_worker * (ii + 1)] for ii in range(n_workers)
    ]

    # prep the pool for writing
    pool = mp.Pool(n_workers)

    time_offset = int(
        (
            dt.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            - dt.strptime("1981-01-01", "%Y-%m-%d")
        ).total_seconds()
        / 3600
    )

    args = [
        (f, var, gcs_root, slices_rechunked[ii], time_offset, ii)
        for ii in range(n_workers)
    ]

    results = pool.starmap(write_worker_mp, args)

    return np.prod(results)


def loop(fs, gcs_root, n_workers):

    for f in fs:
        print(f)
        push_month(gcs_root, f, n_workers)


def scheduler():
    # a scheduler. Let's do a full year-variable at a time, and then
    pass


if __name__ == "__main__":
    gcs_root = "oxeo-forecasts/ecmwf-tigge-15day"

    build_forecast_archive(
        start_date="2010-01-01 00:00:00",
        end_date="2024-12-31 23:00:00",
        chunks=(10, 10, 1461, 61),  # lon, lat, day, steps
        gcs_root=gcs_root,
    )
