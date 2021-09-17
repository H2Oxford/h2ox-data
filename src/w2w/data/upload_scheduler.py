import os
import time
import glob
from datetime import datetime as dt
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)

import multiprocessing as mp
import xarray as xr
import dask.array


from workers import write_worker_mp


def upload_scheduler(data_dir, gcs_root, n_upload_workers):
    """ This download scheduler makes sure that there are always data ready to uploaded to zarr"""

    logger = logging.getLogger("UPLOAD-SCHEDULER")

    while True:

        nc_files = sorted(glob.glob(os.path.join(data_dir, "*.nc")))

        if len(nc_files) > 0:

            logger.info(f"Running File: {nc_files[0]}")

            # run the file
            nc_to_zarr(gcs_root, nc_files[0], n_upload_workers, logger)

            logger.info(f"Removing file: {nc_files[0]}")
            os.remove(nc_files[0])

        else:
            logger.info("No Files. Sleeping 30s...")
            # sleep 30s otherwise
            time.sleep(30)


def nc_to_zarr(gcs_root, f, n_workers, logger):

    year, month, var = os.path.splitext(os.path.split(f)[1])[0].split("_")

    logger.info(f"PUSH: {year} {month} {var}")

    # get the ds object
    ds = xr.open_dataset(f, chunks={"longitude": 10, "latitude": 10, "time": 35064})

    # get slices to write
    slices = dask.array.core.slices_from_chunks(
        dask.array.empty_like(ds.to_array()).chunks
    )  # null, time, lat, lon

    # eliminate the slices which hit the boundary
    slices = [s for s in slices if s[2].stop != 1801]
    logger.info(f"N_slices: {len(slices)}")

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


if __name__ == "__main__":

    upload_scheduler(
        data_dir=os.path.join(os.getcwd(), "data"),
        gcs_root="oxeo-era5/lk-test-build",
        n_upload_workers=60,
    )
