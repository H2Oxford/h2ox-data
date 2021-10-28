import logging

logging.basicConfig(level=logging.INFO)

import netCDF4 as nc
import gcsfs
import numpy as np
import zarr
import os
from datetime import datetime as dt
import xarray as xr

from multiprocessing import shared_memory

from ecmwfapi import ECMWFDataServer


def sharedmem_worker(shm_spec, variable, gcs_path, slices, time_offset, worker_idx):

    logger = logging.getLogger(f"worker_{worker_idx}")

    # open the dataset from sharedmemory
    existing_shm = shared_memory.SharedMemory(name=shm_spec["name"])
    data = np.ndarray(
        shm_spec["shape"], dtype=shm_spec["dtype"], buffer=existing_shm.buf
    )

    # open the zarr
    store = gcsfs.GCSMap(root=gcs_path)
    z = zarr.open(store)

    # write each slice
    for ii_s, s in enumerate(slices):
        if ii_s % 100 == 0:
            logger.info(f"ii_s:{ii_s}")

        time_slice = s[1]
        offset_slice = slice(s[1].start + time_offset, s[1].stop + time_offset)
        step_slice = s[2]
        lat_slice = s[3]
        lon_slice = s[4]
        z[variable][lon_slice, lat_slice, offset_slice, step_slice] = data[
            lon_slice, lat_slice, time_slice, step_slice
        ]

    # del data  # Unnecessary; merely emphasizing the array is no longer used
    existing_shm.close()

    return 1


def ecmwf_caller(fname):

    year, month = os.path.splitext(os.path.split(fname)[1])[0].split("_")

    if int(month) == 2:
        if year in [2008, 2012, 2016, 2020]:
            last_day = 29
        else:
            last_day = 28
    elif int(month) in [1, 3, 5, 7, 8, 10, 12]:
        last_day = 31
    else:
        last_day = 30

    print(f"getting {fname}: {year} {month}")

    server = ECMWFDataServer()

    server.retrieve(
        {
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
        }
    )

    return 1


def write_worker_mp(
    filename: str, variable, gcs_path, slices, time_offset, worker_idx
) -> int:

    logger = logging.getLogger(f"worker_{worker_idx}")

    # open the dataset and the zarr
    ds_worker = nc.Dataset(filename)
    store = gcsfs.GCSMap(root=gcs_path)
    z = zarr.open(store)

    # write each slice
    for ii_s, subslice in enumerate(slices):
        if ii_s % 100 == 0:
            logger.info(f"ii_s:{ii_s}")
        time_slice = subslice[1]
        offset_slice = slice(
            subslice[1].start + time_offset, subslice[1].stop + time_offset
        )
        lat_slice = subslice[2]
        lon_slice = subslice[3]
        z[variable][lon_slice, lat_slice, offset_slice] = np.transpose(
            np.squeeze(ds_worker[variable][time_slice, lat_slice, lon_slice]), [2, 1, 0]
        )

    return 1


def write_worker_ecmwf(filename, gcs_path, slices, worker_idx):

    logger = logging.getLogger(f"worker_{worker_idx}")

    year, month = os.path.splitext(os.path.split(filename)[1])[0].split("_")

    time_offset = (
        dt(year=int(year), month=int(month), day=1) - dt(year=2010, month=1, day=1)
    ).days

    ds_worker = xr.open_dataset(filename, engine="cfgrib")

    store = gcsfs.GCSMap(root=gcs_path)
    z = zarr.open(store)

    variables = {
        0: "t2m",
        1: "tp",
    }

    for ii_s, s in enumerate(slices):

        variable = variables[s[0].start]

        if ii_s % 10 == 0:
            logger.info(f"ii_s:{ii_s}")

        time_slice = s[1]
        offset_slice = slice(s[1].start + time_offset, s[1].stop + time_offset)
        step_slice = s[2]
        lat_slice = s[3]
        lon_slice = s[4]
        z[variable][lon_slice, lat_slice, offset_slice, step_slice] = np.transpose(
            np.squeeze(
                ds_worker[variable][time_slice, step_slice, lat_slice, lon_slice]
            ),
            [3, 2, 0, 1],
        )
