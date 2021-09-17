from typing import List, Tuple, Optional, Dict, Any
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path


def load_data_into_xarray_format(data_dir: Path, search_str: str = ".csv") -> xr.Dataset:
    csvs = list(data_dir.glob(f"*{search_str}*"))

    # pandas DataFrames
    dfs = [pd.read_csv(c, parse_dates=[0]) for c in csvs]
    reservoirs = [c.name.split("-")[0] for c in csvs]

    #  remove duplicated
    dfs = [df.loc[~df["date"].duplicated()].set_index("date") for df in dfs]

    #  xarray Datasets
    datasets = [
        df.to_xarray().sortby("date").expand_dims(reservoir=[reservoirs[ix]])
        for ix, df in enumerate(dfs)
    ]

    #  join into one dataset
    all_ds = xr.concat(datasets, dim="reservoir")
    all_ds.to_netcdf(data_dir / "w2w_fcast/reservoir_forecast.nc")
    all_ds = all_ds.transpose("time", ...)

    return all_ds


def check_duplicated_times(df: pd.DataFrame, time_str="date"):
    duplicated_dates = df[time_str][df[time_str].duplicated()]
    return df.loc[np.isin(df[time_str], duplicated_dates)]
    

def normalise_ds(
    ds: xr.Dataset, mean: Optional[xr.Dataset] = None, std: Optional[xr.Dataset] = None
) -> Tuple[xr.Dataset, ...]:
    if mean is None:
        assert std is None, "If train data then CANNOT PASS EITHER std or mean"
        mean = ds.mean()
    if std is None:
        std = ds.std()
    norm_ds = (ds - mean) / std
    return norm_ds, mean, std


def unnormalize_ds(ds: xr.Dataset, mean: float, std: float):
    return (ds * std) + mean


def get_drop_nan_indexes(
    X: np.ndarray, Y: np.ndarray, time_axis: int = 1
) -> np.ndarray:
    collapse_axis = 0 if time_axis == 1 else 1
    X_nan_times = np.any(np.any(np.isnan(X), axis=collapse_axis), axis=-1)
    Y_nan_times = np.any(np.any(np.isnan(Y), axis=collapse_axis), axis=-1)
    nan_times = X_nan_times | Y_nan_times
    return nan_times
