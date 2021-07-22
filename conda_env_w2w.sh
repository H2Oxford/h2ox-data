conda create -n w2w python=3.8 --yes 
conda activate w2w
conda install -c conda-forge pytorch xarray=0.16 pytorch torchvision cudatoolkit --yes
conda install dask --yes
conda install -c conda-forge rasterio scikit-image --yes
conda install -c conda-forge seaborn=0.11 --yes
conda install -c anaconda networkx --yes
conda install -c conda-forge netcdf4 numba tqdm jupyterlab tensorboard ipython pip ruamel.yaml descartes statsmodels scikit-learn black mypy --yes
