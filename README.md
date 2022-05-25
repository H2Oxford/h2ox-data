[<img alt="Wave2Web Hack" width="1000px" src="https://github.com/H2Oxford/.github/raw/main/profile/img/wave2web-banner.png" />](https://www.wricitiesindia.org/content/wave2web-hack)

H2Ox is a team of Oxford University PhD students and researchers who won first prize in the[Wave2Web Hackathon](https://www.wricitiesindia.org/content/wave2web-hack), September 2021, organised by the World Resources Institute and sponsored by Microsoft and Blackrock. In the Wave2Web hackathon, teams competed to predict reservoir levels in four reservoirs in the Kaveri basin West of Bangaluru: Kabini, Krishnaraja Sagar, Harangi, and Hemavathy. H2Ox used sequence-to-sequence models with meterological and forecast forcing data to predict reservoir levels up to 90 days in the future.

The H2Ox dashboard can be found at [https://h2ox.org](https://h2ox.org). The data API can be accessed at [https://api.h2ox.org](https://api.h2ox.org/docs#/). All code and repos can be [https://github.com/H2Oxford](https://github.com/H2Oxford). Our Prototype Submission Slides are [here](https://docs.google.com/presentation/d/1J_lmFu8TTejnipl-l8bXUZdKioVseRB4tTzqK6sEokI/edit?usp=sharing). The H2Ox team is [Lucas Kruitwagen](https://github.com/Lkruitwagen), [Chris Arderne](https://github.com/carderne), [Tommy Lees](https://github.com/tommylees112), and [Lisa Thalheimer](https://github.com/geoliz).

# H2Ox - Data

This repo is for a dockerised service to ingest [ECMWF](https://www.ecmwf.int/) [ERA5-Land](https://www.ecmwf.int/en/era5-land) data into a [Zarr archive](https://zarr.readthedocs.io/en/stable/). The Zarr data is rechunked in the time domain in blocks of four years. This ensures efficient access to moderately-sized chunks of data, facilitating timeseries research. Two variables are ingested: two-meter temperature (t2m) and total precipitation (tp).

## Installation - development
    
For development, the repo can be pip installed with the `-e` flag and `[pre-commit]` options:

    git clone https://github.com/H2Oxford/h2ox-chirps.git
    cd h2ox-chirps
    pip install -e .[pre-commit]
    
## Useage
    
For containerised deployment, a docker container can be built from this repo.
This repo supports the creation of three different dockerised services:

- an `enqueuer` queues up data requests from the [Copernicus Data Store (CDS)](https://cds.climate.copernicus.eu)
- the `downloader` periodically pings the CDS to determine if the data is ready for download. When it is ready, it downloads the data and stores it in cloud storage.
- the `ingestor` ingests the downloaded data into a zarr archive, rechunking it in the time dimension.

The apps use three sequential cloud storage buckets to store download status json tokens, and then raw .nc files of large chunks of continuguous ECMWF data.
Then the three apps behave as follows: `enqueuer` stores a queue token in `CLOUD_STAGING_QUEUE` and periodically checks the CDS API is the data is ready for download.
When data is ready to download, `enqueuer` places a queue token in `CLOUD_STAGING_SCHEDULE`, indicating the data is ready for download, and returns a 'success' message.
`ecwmf_downloader` then downloads the data and stores it in the `CLOUD_STAGING_RAW` directory.

This repo also allows the user to specify a `PROVIDER` environment variable, making the docker container flexible to different cloud service ecosystems.
The code at [h2ox/provider](src/h2ox/provider) allows utilities specific to different cloud services providers to be imported in a flexible way.
Google Cloud Platform (GCP) is provided as a full implementation, but other cloud service providers could be added.

### Credentials

The Copernicus Data Store serves era5land data using the [CDS API](https://github.com/ecmwf/cdsapi) library. 
To protect the CDS API and to schedule repeated updates, this app schedules requests to the CDS queue.
Users of this app will need to request credentialed access to the CDS API. 
Then, to use this app and the CDS API, the user needs specify the URL and [API-key](https://cds.climate.copernicus.eu/api-how-to) in: `~/.cdsapirc`. 

A slackbot messenger is also implemented to post updates to a slack workspace.
Follow [these](https://api.slack.com/bot-users) instuctions to set up a slackbot user, and then set the `SLACKBOT_TOKEN` and `SLACKBOT_TARGET` environment variables.


### Environment Variables

The three different services require environment variables to target the various cloud and ECMWF resources.

    PROVIDER=<GCP|e.g. AWS>                                        # a string to tell the app which utilities to use in src/h2ox/provider
    CLOUD_STAGING_QUEUE=<gs://path/to/queue/tokens/>               # path to the tokens for enqueued data request
    CLOUD_STAGING_SCHEDULE=<gs://path/to/download/staging/tokens/> # path to the tokens for data which had been stages
    CLOUD_STAGING_RAW=<gs://path/to/raw/ncdata/files/>             # path to the raw staged .nc files
    SLACKBOT_TOKEN=<my-slackbot-token>                             # a token for a slack-bot messenger
    SLACKBOT_TARGET=<my-slackbot-target>                           # target channel to issue ingestion updates
    CDSAPI_URL=<url-included-in-cds-credentials>                   # the url used to access the CDS api
    CDSAPI_KEY=<key-included-in-cds-credentials>                   # the key to access the CDS api
    TARGET=<gs://my/era5/zarr/archive>                             # the cloud path for the zarr archive
    ZERO_DT=<YYYY-mm-dd>                                           # the initial date offset of the zarr archive
    N_WORKERS=<int>                                                # the number of workers the cloud machine should use for data ingestion.

### Docker

To set each app, the Docker service needs to be given the `MAIN` argument, which is the filepath to the app root directory, e.g. for the enqueuer:

    docker build -t <my-tag> --build-arg', MAIN=apps/enqueuer .

Cloudbuild container registery services can also be targeted at forks of this repository. The cloudbuild service will need to provide the `MAIN` build argument.

To run the docker container, the environment variables can be passed as a `.env` file:

    docker run --env-file=.env -t <my-tag>


### Accessing ingested data

[xarray](https://docs.xarray.dev/en/stable/) can be used with a zarr backend to lazily access very large zarr archives.

<img alt="Zarr Xarray" width="1000px" src="https://github.com/H2Oxford/.github/raw/main/profile/img/zarr_chirps.png"/>


## Citation

CHIRPS can be cited as:

    Funk, C.C., Peterson, P.J., Landsfeld, M.F., Pedreros, D.H., Verdin, J.P., Rowland, J.D., Romero, B.E., Husak, G.J., Michaelsen, J.C., and Verdin, A.P., 2014, A quasi-global precipitation time series for drought monitoring: U.S. Geological Survey Data Series 832, 4 p. http://pubs.usgs.gov/ds/832/
    
Our Wave2Web submission can be cited as: 
 
    <citation here>