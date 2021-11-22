# H2Ox Wave2Web - Data
A repo for the H2Ox team building tools for the [Wave2Web Hackathon](https://www.wricitiesindia.org/content/wave2web-hack).

## About H2Ox and Wave2Web
H2Ox is a team of Oxford University PhD students and researchers who won first prize in the Wave2Web Hackathon, September 2021. In the Wave2Web hackathon, teams competed to predict reservoir levels in four reservoirs in the Kaveri basin West of Bangaluru: Kabini, Krishnaraja Sagar, Harangi, and Hemavathy. H2Ox used sequence-to-sequence models with meterological and forecast forcing data to predict reservoir levels up to 90 days in the future.

The H2Ox dashboard can be found at [https://h2ox.org](https://h2ox.org). The data API can be found at [https://github.com/Lkruitwagen/wave2web-api](https://github.com/Lkruitwagen/wave2web-api). Our Prototype Submission Slides are [here](https://docs.google.com/presentation/d/1J_lmFu8TTejnipl-l8bXUZdKioVseRB4tTzqK6sEokI/edit?usp=sharing).

H2Ox is comprised of Lucas Kruitwagen. Chris Arderne, Tommy Lees, and Lisa Thalheimer.

## About this Repository

This repository contains the data preparation tools built by the H2Ox team during and after the hackathon.
The data preparation tools in this repository perform the tasks of enqueuing, downloading, and ingesting ECMWF data, explicitly data products era5land reanalysis data and the TIGGE ensemble forecast. 

This repository has been designed to be flexible to the cloud services provider which will host the dockerised instances.
The code at [h2ox/provider](src/h2ox/provider) allows utilities specific to different cloud services providers to be imported in a flexible way.
Google Cloud Platform (GCP) is provided as a full implementation, but other cloud service providers could be added.
An environment variable `PROVIDER` must be specified at run-time.
To use the default GCP implementation, for example:

    export PROVIDER=GCP

This repository contains three dockeriseable apps which can be used to scale the data enqueing, downloading, and ingesting. 
The apps use three sequential cloud storage buckets to store download status json tokens, and then raw .nc files of large chunks of continuguous ECMWF data.
The addresses of the cloud storage buckets needs to be set in environment variables:

    export CLOUD_STAGING_QUEUE=<cloud-storage-directory-for-queue-tokens>
    export CLOUD_STAGING_SCHEDULE=<cloud-storage-directory-for-download-ready-tokens>
    export CLOUD_STAGING_RAW=<cloud-storage-directory-for-downloaded-nc-files>
    
Then the three apps behave as follows: `enqueuer` stores a queue token in `CLOUD_STAGING_QUEUE` and periodically checks the CDS API is the data is ready for download.
When data is ready to download, `enqueuer` places a queue token in `CLOUD_STAGING_SCHEDULE`, indicating the data is ready for download, and returns a 'success' message.
`ecwmf_downloader` then downloads the data and stores it in the `CLOUD_STAGING_RAW` directory.
To set each app, the Docker service needs to be given the `MAIN` argument, which is the filepath to the app root directory, e.g. for the enqueuer:

    docker build -t <my-tag> --build-arg', MAIN=apps/enqueuer .
    
This repository is also prepared with a slackbot messenger which automatically logs messages to a slack workspace.
Follow [these](https://api.slack.com/bot-users) instuctions to set up a slackbot user, and then set the `SLACKBOT_TOKEN` and `SLACKBOT_TARGET` environment variables.

Each app is described in more detail below, along with additional environment variables needed for each app.

### enqueuer

This app, at `apps/enqueuer/main.py`, enqueues data requests with the [Copernicus Data Store](https://cds.climate.copernicus.eu/cdsapp#!/home). 
The Copernicus Data Store serves era5land data using the [CDS API](https://github.com/ecmwf/cdsapi) library. 
To protect the CDS API and to schedule repeated updates, this app schedules requests to the CDS queue.
Users of this app will need to request credentialed access to the CDS API. 
Then, to use this app, the CDS API and 

    CDSAPI_URL = <url-included-in-cds-credentials>
    CDSAPI_KEY = <key-included-in-cds-credentials>


### ecmwf_downloader

This app, at `apps/ecmwf_downloader/main.py`, downloads the data from the Copernicus Data Store once it has finished in the queue and is sitting in the buffer.
This app also requires the `CDSAPI_URL` and `CDSPI_KEY` environment variables.
The large file in this long-running process is downloaded and then uploaded to cloud storage at the `CLOUD_STAGING_RAW` directory.

### zarr_ingestor

The final step in the ingestion process is to break the `netCDF4` file into small pieces in the cloud zarr archive.
This app receives the address of a cloud-stored .nc file, downloads it, removes specific slices of data, and uploads these to a target zarr archive.
The following additional environment variables are required:

    TARGET = <zarr-archive-root>
    ZERO_DT = <datetime-string-of-beginning-of-archive-as-YYYY-mm-ddTHH:MM:SS>
    N_WORKERS = <number-of-workers-available-to-cloud-machine>
    
    
## Build Instructions

### GCP

TBC
