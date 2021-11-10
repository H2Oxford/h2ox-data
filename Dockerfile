# Use the official osgeo/gdal image.
FROM osgeo/gdal:ubuntu-small-latest

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Set default env vars to clear CI
ENV PROVIDER GCP
ENV MAIN apps/ecmwf_downloader

ENV APP_HOME /app

COPY $MAIN $APP_HOME

WORKDIR $APP_HOME

# Copy local code to the container image.
# __context__ to __workdir__ 
COPY . ./h2ox-data
COPY ./src/h2ox/provider/requirements/$PROVIDER/requirements.txt ./
# Install GDAL dependencies
RUN apt-get update
RUN apt-get install -y python3-pip
# Install production dependencies.
RUN pip install --no-cache-dir ./h2ox-data
RUN pip install --no-cache-dir -r requirements.txt

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app