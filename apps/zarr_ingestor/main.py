
import base64
import datetime
import pickle
import io
import json
import logging
import os
import sys
import time
import traceback
import multiprocessing as mp
import zarr

from flask import Flask
from flask import request
from h2ox.data.slackbot import SlackMessenger
from h2ox.data.uploaders import era5_ingest_local_worker
from h2ox.provider import upload_blob, download_cloud_json, download_blob_to_filename
from h2ox.provider import mapper, prefix
from loguru import logger

"""downloader - gets json from scheduler and downloads"""

app = Flask(__name__)


if __name__ != "__main__":
    # Redirect Flask logs to Gunicorn logs
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.info("Service started...")
else:
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


def format_stacktrace():
    parts = ["Traceback (most recent call last):\n"]
    parts.extend(traceback.format_stack(limit=25)[:-2])
    parts.extend(traceback.format_exception(*sys.exc_info())[1:])
    return "".join(parts)


@app.route("/", methods=["POST"])
def zarr_ingestor():
    
    """Receive a request and queue downloading ecmwf data
    
    Request params:
    ---------------
    
        archive: Union[]
        year: int
        month: 
        days:
        variable:
        
    
    # download data
    # upload to bucket
    # delete local
    
    """
    """ if pubsub:
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    request_json = envelope["message"]["data"]

    if not isinstance(request_json, dict):
        json_data = base64.b64decode(request_json).decode("utf-8")
        request_json = json.loads(json_data)

    logger.info('request_json: '+json.dumps(request_json))
    
    # parse request
    bucket_id = request_json['bucket']
    object_id = request_json['name']
    """
    
    payload = request.get_json()
    
    if not payload:
        msg = "no message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400


    logger.info('payload: '+json.dumps(payload))

    if not isinstance(payload, dict):
        msg = "invalid task format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400
    
    bucket_id = payload['bucket']
    object_id = payload['name']
    slice_start_idx = payload['slice_start_idx']
    slice_end_idx = payload['slice_end_idx']
    
    
    TARGET = os.environ['TARGET']
    ZERO_DT = datetime.datetime.strptime(os.environ['ZERO_DT'],'%Y-%m-%dT%H:%M:%S')
    N_WORKERS = int(os.environ['N_WORKERS'])

    
    slackmessenger = SlackMessenger(
        token=os.environ.get('SLACKBOT_TOKEN'),
        target = os.environ.get('SLACKBOT_TARGET'),
        name='era5-downloader',
    )
    
    # download data
    logger.info(f'downloading data: {bucket_id}, {object_id}')
    slackmessenger.message(f'Ingesting {bucket_id}/{object_id} to {TARGET} with {N_WORKERS} workers')
    
    archive = object_id.split('/')[0]

    if archive=='era5land':
        # download the nc archive
        
        local_path = os.path.join(os.getcwd(),object_id.split('/')[-1])
        download_blob_to_filename(f'{bucket_id}/{object_id}', local_path)
        
        # download the slices
        slices_path = os.path.join(os.getcwd(),'slices.pkl')
        download_blob_to_filename(f'{bucket_id}/{archive}/slices.pkl',slices_path)
        
        slices = pickle.load(open(slices_path,'rb'))
        
        slices_chunk = slices[slice_start_idx:slice_end_idx]
        
        z_dst = zarr.open(mapper(prefix+TARGET))
        
        era5_ingest_local_worker(
            local_path, 
            z_dst, 
            slices_chunked[ii], 
            ZERO_DT,
            ii,
        )
        
        
        
        """multiprocessing
        # dispatch the workers in parallel
        chunk_size = len(slices)//N_WORKERS + 1
        slices_chunked = [slices[ii*chunk_size:(ii+1)*chunk_size] for ii in range(N_WORKERS)]
        
        z_dst = zarr.open(mapper(prefix+TARGET))
        
        #local_path, z_dst, slices, zero_dt
        args = [
            (
                local_path, 
                z_dst, 
                slices_chunked[ii], 
                ZERO_DT,
                ii
            ) for ii in range(N_WORKERS)
        ]
        
        pool = mp.Pool(N_WORKERS)

        results = pool.starmap(era5_ingest_local_worker, args)
        """
        
        logger.info(f'ingested data: {bucket_id}, {object_id}')
        slackmessenger.message(f'Done ingesting {bucket_id}/{object_id} to {TARGET}')
        
    else:
        raise NotImplementedError
        
    
    return "Ingestion done!", 200