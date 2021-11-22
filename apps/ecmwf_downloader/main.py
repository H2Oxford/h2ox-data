
import base64
import datetime
import io
import json
import logging
import os
import sys
import time
import traceback

from flask import Flask
from flask import request
from h2ox.data.slackbot import SlackMessenger
from h2ox.data.downloaders import era5_downloader
from h2ox.provider import upload_blob, download_cloud_json, download_blob
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
def download_ecmwf():
    
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

    
    slackmessenger = SlackMessenger(
        token=os.environ.get('SLACKBOT_TOKEN'),
        target = os.environ.get('SLACKBOT_TARGET'),
        name='era5-downloader',
    )
    
    

    # download data
    logger.info(f'downloading data: {bucket_id}, {object_id}')
    slackmessenger.message(f'downloading {bucket_id}/{object_id}')
    
    archive = object_id.split('/')[0]

    if archive=='era5land':
        
        # get the nc file
        download_blob(bucket_id, object_id)  

        
        # download the file
        fname_root = os.path.splitext(os.path.split(object_id)[-1])[0]
        savepath = os.path.join(os.getcwd(),fname_root+'.nc')
        savepath = era5_downloader(reply, savepath)
        
        # move it to the bucket
        slackmessenger.message(f'moving {fname_root} to bucket')
        blob_dest = os.path.join(os.environ['CLOUD_STAGING_RAW'],os.path.split(savepath)[-1])
        logger.info(f'moving {savepath} to {blob_dest} (size: {os.path.getsize(savepath)/1000/1000}mb)')
        
        upload_blob(savepath,blob_dest)
        
    else:
        raise NotImplementedError
        
    logger.info('debug size issue:')
    _name=''
    for _name, obj in locals().items():
        logger.info(f'name: {_name}, size:{sys.getsizeof(obj)}')
        
    



    return f"Staged {blob_dest}", 200