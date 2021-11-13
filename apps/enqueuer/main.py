
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
from h2ox.data.downloaders import era5_enqueuer, era5_checker
from h2ox.provider import upload_blob, cloud_file_exists, download_cloud_json
from loguru import logger

"""enqueuer"""


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
def queue_ecmwf():
    
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

    for kk in ['archive','year','month','days','variable']:
        if not kk in payload.keys():
            msg = f"{kk} not in payload keys: {payload.keys()}"
            print (f"error: {msg}")
            return f"Bad Request: {msg}", 400

    if not isinstance(payload, dict):
        json_data = base64.b64decode(payload).decode("utf-8")
        payload = json.loads(json_data)

    # parse request
    archive = payload["archive"]
    year = int(payload["year"])
    month = int(payload["month"])
    days = payload["days"]
    variable = payload["variable"]
    
    slackmessenger = SlackMessenger(
        token=os.environ.get('SLACKBOT_TOKEN'),
        target = os.environ.get('SLACKBOT_TARGET'),
        name='era5-downloader',
    )

    # download data
    logger.info(f'Received request for {archive} {year} {month} {variable}')

    if archive=='era5land':
        
        fname_root = '_'.join(['era5land',str(year),f'{month:02d}',variable])
        
        full_path = os.path.join(os.environ['CLOUD_STAGING_QUEUE'],f'{fname_root}.token')
        
        if not cloud_file_exists(full_path):
            # trigger task for first time
            reply = era5_enqueuer(year, month, variable,days)
            
            # store reply json
            local_path = os.path.join(os.getcwd(),f'{fname_root}.token')
            json.dump(reply, open(local_path,'w'))
            blob_dest = os.path.join(os.environ['CLOUD_STAGING_QUEUE'],f'{fname_root}.token')
            upload_blob(local_path,blob_dest)
            
            # then fail
            
            slackmessenger.message(f'Queued {fname_root}')
            
            msg = f"Enqueued {fname_root}"
            logger.info(msg)
            return f"Failing to backoff CDS queue", 400
        
        else: 
            # cloud exists -> download to get _id
            
            queue_blob_dest = os.path.join(os.environ['CLOUD_STAGING_QUEUE'],f'{fname_root}.token')
            
            reply = download_cloud_json(
                bucket_name=queue_blob_dest.split('/')[0], 
                filename='/'.join(queue_blob_dest.split('/')[1:]),
            )
            
            logger.info(f'pre-check: {json.dumps(reply)}')

            reply = era5_checker(reply)
            
            logger.info(f'post-check: {json.dumps(reply)}')
            
            if 'state' not in reply.keys():
                
                logger.info('No state retrieved from CDS')
                # requeue and fail open
                
                return f"No state returned from CDS :(", 400
            
            elif reply['state']=='completed':
                # download ready. push ready token and success
                local_path = os.path.join(os.getcwd(),f'{fname_root}.token')
                json.dump(reply, open(local_path,'w'))
                ready_blob_dest = os.path.join(os.environ['CLOUD_STAGING_SCHEDULE'],f'{fname_root}.token')
                upload_blob(local_path,ready_blob_dest)
                
                # success
                slackmessenger.message(f'{fname_root} completed!')
                
                msg = f"{fname_root} ready!"
                logger.info(msc)
                return f"{fname_root} ready for download!", 200
                
            else:
                #download not ready yet.
                slackmessenger.message(f'{fname_root} not ready for download yet')
                
                msg = f"{fname_root} still queued."
                logger.info(msg)
                return f"failing to backoff ", 400

    else:
        raise NotImplementedError