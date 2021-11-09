
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
from h2ox.data.downloaders import era5_downloader, tigge_downloader
from h2ox.provider import upload_blob
from loguru import logger


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
    
    
    

    try:
        tic = time.time()

        envelope = request.get_json()
        if not envelope:
            msg = "no message received"
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
        
        # parse request
        archive = request_json["archive"]
        year = int(request_json["year"])
        month = int(request_json["month"])
        days = request_json["days"]
        variable = request_json["variable"]

        # download data
        logger.info(f'Requesting download for {archive} {year} {month} {variable}')
        
        if archive=='era5land':
            savepath = era5_downloader(year, month, variable, days)
        elif archive=='tigge':
            savepath = tigge_downloader(year, month, days)
        else:
            raise NotImplementedError
            
        # upload data
        blob_dest = os.path.join(os.environ['CLOUD_STAGING'],os.path.split(savepath)[-1])
        
        logger.info(f'Uploading to staging {blob_dest}')
        upload_blob(savepath,blog_dest)
        
        # remove local data
        os.remove(savepath)

        logger.info(f'Removed {savepath}')

        return f"Staged {blobdest}", 200

    except Exception as e:

        trace = format_stacktrace()

        if "MONITOR_TABLE" in os.environ:
            monitor.post_status(msg_type="FAILED", msg_payload=trace)

        raise e