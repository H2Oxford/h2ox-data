from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from google.protobuf import duration_pb2
import datetime
import json
import os
import yaml

TASK_CONFIG =yaml.load(open(os.path.join(os.getcwd(),'gcp_task_conf.yaml'),'r'),Loader=yaml.SafeLoader)

def create_task(cfg, client, payload):
    
    # Construct the request body.
    task = {
        "http_request": {  # Specify the type of request.
            "http_method": tasks_v2.HttpMethod.POST,
            "url": cfg['url'],  # The full url path that the task will be sent to.
            'oidc_token': {
               'service_account_email': cfg['service_account']
            },
        }
        
    }
    

    task_name = '_'.join([str(vv) for kk,vv in payload.items()])
    
    if isinstance(payload, dict):
        # Convert dict to JSON string
        payload = json.dumps(payload)
        # specify http content-type to application/json
        task["http_request"]["headers"] = {"Content-type": "application/json"}

    # The API expects a payload of type bytes.
    converted_payload = payload.encode()

    # Add the payload to the request.
    task["http_request"]["body"] = converted_payload

    """ No lead time.
    if in_seconds is not None:
        # Convert "seconds from now" into an rfc3339 datetime string.
        d = datetime.datetime.utcnow() + datetime.timedelta(seconds=in_seconds)

        # Create Timestamp protobuf.
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(d)

        # Add the timestamp to the tasks.
        task["schedule_time"] = timestamp
    """
        
    task["name"] = client.task_path(cfg['project'], cfg['location'], cfg['queue'], task_name)
    
    return task

def create_payload(archive,year,month,days,variable):
    
    if days is not None:
        days=[int(dd) for dd in days]
    
    return dict(
        archive=archive,
        year=int(year),
        month=int(month),
        days=days,
        variable=variable,
    )

def deploy(cfg, archive, year, month, days, variable):
    
    # Create a client.
    client = tasks_v2.CloudTasksClient()
    
    ### create payload
    payload = create_payload(archive, year, month, days, variable)
    
    ### create task
    task = create_task(cfg, client, payload)

    
    # Construct the fully qualified queue name.
    parent = client.queue_path(
        cfg['project'], 
        cfg['location'], 
        cfg['queue'],
    )
    
    # Use the client to build and send the task.
    response = client.create_task(request={"parent": parent, "task": task})

    print("Created task {}".format(response.name))
    
    return 1


if __name__=="__main__":
    
    for ii in range(4,6):
        
        deploy(
            cfg=TASK_CONFIG,
            archive='era5land',
            year=2006,
            month=ii,
            days=None,
            variable='t2m',
        )

