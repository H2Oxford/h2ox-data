from typing import List, Optional
import os
import json
import time
import certifi

import cdsapi

VARIABLES = {
    't2m':'2m_temperature',
    'u10':'10m_u_component_of_wind',
    'v10':'10m_v_component_of_wind',
    'tp':'total_precipitation',
    'potev':'potential_evaporation',
}

def era5_enqueuer(
    year:int, 
    month:int, 
    variable:str, 
    days:Optional[List[int]]=None
):
    
    assert variable in VARIABLES.keys(), f'<variable> must be one of {VARIABLES.keys()}'
    
    if days is not None:
        days = [f'{d_int:02d}' for d_int in days]
    else:
        days = [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ]
    
    year = str(year)
    month = int(month)
    
    client = cdsapi.Client(wait_until_complete=False)
    
    result_object = client.retrieve(
        'reanalysis-era5-land',
        {
            'format': 'netcdf',
            'variable': VARIABLES[variable],
            'year': str(year),
            'month': f'{month:02d}',
            'day': days,
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
        }
    )
    
    time.sleep(10)
    
    return result_object.reply
    
def era5_checker(reply: dict):
    """update our reply dict"""
    
    client = cdsapi.Client(wait_until_complete=False)
    
    task_url = "%s/tasks/%s" % (client.url, reply['request_id'])
    
    refresh = client.robust(client.session.get)(task_url, verify=True)
    
    reply = json.loads(refresh.text)
    
    return reply
    

def era5_downloader(
    reply: dict,
    savepath: str
):
    
    client = cdsapi.Client(wait_until_complete=False, quiet=True)
    
    task_url = "%s/tasks/%s" % (client.url, reply['request_id'])
    
    refresh = client.robust(client.session.get)(task_url, verify=True)
    
    reply = json.loads(refresh.text)
    
    result_ob = cdsapi.api.Result(client, reply) # building this guy break the current request
    
    result_ob.download(savepath)
    
    return savepath