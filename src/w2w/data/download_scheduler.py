import os, time
import cdsapi
import glob
import multiprocessing as mp

import logging
logging.basicConfig(level=logging.INFO)


VARIABLES = {
    't2m':'2m_temperature',
    'u10':'10m_u_component_of_wind',
    'v10':'10m_v_component_of_wind',
    'tp':'total_precipitation',
    'potev':'potential_evaporation',
}


def touch(fname):
    if os.path.exists(fname):
        os.utime(fname, None)
    else:
        open(fname, 'a').close()

def download_worker(f_queue, data_dir):
    
    root = os.path.splitext(os.path.split(f_queue)[1])[0]
    
    year, month, variable = root.split('_')
    
    year = int(year)
    month = int(month)
    
    savepath = os.path.join(data_dir,f'{year}_{month:02d}_{variable}.nc')
    
    c = cdsapi.Client()
    
    c.retrieve(
    'reanalysis-era5-land',
    {
        'format': 'netcdf',
        'variable': VARIABLES[variable],
        'year': str(year),
        'month': f'{month:02d}',
        'day': [
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
        ],
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
    },
    savepath)
    
    # Remove pending file
    os.remove(os.path.join(data_dir,f'{year}_{month:02d}_{variable}.ncp'))
    
    return 1
    
    
    
    
def download_scheduler(n_files, n_workers, data_dir, hopper_dir):
    """ This download scheduler makes sure that there are always data ready to uploaded to zarr"""
    
    logger = logging.getLogger('DOWNLOAD-SCHEDULER')
    
    download_pool = mp.Pool(n_workers)
    
    download_processes = []
    
    while True:
        
        fs = glob.glob(os.path.join(data_dir,'*.nc'))
        
        logger.info(f'found {len(fs)} nc files on deck')
        
        # if there aren't that many data files, then fetch a new one and remove the old one from the queue
        if len(fs)<n_files:
            
            logger.info('Less than files threshold. Add some more?')
            
            # get the files in the queue
            queue = sorted(glob.glob(os.path.join(hopper_dir,'*.ncq')))
            
            # get the files that are pending
            pending = glob.glob(os.path.join(data_dir,'*.ncp'))
            
            logger.info(f'Got {len(queue)} in queue; {len(pending)} in pending')
            
            if len(queue)>0 and len(pending)<n_workers:
                logger.info(f'Adding file to download queue: {queue[0]}')
            
                # get the first file in the queue and send it to async download
                download_pool.apply_async(download_worker, (queue[0], data_dir))
            
                # remove it from the queue...
                os.remove(queue[0])
            
                # and put it as pending the data
                root = os.path.splitext(os.path.split(queue[0])[1])[0]
            
                touch(os.path.join(data_dir,root+'.ncp'))
                
            else:
                logger.info(f'No queue files or too many pending!')
            
        
        # check back every 30s
        logger.info('Checking back in 30s')
        time.sleep(30)
        
        
    
if __name__=="__main__":
    
    download_scheduler(
        n_files=3,
        n_workers=3,
        data_dir = os.path.join(os.getcwd(),'data'),
        hopper_dir = os.path.join(os.getcwd(),'hopper')
    )
        
    #pool = mp.Pool(3)
    #pool.apply_async(download_worker, (os.path.join(os.getcwd(),'data','2011_05_tp.ncp'), os.path.join(os.getcwd(),'data')))