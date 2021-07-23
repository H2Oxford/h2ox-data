""" run this from the cli to add stubs to the queue"""
import os
import logging
logging.basicConfig(level=logging.INFO)


def touch(fname):
    if os.path.exists(fname):
        os.utime(fname, None)
    else:
        open(fname, 'a').close()

def enqueue_year_variable(year, variable, hopper_dir):
    """ generate a years' worth fo stubs and write it to the hopper"""
    
    logger = logging.getLogger('ENQUEUE')
    
    enqueue_fnames = [f'{year}_{mm:02d}_{variable}.ncq' for mm in range(1,13)]
    
    for f in enqueue_fnames:
        
        logger.info(f)
        touch(os.path.join(hopper_dir,f))
        
    
if __name__=="__main__":
    years = ['2018','2019']
    variables = ['tp']
    hopper_dir = os.path.join(os.getcwd(),'hopper')
    
    for year in years:
        for variable in variables:
            enqueue_year_variable(year,variable,hopper_dir)