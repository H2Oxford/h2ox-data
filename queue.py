""" run this from the cli to add stubs to the queue"""
import os


def touch(fname):
    if os.path.exists(fname):
        os.utime(fname, None)
    else:
        open(fname, 'a').close()

def enqueue_year_variable(year, variable, hopper_dir):
    """ generate a years' worth fo stubs and write it to the hopper"""
    
    enqueue_fnames = [f'{year}_{mm:02d}_{variable}.ncq' for mm in range(12)]
    
    for f in enqueue_fnames:
        touch(os.path.join(hopper_dir,f))
        
    