

FORECAST_VARIABLES = {
    't2m':'2m_temperature',
    'tp':'total_precipitation',
}            
            
            
def tigge_downloader(
    year:int, 
    month:int, 
    days:Optional[List[int]]=None
):
    
    if days is None:
        
        start_day = 1
    
        if month==2:
            if year in [2008,2012,2016, 2020]:
                last_day = 29
            else:
                last_day = 28
        elif month in [1,3,5,7,8,10,12]:
            last_day = 31
        else:
            last_day=30
            
    else:
        start_day = days[0]
        last_day = days[-1]
    
    savepath = os.path.join(os.getcwd(),'data','_'.join(['tigge',str(year),f'{month:02d}'])+'.grib')
    
    server = ECMWFDataServer()

    server.retrieve({
        "class": "ti",
        "dataset": "tigge",
        "date": f"{year}-{month:02d}-{start_day:02d}/to/{year}-{month:02d}-{last_day:02d}",
        "expver": "prod",
        "grid": "0.5/0.5",
        "levtype": "sfc",
        "origin": "ecmf",
        "param": "167/228228",
        "step": "0/6/12/18/24/30/36/42/48/54/60/66/72/78/84/90/96/102/108/114/120/126/132/138/144/150/156/162/168/174/180/186/192/198/204/210/216/222/228/234/240/246/252/258/264/270/276/282/288/294/300/306/312/318/324/330/336/342/348/354/360",
        "time": "00:00:00",
        "type": "cf",
        "target": savepath,
    })
    
    return savepath