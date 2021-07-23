import click
import cdsapi

VARIABLES = {
    't2m':'2m_temperature',
    'u10':'10m_u_component_of_wind',
    'v10':'10m_v_component_of_wind',
    'tp':'total_precipitation',
    'potev':'potential_evaporation',
}

@click.group()
def cli():
    pass

@cli.command()
@click.argument('year', type=click.INT)
@click.argument('month', type=click.INT)
@click.argument('variable', type=click.STRING)
@click.option('--savepath', type=click.STRING, default=None, help='path to save to')
def get_yrmonth(year, month, variable, savepath):
    """
    A simple CLI to fetch a single month from ERA-5 Land
    
    parameters
    ----------
    YEAR: INT -- the year to fetch
    MONTH: INT -- the month to fetch
    VARIABLE: STR -- the variable to fetch
    """
    
    assert (year in range(1981,2021)), 'YEAR must be >1981, <2021'
    assert (month in range(13)), 'month must be <13'
    assert (variable in VARIABLES.keys()), f'VARIABLE must be one of {list(VARIABLES.keys())}'
    
    if savepath is None:
        savepath = f'{year}_{month:02d}_{variable}.nc'
    
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
    
    
        
if __name__ == '__main__':
    cli()