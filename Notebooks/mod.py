'''

Module Librairies: recap de toutes nos fonctions et trucs utiles

'''

# Definition de la fonction de prix Medeco
import requests
from requests_kerberos import HTTPKerberosAuth
import datetime as dt
import pandas as pd

proxyAddress = 'http://proxymwg:8080'
medeco_protocol = 'https'
medeco_server = 'api-medeco.dts.corp.local'
medeco_server_NP1 = 'api-medeco-dev.dts.corp.local:'
medeco_port = ''
medeco_dt_format ='%Y-%m-%d'

def medeco_get_values(medeco_series: str, dt_from: dt.datetime,  dt_to: dt.datetime, radicalDef: str,
					  frequency: float = 1, server = 'prod', format='df'):

	'''Function that fetches data from Medeco through the Rest API

	-----------PARAMETERS------------------

	medeco_series: the medeco series you want to get as string, catalog included with forwardness but without radical (eg: mu.LNG_JKM.1LI.)
	dt_from: date from which you want to get the data, must be a datetime.datetime
	dt_to: date until which you want to get the data, must be a datetime.datetime
	radicalDef: the radical you want to get for your medeco series (eg: .P for the price on a forward curve), must be a string
	frequency: the frequency on which you want to get the data, must be an number (1 for Daily, 2 for Weekdays and 10 for Monthly)
	server: the server you want to get the data from (dev for test database and prod for prod database)
	format: the format you want to get the data in (df to get a pandas.DataFrame, serie for a pandas.Series, else it will be a dict)

	----------------------------------------'''

	serv = medeco_server if server.lower() == 'prod' else medeco_server_NP1
	url = medeco_protocol + '://' + serv + medeco_port + '/api/series/' + medeco_series.replace('@', '%40') + '/values?'
	parameters = {'from': dt_from.strftime(medeco_dt_format),
				  'to': dt_to.strftime(medeco_dt_format),
				  'radical': radicalDef, 'frequency': int(frequency)}

	req = requests.get(url, auth = HTTPKerberosAuth(), verify=False, params=parameters)
	response = req.json()

	if format.lower() == 'df':
		result = pd.DataFrame.from_dict(response['values'])
		result.set_index(keys= 'date', drop= True, inplace= True)
	elif format.lower() == 'serie':
		result = pd.Series(response['values'])
	else:
		result = response['values']

	return result

