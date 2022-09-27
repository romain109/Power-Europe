# import des libs
import pandas as pd
import numpy as np
import datetime as dt
from os import *
from os.path import *
import re 
import xlwings as xw
from xlwings import Range

folder_paths = [r"W:\UK\Risk_Control\Risk_Control_Private\MiddleOffice\GPE\Prices\Price Check Tool\Export\Mark-It Raw\2022 08"]

for folder_path in folder_paths:
    onlyfiles = pd.DataFrame([f for f in listdir(folder_path) if isfile(join(folder_path, f))], columns = ["Files"])
    onlyfiles['Date'] = onlyfiles['Files'].apply(lambda x: dt.datetime.strptime(re.search('(\d\d-\d\d-\d\d\d\d)', x).groups()[0], '%d-%m-%Y'))

markit_consensus = []

# for each file in the month folder
for index, row in onlyfiles.iterrows():
    print(row['Files'])
    wb = xw.Book(folder_path + '/' + row['Files'], update_links = False)
    ws = wb.sheets('Markit Raw')

# filter data on power   
    #num_col = ws.range('A1').end('right').column
    num_row = ws.range('A1').end('down').row
    markit = pd.DataFrame(ws.range((1,1),(num_row,46)).value)
    markit.columns = markit.iloc[0]
    markit = markit.iloc[1:]
    markit_consensus.append(markit)
    print('Data collected from' + row['Files'])


markit_consensus = pd.concat(markit_consensus)
markit_consensus = markit_consensus[markit_consensus['ns1:ContractGroup'] == 'European Power']
markit_consensus['ns1:StandardDeviationPrice'].replace('', np.nan, inplace = True)
markit_consensus.dropna(subset = 'ns1:StandardDeviationPrice')
#markit_consensus.to_csv(r"C:\Users\rmolli\Desktop\Power-Europe\Data Mark-It Raw 07.csv", index = False)
print("travail fait")
markit_consensus
