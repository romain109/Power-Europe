import warnings
warnings.filterwarnings("ignore")

import pandas as pd 
import numpy as np 
from datetime import datetime
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import seaborn as sns
import re
import movecolumn as mc
from qpython import qconnection 

def kdb(date:str, index:str):

    print (f"Data retrieved from kdb for {date}")
    kdb_date = date
    kdb_date = kdb_date.replace('-','.')
 
    ## Open a connection to kdb 
    q = qconnection.QConnection(host = 'kdb.dts.corp.local',port = 8004,username = 'Administrator',password = 'password', pandas = True)

    ## Retrieves data from KDB
    q.open()
    kdb_trades=q.sendSync('.engie.getAssessmentsPrices('+kdb_date+')')
    q.close() 

    ## kdb symbols/strings returned as python byte strings -- convert these to regular strings 
    bstr_cols = kdb_trades.select_dtypes([object]).columns
    for i in bstr_cols:
        kdb_trades[i]=kdb_trades[i].apply(lambda x: x.decode('latin'))


    if "iceeodfutures" in kdb_trades.columns:
        columns = ['date', 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE','markit consensus', 'markit deviation', 'eexpower', 'iceeodfutures', 'skylight consensus', 'RISK',  'RULE',  'VALIDATION_STATUS', 'COMMENT']
        columns_corrected = ['Date', 'INDEX1', 'PROFILE', 'CONTRACT', 'Start Date','TGP','Markit','Markit Std', 'EEX', 'Ice', 'Skylight', 'Risk', 'Rule', 'Validation_status', 'Comment']

        if "skylight consensus" in kdb_trades.columns:
                columns = ['date', 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'markit consensus', 'markit deviation','eexpower', 'iceeodfutures', 'skylight consensus', 'RISK', 'RULE', 'VALIDATION_STATUS', 'COMMENT']
                columns_corrected = ['Date', 'INDEX1', 'PROFILE', 'CONTRACT', 'Start Date','TGP', 'Markit', 'Markit Std', 'EEX', 'Ice', 'Skylight', 'Risk', 'Rule','Validation_status', 'Comment' ]
            
        else: 
            columns = ['date', 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'markit consensus', 'markit deviation','eexpower', 'iceeodfutures', 'RISK','RULE', 'VALIDATION_STATUS', 'COMMENT']
            columns_corrected = ['Date', 'INDEX1', 'PROFILE', 'CONTRACT', 'Start Date','TGP', 'Markit','Markit Std','EEX', 'Ice', 'Risk', 'Rule', 'Validation_status', 'Comment']
 
    else: 
        columns = ['date',  'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'markit consensus','markit deviation','eexpower', 'RISK', 'RULE', 'VALIDATION_STATUS', 'COMMENT']
        columns_corrected = ['Date',  'INDEX1', 'PROFILE', 'CONTRACT', 'Start Date','TGP', 'Markit','Markit Std','EEX', 'Risk',  'Rule', 'Validation_status', 'Comment']
 

    kdb_trades = kdb_trades.loc[:,columns]
    kdb_trades.columns = columns_corrected
    kdb_trades["Index"] = kdb_trades["INDEX1"] + "_" + kdb_trades["PROFILE"]
    kdb_trades = kdb_trades.loc[kdb_trades['Index'] == index,:] 
    kdb_trades = mc.MoveToN(kdb_trades,'Index',2)
    kdb_trades.drop(['INDEX1', 'PROFILE'], axis = 1, inplace = True)
    
    # Creation des contracts types
    # a voir si utile ou non
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".D"),"Contract type"] = "Day"
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".BOM"),"Contract type"] = "BOM"
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".W"),"Contract type"] = "Week"
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".WE"),"Contract type"] = "WE"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".M"),"Contract type"] = "Month"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".Q"),"Contract type"] = "Quarter"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".CAL"),"Contract type"] = "Year"
    kdb_trades.drop(['CONTRACT'], axis = 1, inplace = True)
    kdb_trades = kdb_trades.set_index(['Date'])
  
    #cleaning de l'output 'NoRuleDefined'
    kdb_trades = kdb_trades[kdb_trades['Rule']!="NoRuleDefined"]
    kdb_trades["Start Date"] = pd.to_datetime(kdb_trades["Start Date"])
    kdb_trades.drop_duplicates(subset = ["Start Date","Contract type"], inplace = True)
    kdb_trades.dropna(subset = "Contract type", inplace = True)
    kdb_trades = mc.MoveToN(kdb_trades,'Contract type', 3)

    return kdb_trades


def kdb_plot(kdb_trades, period, Ice = True,  Skylight = True):

    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import seaborn as sns
    import re

    kdb_trades = kdb_trades.loc[kdb_trades["Contract type"] == period, :]
    kdb_trades.drop(['Rule', 'Comment', 'Validation_status', 'Risk'], axis = 1, inplace = True)


    # creation des colonnes des deltas

    if "Ice" in kdb_trades.columns:
        kdb_trades["Ice Delta"] = kdb_trades["TGP"] - kdb_trades["Ice"]
    else:
        pass

    if "Skylight" in kdb_trades.columns:
        kdb_trades["Skylight Delta"] = kdb_trades["TGP"] - kdb_trades["Skylight"]
    else:
        pass

    kdb_trades["EEX Delta"] = kdb_trades["TGP"] - kdb_trades["EEX"]
    kdb_trades["Markit Delta"] = kdb_trades["TGP"] - kdb_trades["Markit"]

    # determination des colonnes delta presente
    
    plt.figure(figsize = (30, 10), dpi = 650)
    plt.style.use('seaborn')
    plt.bar(kdb_trades["Start Date"] , kdb_trades["Markit Delta"], width = 5 ,  label = "Markit Delta", color = 'royalblue')
    plt.plot(kdb_trades["Start Date"] , kdb_trades["Markit Std"], label = " + Markit Std",  color = 'g')
    plt.plot(kdb_trades["Start Date"] , -kdb_trades["Markit Std"], label = " - Markit Std",  color = 'g')
    
    if Ice == True:
        try:
            plt.scatter(kdb_trades["Start Date"] , kdb_trades["Ice Delta"], label = "Ice Delta",  color = 'dodgerblue', marker = 'o', s = 15)
        except:
            print("Ice Data not in timeserie")
    else:
        pass

    if Skylight == True:
        try:
            plt.scatter(kdb_trades["Start Date"] , kdb_trades["Skylight Delta"], label = "Skylight Delta",  color = 'g', marker = 'o', s = 15)
        except:
            print("Skylight Data not in timeserie")
    else: 
        pass

    # plot du graph
    plt.scatter(kdb_trades["Start Date"], kdb_trades["EEX Delta"], label = "EEX Delta",  color = 'crimson', marker = 'o', s = 15)
    plt.fill_between(kdb_trades["Start Date"], kdb_trades["Markit Std"], -kdb_trades["Markit Std"] , alpha = 0.1, color = 'g')
    plt.axhline(y = 0.2, color='r', linestyle=(0,(1,1)), label = " Current threshold")
    plt.axhline(y = -0.2, color='r', linestyle=(0,(1,1)))
    plt.axhline(y = 12.42, color = 'royalblue', linestyle=(0,(1,1)))
    plt.axhline(y = -12.42, color='royalblue', linestyle=(0,(1,1)))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval = 180))

    index = kdb_trades["Index"].iloc[0]
    date = kdb_trades.index.iloc[0]

    plt.title(f"Std Deviation Price Validation - {index} - {period} - {date}")
    plt.xlabel("Maturity")
    plt.ylabel("Discrepancy €")
    plt.xticks(rotation=90)
    plt.legend()

    return 
 
def kdb_plot_fwd_curve(kdb_trades, period, Ice = True,  Skylight = True):

    # on choisit le type de maturite a display
    kdb_trades = kdb_trades.loc[kdb_trades["Contract type"] == period, :]


    # plot du graph 
    plt.figure(figsize = (30, 10), dpi = 650)
    plt.style.use('seaborn')

    if Skylight == True:
        plt.plot(kdb_trades["Start Date"] , kdb_trades["Skylight"], label = "Skylight",  color = 'dodgerblue')
    else:
        pass

    if Ice == True:
        plt.plot(kdb_trades["Start Date"] , kdb_trades["Ice"], label = "Ice",  color = 'purple')
    else: 
        pass

    plt.plot(kdb_trades["Start Date"] , kdb_trades["TGP"], label = "TGP")
    plt.plot(kdb_trades["Start Date"] , kdb_trades["Markit"], label = "Markit")
    plt.plot(kdb_trades["Start Date"] , kdb_trades["EEX"], label = "EEX")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval = 180))

    index = kdb_trades["Index"].iloc[0]
    date = kdb_trades.index[0]

    plt.title(f"Forward Curve - {index} - {period} - {date}")
    plt.xlabel("Maturity")
    plt.ylabel("Price €")
    plt.xticks(rotation=90)
    plt.legend()

    return


def Markit(date:str, index:str):
    
    print (f"Data retrieved from kdb for {date}")
    kdb_date = date
    kdb_date = kdb_date.replace('-','.')
 
    ## Open a connection to kdb 
    q = qconnection.QConnection(host = 'kdb.dts.corp.local',port = 8004,username = 'Administrator',password = 'password', pandas = True)

    ## Retrieves data from KDB
    q.open()
    kdb_trades=q.sendSync('.engie.getAssessmentsPrices('+kdb_date+')')
    q.close() 

    ## kdb symbols/strings returned as python byte strings -- convert these to regular strings 
    bstr_cols = kdb_trades.select_dtypes([object]).columns
    for i in bstr_cols:
        kdb_trades[i]=kdb_trades[i].apply(lambda x: x.decode('latin'))

    columns = ['date', 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'markit consensus', 'RULE']
    columns_corrected = ['Date', 'INDEX1', 'PROFILE', 'CONTRACT', 'Start Date','TGP', 'Markit', 'Rule']
            

    kdb_trades = kdb_trades.loc[:,columns]
    kdb_trades.columns = columns_corrected
    kdb_trades["Index"] = kdb_trades["INDEX1"] + "_" + kdb_trades["PROFILE"]
    kdb_trades = kdb_trades.loc[kdb_trades['Index'] == index,:] 
    kdb_trades = mc.MoveToN(kdb_trades,'Index',2)
    kdb_trades.drop(['INDEX1', 'PROFILE'], axis = 1, inplace = True)
    
    # Creation des contracts types
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".D"),"Contract type"] = "Day"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".BOM"),"Contract type"] = "BOM"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".W"),"Contract type"] = "Week"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".WE"),"Contract type"] = "WE"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".M"),"Contract type"] = "Month"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".Q"),"Contract type"] = "Quarter"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".CAL"),"Contract type"] = "Year"
    kdb_trades.drop(['CONTRACT'], axis = 1, inplace = True)
    kdb_trades = kdb_trades.set_index(['Date'])
  
    #cleaning de l'output 'NoRuleDefined'
    kdb_trades = kdb_trades[kdb_trades['Rule']!="NoRuleDefined"]
    kdb_trades["Start Date"] = pd.to_datetime(kdb_trades["Start Date"])
    kdb_trades.drop_duplicates(subset = ["Start Date","Contract type"], inplace = True)
    kdb_trades.dropna(subset = "Contract type", inplace = True)
    kdb_trades = mc.MoveToN(kdb_trades,'Contract type', 3)


    kdb_trades = kdb_trades.set_index("Start Date")
    kdb_trades = kdb_trades[["Markit", "TGP"]]

    return kdb_trades


def auctions_kdb():

    import datetime
    # Fixture
    start_date = datetime.date(2016, 1, 1)
    end_date = datetime.date(2019, 12, 31)

    index = "FRANCE"

    q = qconnection.QConnection(host='kdb.dts.corp.local', port=8027,username='Administrator',password='', pandas=True)  
   
    q.open()
    kdb_auctions = q.sendSync('{.ccgtPython.getCCGTData x}',
                              f"Table=AUCTIONS, Start_Date={start_date}, End_Date={end_date}, Index={index}")
    q.close()

    dataframe = pd.DataFrame(kdb_auctions)
    dataframe['TIMELINE'] = pd.to_datetime(dataframe['TIMELINE'])
    dataframe.index = dataframe['TIMELINE']
    dataframe = dataframe.sort_index()

    return dataframe 