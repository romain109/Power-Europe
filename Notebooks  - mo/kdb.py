def kdb(date:str, index:str):

    from qpython import qconnection 
    import pandas as pd 
    import numpy as np 
    from datetime import datetime
    import movecolumn as mc

    print ("mawarae")
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

    if "ice" in kdb_trades.columns:
        columns = ['date', 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE','markit consensus', 'markit deviation', 'eexpower', 'ice', 'skylight consensus', 'RISK',  'RULE',  'VALIDATION_STATUS', 'COMMENT']
        columns_corrected = ['Date', 'INDEX1', 'PROFILE', 'CONTRACT', 'Start Date','TGP','Markit','Markit Std', 'EEX', 'Ice', 'Skylight', 'Risk', 'Rule', 'Validation_status', 'Comment']
        if "skylight consensus" in columns:
                columns = ['date', 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'markit consensus', 'markit deviation','eexpower', 'ice', 'skylight consensus', 'RISK', 'RULE', 'VALIDATION_STATUS', 'COMMENT']
                columns_corrected = ['Date', 'INDEX1', 'PROFILE', 'CONTRACT', 'Start Date','TGP', 'Markit', 'Markit Std', 'EEX', 'Ice', 'Skylight', 'Risk', 'Rule','Validation_status', 'Comment' ]
        else: 
            columns = ['date', 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'markit consensus', 'markit deviation','eexpower', 'ice', 'RISK','RULE', 'VALIDATION_STATUS', 'COMMENT']
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
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".M"),"Contract type"] = "Month"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".Q"),"Contract type"] = "Quarter"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".CAL"),"Contract type"] = "Year"
    kdb_trades.drop(['CONTRACT'], axis = 1, inplace = True)

    #cleaning de l'output 'NoRuleDefined'
    kdb_trades = kdb_trades[kdb_trades['Rule']!="NoRuleDefined"]
    kdb_trades["Start Date"] = pd.to_datetime(kdb_trades["Start Date"])
    kdb_trades.dropna(subset = "Contract type", inplace = True)
    kdb_trades.dropna(subset = "Markit", inplace = True)
    kdb_trades.dropna(subset = "Risk", inplace = True)
    kdb_trades.dropna(subset = "Contract type", inplace = True)
    kdb_trades = mc.MoveToN(kdb_trades,'Contract type', 3)

    
    return kdb_trades


def kdb_plot(kdb_trades, period, Ice = True,  Skylight = True):

    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import seaborn as sns
    import re

    kdb_trades = kdb_trades.loc[kdb_trades["Contract type"] == period, :]
    kdb_trades.drop(['Rule', 'Comment', 'Validation_status'], axis = 1, inplace = True)


    # creation des colonnes des deltas

    delta = []
    if "Ice" in kdb_trades.columns:
        kdb_trades["Ice Delta"] = kdb_trades["TGP"] - kdb_trades["Ice"]
        delta.append("Ice Delta")
    else:
        kdb_trades = kdb_trades


    if "Skylight" in kdb_trades.columns:
        kdb_trades["Skylight Delta"] = kdb_trades["TGP"] - kdb_trades["Skylight"]
        delta.append("Skylight Delta")
    else:
        kdb_trades = kdb_trades


    kdb_trades["EEX Delta"] = kdb_trades["TGP"] - kdb_trades["EEX"]
    kdb_trades["Markit Delta"] = kdb_trades["TGP"] - kdb_trades["Markit"]
    delta.append("EEX Delta")
    delta.append("Markit Delta")

    # determination des colonnes delta presente
    
    mylist = kdb_trades.columns
    r = re.compile(".*Delta")
    newlist = list(filter(r.match, mylist))


    plt.figure(figsize = (30, 10), dpi = 650)
    plt.style.use('seaborn')
    plt.bar(kdb_trades["Start Date"] , kdb_trades["Markit Delta"], width = 5 ,  label = "Markit Delta", color = 'royalblue')
    plt.plot(kdb_trades["Start Date"] , kdb_trades["Markit Std"], label = " + Markit Std",  color = 'g')
    plt.plot(kdb_trades["Start Date"] , -kdb_trades["Markit Std"], label = " - Markit Std",  color = 'g')
    
    if Ice == True:
        plt.scatter(kdb_trades["Start Date"] , kdb_trades["Ice Delta"], label = "Ice Delta",  color = 'dodgerblue', marker = 'o', s = 15)
    else:
        a = 0

    if Skylight == True:
        plt.scatter(kdb_trades["Start Date"] , kdb_trades["Skylight Delta"], label = "Skylight Delta",  color = 'g', marker = 'o', s = 15)
    else: 
        a = 0


    plt.scatter(kdb_trades["Start Date"] , kdb_trades["EEX Delta"], label = "EEX Delta",  color = 'crimson', marker = 'o', s = 15)
    plt.fill_between(kdb_trades["start_date"], kdb_trades["Markit Std"], -kdb_trades["Markit Std"] , alpha = 0.1, color = 'g')
    plt.axhline(y=0.2, color='r', linestyle=(0,(1,1)), label = " Current threshold")
    plt.axhline(y=-0.2, color='r', linestyle=(0,(1,1)))
    plt.axhline(y=12.42, color = 'royalblue', linestyle=(0,(1,1)))
    plt.axhline(y=-12.42, color='royalblue', linestyle=(0,(1,1)))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval = 180))
    plt.title("Std Deviation Price Validation")
    plt.xlabel("Maturity")
    plt.ylabel("Discrepancy €")
    plt.legend()
    plt.show();

    return kdb_trades
