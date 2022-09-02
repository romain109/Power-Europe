def kdb( date:str, index:str):

    from qpython import qconnection 
    import pandas as pd 
    import numpy as np 
    from datetime import datetime
    import movecolumn as mc

    print(date)
    kdb_date = date
    print(date)
    kdb_date = kdb_date.replace('-','.')
    print(kdb_date)

    ## Open a connection to kdb 
    q = qconnection.QConnection(host = 'kdb.dts.corp.local',port=8004,username='Administrator',password='password', pandas=True)

    ## Retrieves data from KDB
    q.open()
    kdb_trades=q.sendSync('.engie.getAssessmentsPrices('+kdb_date+')')
    q.close() 

    ## kdb symbols/strings returned as python byte strings -- convert these to regular strings 
    bstr_cols = kdb_trades.select_dtypes([object]).columns
    for i in bstr_cols:
        kdb_trades[i]=kdb_trades[i].apply(lambda x: x.decode('latin'))

    if "ice" in kdb_trades.columns:
        columns = ['date', 'ASSESS_DATE' , 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'RULE', 'eexpower', 'ice', 'skylight consensus', 'RISK', 'VALIDATION_STATUS', 'COMMENT' ]
        if "skylight consensus" in columns:
                columns = ['date', 'ASSESS_DATE' , 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'RULE', 'eexpower', 'ice', 'skylight consensus', 'RISK', 'VALIDATION_STATUS', 'COMMENT' ]
        else: 
            columns = ['date', 'ASSESS_DATE' , 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'RULE', 'eexpower', 'ice', 'RISK', 'VALIDATION_STATUS', 'COMMENT' ]
    else: 
        columns = ['date', 'ASSESS_DATE' , 'INDEX1', 'PROFILE', 'CONTRACT', 'CONTRACT1_START_DATE','TGP_PRICE', 'RULE', 'eexpower', 'RISK', 'VALIDATION_STATUS', 'COMMENT' ]

    kdb_trades = kdb_trades.loc[:, columns]

    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".M"),"Contract type"] = "Month"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".Q"),"Contract type"] = "Quarter"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".CAL"),"Contract type"] = "Year"
    kdb_trades.dropna(subset = "Contract type", inplace = True)
    kdb_trades = mc.MoveToN(kdb_trades,'Contract type',6)
    kdb_trades = kdb_trades.loc[kdb_trades['INDEX1'] == index,:]

    return kdb_trades
