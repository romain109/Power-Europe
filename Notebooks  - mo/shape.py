# module de shaping courbe MO

# immport des modules
import pandas as pd
from ratios.ratio import Ratio
from ratios.scope import Scope
from ratios.ratio import Ratio
from ratios.profiles.filters.filter import Filter
from ratios.smoothings.smoothing import Smoothing
from ratios.smoothings.integral_cubic_spline import IntegralCubicSpline
from ratios.profiles.baseload import Baseload
from ratios.profiles.filters.hours import Hours
from ratios.profiles.filters.months import Months
from ratios.profiles.filters.years import Years
from ratios.profiles.off_peak import OffPeak, OverlappingIndexesPeakAndOffPeak
from ratios.profiles.peak import Peak
from ratios.scope import Scope


def Shape(date:str, index:str, period:str):
    
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
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".D"),"Contract type"] = "Day"
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".BOM"),"Contract type"] = "BOM"
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".W"),"Contract type"] = "Week"
    #kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".WE"),"Contract type"] = "WE"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".M"),"Contract type"] = "Month"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".Q"),"Contract type"] = "Quarter"
    kdb_trades.loc[kdb_trades["CONTRACT"].str.contains(".CAL"),"Contract type"] = "Year"
    kdb_trades.drop(['CONTRACT'], axis = 1, inplace = True)
    kdb_trades = kdb_trades.set_index(['Date'])
  
    # cleaning de l'output 'NoRuleDefined'
    kdb_trades = kdb_trades[kdb_trades['Rule']!="NoRuleDefined"]
    kdb_trades["Start Date"] = pd.to_datetime(kdb_trades["Start Date"])
    kdb_trades.drop_duplicates(subset = ["Start Date","Contract type"], inplace = True)
    kdb_trades.dropna(subset = "Contract type", inplace = True)
    kdb_trades = mc.MoveToN(kdb_trades,'Contract type', 3)
    kdb_trades = kdb_trades.loc[kdb_trades["Contract type"] == period, :]

    kdb_trades = kdb_trades.set_index("Start Date")
    kdb_trades = kdb_trades[["Markit", "TGP"]]

    return kdb_trades



# On calcule ici le ratio de prix de markit
ratio_quar_cal = Ratio(Baseload(data_fr_qua_markit),  #on peut utiliser les filtres par annees, quarters, mois .filter_(Years([2022,2023,2024])
                        line_scope=[Scope.YEAR_NUMBER],
                        column_scope=[Scope.QUARTER]) \
    .compute(numerator_computing_scope=[Scope.YEAR_NUMBER, Scope.QUARTER]).evaluate()

ratio_months_quar = Ratio(Baseload(data_fr_month_markit),  #on peut utiliser les filtres par annees, quarters, mois .filter_(Years([2022,2023,2024])
                        line_scope=[Scope.YEAR_NUMBER,Scope.QUARTER],
                        column_scope=[Scope.MONTH_NUMBER]) \
    .compute(numerator_computing_scope=[Scope.YEAR_NUMBER,Scope.QUARTER, Scope.MONTH_NUMBER]).evaluate()


#prix recalcules du quarter avec les prix cal
a = Baseload(data_fr_cal_1_markit.to_period("Q").resample("D").ffill()).shape(ratio_quar_cal).data
a = a.resample("Q").mean()
a.index = a.index.to_timestamp()

#prix recalcules des months avec les quarters
a = Baseload(data_fr_qua_1_markit.to_period("M").resample("D").ffill()).shape(ratio_months_quar).data
a = a.resample("M").mean()
a.index = a.index.to_timestamp()