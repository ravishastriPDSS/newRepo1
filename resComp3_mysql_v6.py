# -*- coding: utf-8 -*-

import sys
import getopt
# import cx_Oracle
from sys import argv 
import pandas as pd
 
from datetime import datetime, timedelta
import numpy as np
import sqlalchemy
from datetime import date
# import holidays

# Principal Component Analysis
from numpy import array
from sklearn.decomposition import PCA
from scipy import stats    
from sklearn.linear_model import LinearRegression   
from sklearn import metrics        
from datetime import datetime, date, timedelta

import mysql.connector




import holidays


from pandas.tseries.holiday import USFederalHolidayCalendar
cal = USFederalHolidayCalendar()
holidays = cal.holidays(start='2000-01-01', end='2030-12-31')

def today_is_holidayN(dat):
    
    return(dat in holidays)
    

def date_by_adding_business_days(from_date, add_days):
    business_days_to_add = add_days
    current_date = from_date
    while business_days_to_add > 0:
        current_date += timedelta(days=1)
        # if today_is_holiday(current_date):
        rt = today_is_holidayN(current_date)
        if rt:
            continue
        business_days_to_add -= 1
    return current_date

def transformSpotDF( df, shift, factor):
    if shift == 1:
         
        df1 = df.shift(1,axis=0)
        df1.iloc[0,:] = df1.iloc[1,:]
        df1 = df1.append(df.iloc[df.shape[0]-1,:])
    else:
        df1 = df
        
    yz = df1.shape
    
    if factor == 1:
        print ('factor one')
    elif ( factor == 2 or factor == 3 or factor == 4):
        #df2=df1[df1.columns[1:yz[1]-1]].pct_change()
        df2= df1.pct_change()
        df3=df2
        df3 = df3.fillna(100)
        n = df2.shape
        #for i in range(n[0]-1):
        for i in range(n[0]-1):
            df3.iloc[i+1,:] = ( 1 +  df2.iloc[i+1,:] ) * df3.iloc[i,:]
 

        print('processig factortwo and three')
        return df3
    else:
        print ('do nothing')
        
def transformFactorDF( df, shift, factor):
    if shift == 1:
         
        df1 = df.shift(1,axis=0)
        df1.iloc[0,:] = df1.iloc[1,:]
        df1 = df1.append(df.iloc[df.shape[0]-1,:])
    else:
        df1 = df
        
    yz = df1.shape
    
    if factor == 1:
        print ('factor one')
        return(df1)
    elif ( factor == 2 or factor == 3):
        #df2=df1[df1.columns[1:yz[1]-1]].pct_change()
        df2= df1.pct_change()
        df3=df2
        df3 = df3.fillna(100)
        n = df2.shape
        for i in range(n[0]-1):
            df3.iloc[i+1,:] = ( 1 +  df2.iloc[i+1,:] ) * df3.iloc[i,:]
 

        print('processig factortwo and three')
        return df3
    else:
        print ('do nothing')
        return(df1)
        
def add_days_skipping_weekends(start_date, days):
    if not days:
        return start_date
    start_date += timedelta(days=1)
    if start_date.weekday() < 5:
        days -= 1
    return add_days_skipping_weekends(start_date, days)      
        
def shiftDF( df,lstDt):
    #Shift all and replace the last date with today's date
    df1 = df.shift(1,axis=0)
    df1.iloc[0,:] = df1.iloc[1,:]
    pa =df.iloc[df.shape[0]-1,:]
    pa = df.tail(1)
    df1 = pd.concat([df1,pa], axis=0,join='inner')
    # df1 = df1.append(df1,df.iloc[df.shape[0]-1,:], ignore_index=True)
    df1Dts = df1.iloc[:,0]
    ka = df1Dts.shift(-1)
     
    ka.iloc[ka.shape[0]-1]= lstDt

    #today_TS = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
    #ka.iloc[ka.shape[0]-1] = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
    #ka.iloc[ka.shape[0]-1] = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
    df1 = pd.concat([ka,df1.iloc[:,1:4]],axis=1)
    return df1
    #arg1 = sys.argv[1] # window id
    #    arg2 = sys.argv[2] # compute sensitivities
    #    arg3 = sys.argv[3] # snap hr
    #    arg4 = sys.argv[4] # princ comp window id
    #    arg5 = sys.argv[5] # window type
    
    
def getCurrencyCrossTabFI(minPrinCompDtStr, snaphr):
    
    config = {
            'user': 'root',
            'password': 'Sairam1959!',
            'host': '127.0.0.1',
            'database': 'tickdata',
            'raise_on_warnings': True
        }

    con = mysql.connector.connect(**config)
    selectStr = ' select score_dt, MAX(CASE when asset_code = %s THEN item_value ELSE NULL END) as %s,\
        MAX(CASE when asset_code = %s THEN item_value ELSE NULL END) as %s,\
        MAX(CASE when asset_code = %s THEN item_value ELSE NULL END) as %s, \
        MAX(CASE when asset_code =%s THEN item_value ELSE NULL END) as %s,\
        MAX(CASE when asset_code = %s THEN item_value ELSE NULL END) as %s,\
        MAX(CASE when asset_code = %s THEN item_value ELSE NULL END) as %s FROM currencyprices where snaphr = %f and score_dt >= str_to_date(%s,%s)  group by score_dt \
        order by score_dt '% ( "'CN1 Comdty'", "'CN1 Comdty'",\
            "'G 1 Comdty'", "'G 1 Comdty'", \
            "'JB1 Comdty'", "'JB1 Comdty'" ,\
            "'RX1 Comdty'", "'RX1 Comdty'",\
            "'TY1 Comdty'", "'TY1 Comdty'",\
            "'XM1 Comdty'", "'XM1 Comdty'" ,snaphr,  minPrinCompDtStr, "'%m/%d/%Y'" )

    df1 = pd.read_sql(selectStr, con)
    df2 = df1.replace(0,np.nan).ffill()
        #df1 = df1.fillna(method='ffill', axis=0) 
    return(df1,df2)

def pricComp( arg1, arg2, arg3, arg4, arg5):

    config = {
        'user': 'root',
        'password': 'Sairam1959!',
        'host': '127.0.0.1',
        'database': 'tickdata',
        'raise_on_warnings': True
    }

    con = mysql.connector.connect(**config)
    #arg1 = sys.argv[1] # window id
    #    arg2 = sys.argv[2] # compute sensitivities
    #    arg3 = sys.argv[3] # snap hr
    #    arg4 = sys.argv[4] # princ comp window id
    #    arg5 = sys.argv[5] # window type
    snaphr = int(arg3)
    windowid = int(arg1)
    newModelRefDate = '12/29/2010'

    thresholdDate = '12/29/2010'
    normalizeCurrencies = 1
    normFactors = 1
    
    PCA_WINDOW_TYPE = arg5

    compSensitivities = arg2
    princCompWindowId = arg4
    newModelRefDate = '12/29/2010'
     
    thresholdDate =  '12/29/2010'
    normalizeCurrencies = 1
    normFactors = 1
    

    if (PCA_WINDOW_TYPE == 0):
        # DATE_SUB( sysdate(), INTERVAL 10 DAY)
        selectStr = ' select  distinct windowid, startDate,endDate, RUN_MODE,  DATE_SUB(sysdate(), INTERVAL noDays DAY ) as pcaStartDates, DATE(sysdate()) \
            as pcaEndDates, DATE(sysdate()) as  asOfDt, noDays,refStartDate ,DATE_SUB(Date(refstartDate), INTERVAL 181 DAY ),normFactors,normPrices,residualCalcMethod  \
                from pcawindows  where windowid = %d  and ActiveStatus =  %s order by windowid ' % (
            windowid, "'A'")
        
       # selectStr = ' select  distinct windowid, startDate,endDate, RUN_MODE,  DATE_SUB(sysdate(), INTERVAL noDays DAY ) as pcaStartDates, DATE(sysdate()) as pcaEndDates, DATE(sysdate()) as  asOfDt, noDays,refStartDate ,DATE_SUB(Date(refstartDate), INTERVAL 181 DAY ),normFactors,normPrices  from pcawindows  where windowid = %d  and ActiveStatus =  %s order by windowid ' % (
       #    windowid, "'A'")

    else:
        selectStr = ' select  distinct windowid, startDate,endDate, RUN_MODE,  str_to_date(date(sysdate - noDays)) as pcaStartDates, trunc(sysdate) as pcaEndDates, trunc(sysdate) as  asOfDt, noDays,refStartDate , refstartDate - 181,normFactors,normPrices,residalCalcMethod  from pcawindows_shadow  where ActiveStatus =  %s order by windowid ' % ("'A'")
        #selectStr = ' select  distinct windowid, startDate,endDate, RUN_MODE,  to_date(sysdate - noDays) as pcaStartDates, trunc(sysdate) as pcaEndDates, trunc(sysdate) as  asOfDt, noDays,refStartDate , refstartDate - 181,normFactors,normPrices  from pcawindows_shadow  order by windowid '


    df = pd.read_sql(selectStr, con)
    # con.close()
    print(df.head())
    windowids = df['windowid']
    startDts = df['startDate']
    endDts = df['endDate']
    runModes = df['RUN_MODE']
    pcaStartDates = df['pcaStartDates']
    pcaEndDates = df['pcaEndDates']
    asOfDts = df['asOfDt']
    noDays = df['noDays']
    dayCnts = df['noDays']
    
    refStartDts = df['refStartDate']
    refStartDtsLagged = df['refStartDate'] - timedelta(days=181)

    normFactors = df['normFactors']
    normPrices = df['normPrices']
    residualCalcMethods = df['residualCalcMethod']
    pcaStartDtStr = startDts[0].strftime('%m/%d/%Y')
    # pca
    for i in range(len(windowids)):
        normFactor = normFactors[i]
        normPrice = normPrices[i]
        startStr = '%s,%s' % ("'7/1/2010'", "'mm/dd/yyyy'")
        startStr1 = '%s' % ("'7/1/2010'")
        startStr = startDts[0].strftime('%m/%d/%Y')
        startStr1 = startDts[0].strftime('%m/%d/%Y')
        startStr1 = startDts[0].strftime('%m/%d/%Y')
        pcaEndDt = pcaEndDates.iloc[i]
        mode = runModes[i]
        endDt = endDts[i]
        startDt = startDts[i]
        print('normFactor is', normFactor)
        
        endStr = pcaEndDt.strftime('%m/%d/%Y')
        endStr1 = "'{}'".format(endStr)
        residualCalcVal = residualCalcMethods[i]
 
        tmpStr =   ' select distinct a.asset_code,b.asset_desc, a.normPrices, a.residualCalcMethod from Window_assets a, assetnamemap b \
            where a.windowid = %s and strcmp(a.asset_code,b.asset_code) = 0  and  a.asset_type = %s and asset_stage = %s order by a.asset_code ' \
                %(windowids[i],    "'{}'".format('Asset'),1 )
        df = pd.read_sql(tmpStr, con) 
        assets = df.iloc[:,0] 
        assetNames = df.iloc[:,1]
        normCurrencies = df.iloc[:,2]
        residualCalcVals = df.iloc[:,3]
        
        
        
        if mode == 'Current':
            # endDt = endDts[i]
            # endDtStr = "'{}'".format(endDt.strftime('%m/%d/%Y'))
            
            endDt = datetime.today()
            # endDtStr=datetime.today().strftime('%m/%d/%Y')
            endDtStr = "'{}'".format(endDt.strftime('%m/%d/%Y'))
            endDtVal = endDt.date()
            startDtsStr = "'{}'".format(startDts[i].strftime('%m/%d/%Y'))
           
            # sqlStr = 'select distinct asOfDate from PCR_componentsNN where windowid = %s and  asOfDate = score_dt and asOfDate >= to_date(%s,%s)  and asOfDate <= to_date(%s,%s) order by asOfDate asc' %( windowids[i],  startDtsStr, "'mm/dd/yyyy'", endDtStr, "'mm/dd/yyyy'")


            sqlStr = 'select distinct asofdt from princcomps where windowid = %s and  score_dt = asofdt \
                and asofdt >= str_to_date(%s,%s)  and asofdt <= str_to_date(%s,%s) order by asofdt asc' \
                    %( princCompWindowId,  startDtsStr, "'%m/%d/%Y'", endDtStr, "'%m/%d/%Y'")
            df = pd.read_sql(sqlStr, con)
            
            # iterDates = df.iloc[:,0]
            
            
            sqlStrMinPrincDt = 'select min(x.asofdt) from princcomps x, currencyPrices y  where  x.windowid = 1 and  \
            x.score_dt = x.asofdt and x.score_dt = y.score_dt and y.snaphr = %f and \
                y.asset_code in ( select asset_code from window_assets where windowid = %d)' %( snaphr, windowid )
                    
            #sqlStrMinPrincDt = 'select min(asofdt) from princcomps where windowid = %s and 
            #score_dt = asofdt and asofdt >= str_to_date(%s,%s)  and asofdt <= str_to_date(%s,%s) 
            #order by asofdt asc' %( princCompWindowId,  startDtsStr, "'%m/%d/%Y'", endDtStr, "'%m/%d/%Y'")
            
            minPrinCompDts = pd.read_sql(sqlStrMinPrincDt, con)
            minDt = minPrinCompDts.iloc[0,0]
            minPrinCompDtStr = "'{}'".format(minDt.strftime('%m/%d/%Y'))
            # 
            # tmpStr1 = 'select distinct score_dt, prinComp1, prinComp2, prinComp3  from PCR_componentsNN  where score_dt = asofdate  and windowid =  %s and   asOfDate <= to_date(%s,%s)  order by score_dt' %( windowids[i],   endDtStr, "'mm/dd/yyyy'")
             
            # tmpStr1 = 'select distinct score_dt, PrincComp1, PrincComp2, PrincComp3  from princcomps  where score_dt = asofdt and windowid =  %s and   asofdt <= str_to_date(%s,%s)  order by score_dt' %( princCompWindowId, startDtsStr, "'%m/%d/%Y'",   endDtStr, "'%m/%d/%Y'")
              
            
            # tmpStr1 = 'select distinct score_dt, PrincComp1, PrincComp2, PrincComp3  from princcomps  where score_dt = asofdt and windowid =  %s and    asofdt >= str_to_date(%s,%s) and asofdt <= str_to_date(%s,%s)  order by score_dt' %( princCompWindowId,   startDtsStr, "'%m/%d/%Y'", endDtStr, "'%m/%d/%Y'")
               
            #            dtStr = datetime.today().strftime(today()'%m/%d/%Y')
            tmpStr1 = 'select distinct score_dt, PrincComp1, PrincComp2, PrincComp3  from princcomps  \
                where score_dt = asofdt and windowid =  %s and    asofdt >= str_to_date(%s,%s) \
                    order by score_dt' %( princCompWindowId,   minPrinCompDtStr, "'%m/%d/%Y'")
                    
            tmpStr1 = 'select distinct score_dt, PrincComp1, PrincComp2, PrincComp3  from princcomps  \
                        where score_dt = asofdt and windowid =  %s and    asofdt >= str_to_date(%s,%s) \
                            order by score_dt' %( princCompWindowId,   startDtsStr, "'%m/%d/%Y'")
                             
            df1 = pd.read_sql(tmpStr1, con)
            shiftDtStr = '5/4/2020'
            
            shiftDtObj = datetime.strptime( shiftDtStr,'%m/%d/%Y').date()
                
            # thresholdDt = '12/29/2010'
            # thresholdDtObj = datetime.strptime(thresholdDt,'%m/%d/%Y').date()
            # thresholdDtObjAct = thresholdDtObj
            
            # thresholdDtObj30 = (startDts[i]+ timedelta(30))
              
            doFullShift = 1
                
            if ( doFullShift == 1 or pcaEndDt < shiftDtObj):
                # Shift 
                # df1back = df1
                # get the trailing date and find the next business date to fill in the 
                # shifted date
                lastDate = df1.iloc[ df1.shape[0]-1,0]
                #xx=df1['scoreDt'].tail(1)
                #pd.to_datetime(xx, errors='coerce')
                #xx_dt = pd.to_datetime(xx, errors='coerce')
                #lastDate = add_days_skipping_weekends(xx_dt, 1)
                #date_by_adding_business_days(lastDate,1)
                lastDateN = date_by_adding_business_days(lastDate,1)
                #lastDate = df1.iloc[ df1.shape[0]-1,0]
                # No need to shift anymore since we are going to do calcs with asOfDate
                #df1 = shiftDF( df1, lastDateN)

                transPCPS = transformFactorDF( df1.iloc[:,1:df1.shape[1]], 0, normFactor)
                df4=df1
                df4.iloc[:,1:4]=transPCPS
                
                
                [currencyDForig, currencyDFMod] = getCurrencyCrossTabFI(minPrinCompDtStr, snaphr)

                pricesDF = currencyDFMod.iloc[:,[1,2,3,4,5,6]]
                pricesDFTrans = transformSpotDF(pricesDF,0,3)
                currencyDFModTrans = currencyDFMod
            
                currencyDFModTrans.iloc[:,1:pricesDFTrans.shape[1]+1] = pricesDFTrans
                
                # stIndex = currencyDFModTrans.index[currencyDFModTrans['score_dt'] == startDt].tolist()[0]
                #endIndex = currencyDFModTrans.index[currencyDFModTrans['score_dt'] == endDt].tolist()[0]
                
                # if (stIndex < int(noDays)):
                #     k = stIndex + int(noDays)
                # else:
                #     k = stIndex
                
                # Most current date in the database
                colVal = currencyDFModTrans.iloc[len(currencyDFModTrans)-1,0]
                
                if ( colVal <= endDtVal ):
                    endIndex = currencyDFModTrans.index[currencyDFModTrans['score_dt'] == endDt.date()].tolist()[0]
                    startIndex = endIndex - int(noDays)+1
                    # totalLen = len(currencyDFModTrans)
                    # k=startIndex
                    totalLen = endIndex +1
                
                
              
                    currencyDF = currencyDFModTrans[startIndex:totalLen];
                    
                    mergedDF = pd.merge(df4,currencyDF,on='score_dt') 
                    localScoreDt = pd.to_datetime(mergedDF['score_dt']).iloc[len(mergedDF)-1]


                    localScoreDtStr =  "{}".format(localScoreDt.strftime('%m/%d/%Y'))
                    mergedDF = mergedDF.fillna(method='ffill',axis=0)
                    currencyVal = pd.DataFrame(mergedDF.iloc[:,-6:])
                    prinComponents = mergedDF.iloc[:,1:4]
                
                    print(' will do fitlm\n') 
                            
                    wprinComponents = prinComponents
                    wcurrencyValTrans= currencyVal

                    for i in range(len(assets)):
                        linear_regressor = LinearRegression()  
                        linear_regressor.fit(wprinComponents, wcurrencyValTrans.iloc[:,i])  # perform linear regression
                        
                        r_sq = linear_regressor.score(wprinComponents, wcurrencyValTrans.iloc[:,i])
                                
                        intercept = float(linear_regressor.intercept_)
                        RSquared = float(r_sq)

                        
                        beta1 = float(linear_regressor.coef_[0])
                        
                        beta2=  float(linear_regressor.coef_[1])
                        beta3 = float(linear_regressor.coef_[2])
                        
                        pComp1 = beta1 * wprinComponents.iloc[:,0].iloc[len(wprinComponents)-1]
                        pComp2 = beta2 * wprinComponents.iloc[:,1].iloc[len(wprinComponents)-1]
                        pComp3 = beta3 * wprinComponents.iloc[:,2].iloc[len(wprinComponents)-1]
                        forecastValue = intercept + pComp1 + pComp2 + pComp3
                        currencyValue = wcurrencyValTrans.iloc[:,i].iloc[len(wcurrencyValTrans)-1]
                        asset_code = assets[i]
                        
                        noDays = 10 # residuals[]
                        
                        prediction = linear_regressor.predict(wprinComponents)
                        if (residualCalcVal == 1):  
                            residual =(currencyValue- forecastValue )/ currencyValue
                        else:
                            residual =(currencyValue - forecastValue)
                                
                                           #asOfDate ="'{}'".format(datetime.today().strftime('%m/%d/%Y'))
                                           #asOfDate ="'{}'".format(datetime.today().strftime('%m/%d/%Y'))
                                           
                        asOfDate = datetime.today().date()
                        #asOfDate = datetime.today().date()
                        windowId = float(windowids[0])
                        sqlStr =  'insert into regresults( asset_code, windowid, asofdt,  RSquared, intercept,\
                                               beta1, beta2,beta3 ,noDays) values ( "%s",%d, str_to_date("%s","%s") \
                                                                                   ,%f,%f,%f,%f,%f,%f)' \
                                                                                   % (asset_code,windowId,localScoreDtStr, "%m/%d/%Y",RSquared,intercept,beta1,\
                                                                                      beta2,beta3,noDays)
                                            
                                            
                                
                        cur = con.cursor()
                                
                                                              
                        cur.execute(sqlStr)
                        con.commit()
                                
                                
                        sqlStr =  'insert into residuals( asset_code, score_dt,asofdt, windowid, forecast, marketval, residual) \
                                    values ( "%s", str_to_date("%s","%s"), str_to_date("%s","%s"),%d,%f,%f,%f)' %\
                                        (asset_code, localScoreDtStr, "%m/%d/%Y",localScoreDtStr, "%m/%d/%Y",windowId, forecastValue, currencyValue, residual)
                                                                                   
 
                        cur = con.cursor()
                        cur.execute(sqlStr)
                        
                        con.commit()
                        
                        
                   
                
            
                        prediction = linear_regressor.predict(wprinComponents)
        elif mode == 'History':
           
            endDt = endDts[i]
            endDtStr = "'{}'".format(endDt.strftime('%m/%d/%Y'))
            startDtsStr = "'{}'".format(startDts[i].strftime('%m/%d/%Y'))
           
            # sqlStr = 'select distinct asOfDate from PCR_componentsNN where windowid = %s and  asOfDate = score_dt and asOfDate >= to_date(%s,%s)  and asOfDate <= to_date(%s,%s) order by asOfDate asc' %( windowids[i],  startDtsStr, "'mm/dd/yyyy'", endDtStr, "'mm/dd/yyyy'")


            sqlStr = 'select distinct asofdt from princcomps where windowid = %s and  score_dt = asofdt and asofdt >= str_to_date(%s,%s)  and asofdt <= str_to_date(%s,%s) order by asofdt asc' %( princCompWindowId,  startDtsStr, "'%m/%d/%Y'", endDtStr, "'%m/%d/%Y'")
            df = pd.read_sql(sqlStr, con)
            
            # iterDates = df.iloc[:,0]
            
            
            sqlStrMinPrincDt = 'select min(x.asofdt) from princcomps x, currencyPrices y  where  x.windowid = 1 and  \
                x.score_dt = x.asofdt and x.score_dt = y.score_dt and y.snaphr = %f \
                and  y.asset_code in ( select asset_code from window_assets where windowid = %d)' %( snaphr, windowid)
                    
            #sqlStrMinPrincDt = 'select min(asofdt) from princcomps where windowid = %s and 
            #score_dt = asofdt and asofdt >= str_to_date(%s,%s)  and asofdt <= str_to_date(%s,%s) 
            #order by asofdt asc' %( princCompWindowId,  startDtsStr, "'%m/%d/%Y'", endDtStr, "'%m/%d/%Y'")
            
            minPrinCompDts = pd.read_sql(sqlStrMinPrincDt, con)
            minDt = minPrinCompDts.iloc[0,0]
            minPrinCompDtStr = "'{}'".format(minDt.strftime('%m/%d/%Y'))
            minPrinCompDtStr= startDtsStr
            # 
            # tmpStr1 = 'select distinct score_dt, prinComp1, prinComp2, prinComp3  from PCR_componentsNN  where score_dt = asofdate  and windowid =  %s and   asOfDate <= to_date(%s,%s)  order by score_dt' %( windowids[i],   endDtStr, "'mm/dd/yyyy'")
             
            # tmpStr1 = 'select distinct score_dt, PrincComp1, PrincComp2, PrincComp3  from princcomps  where score_dt = asofdt and windowid =  %s and   asofdt <= str_to_date(%s,%s)  order by score_dt' %( princCompWindowId, startDtsStr, "'%m/%d/%Y'",   endDtStr, "'%m/%d/%Y'")
              
            
            # tmpStr1 = 'select distinct score_dt, PrincComp1, PrincComp2, PrincComp3  from princcomps  where score_dt = asofdt and windowid =  %s and    asofdt >= str_to_date(%s,%s) and asofdt <= str_to_date(%s,%s)  order by score_dt' %( princCompWindowId,   startDtsStr, "'%m/%d/%Y'", endDtStr, "'%m/%d/%Y'")
               
#            dtStr = datetime.today().strftime(today()'%m/%d/%Y')
            tmpStr1 = 'select distinct score_dt, PrincComp1, PrincComp2, PrincComp3  from princcomps  \
                where score_dt = asofdt and windowid =  %s and    asofdt >= str_to_date(%s,%s) \
                    order by score_dt' %( princCompWindowId,   minPrinCompDtStr, "'%m/%d/%Y'")
                             
            df1 = pd.read_sql(tmpStr1, con)
            shiftDtStr = '5/4/2020'
            
            shiftDtObj = datetime.strptime( shiftDtStr,'%m/%d/%Y').date()
                
            # thresholdDt = '12/29/2010'
            # thresholdDtObj = datetime.strptime(thresholdDt,'%m/%d/%Y').date()
            # thresholdDtObjAct = thresholdDtObj
            
            # thresholdDtObj30 = (startDts[i]+ timedelta(30))
              
            doFullShift = 1
                
            if ( doFullShift == 1 or pcaEndDt < shiftDtObj):
                # Shift 
                # df1back = df1
                # get the trailing date and find the next business date to fill in the 
                # shifted date
                lastDate = df1.iloc[ df1.shape[0]-1,0]
                #xx=df1['scoreDt'].tail(1)
                #pd.to_datetime(xx, errors='coerce')
                #xx_dt = pd.to_datetime(xx, errors='coerce')
                #lastDate = add_days_skipping_weekends(xx_dt, 1)
                #date_by_adding_business_days(lastDate,1)
                lastDateN = date_by_adding_business_days(lastDate,1)
                #lastDate = df1.iloc[ df1.shape[0]-1,0]
                # No need to shift as princ comps are as ot date today!
                # df1 = shiftDF( df1, lastDateN)

                transPCPS = transformFactorDF( df1.iloc[:,1:df1.shape[1]], 0, normFactor)
                df4=df1
                df4.iloc[:,1:4]=transPCPS
                
                
                [currencyDForig, currencyDFMod] = getCurrencyCrossTabFI(minPrinCompDtStr, snaphr)

                pricesDF = currencyDFMod.iloc[:,[1,2,3,4,5,6]]
                pricesDFTrans = transformSpotDF(pricesDF,0,int(normPrices))
                currencyDFModTrans = currencyDFMod
            
                currencyDFModTrans.iloc[:,1:pricesDFTrans.shape[1]+1] = pricesDFTrans
                
                
#                 import pandas as pd
# import datetime as dt
# df = pd.DataFrame({'Date':['2021-12-13','2021-12-10','2021-12-09','2021-12-08']})
# df['Date'] = pd.to_datetime(df['Date'].astype(str), format='%Y-%m-%d')
# sample_date = dt.datetime.strptime('2021-12-13', '%Y-%m-%d')
# date_index = df.index[df['Date'] == sample_date].tolist()
# print(date_index)

                stIndex = currencyDFModTrans.index[currencyDFModTrans['score_dt'] == startDt].tolist()[0]
                endIndex = currencyDFModTrans.index[currencyDFModTrans['score_dt'] == endDt].tolist()[0]
                
                if (stIndex < int(noDays)):
                    k = stIndex + int(noDays)
                else:
                    k = stIndex
                
                
                 
                startIndex = int(noDays)-1
                # totalLen = len(currencyDFModTrans)
                # k=startIndex
                totalLen = endIndex
            
            
                while (k <= totalLen):
                    currencyDF = currencyDFModTrans[k-(startIndex):k+1];
                    
                    mergedDF = pd.merge(df4,currencyDF,on='score_dt') 
                    localScoreDt = pd.to_datetime(mergedDF['score_dt']).iloc[len(mergedDF)-1]


                    localScoreDtStr =  "{}".format(localScoreDt.strftime('%m/%d/%Y'))
                    mergedDF = mergedDF.fillna(method='ffill',axis=0)
                    currencyVal = pd.DataFrame(mergedDF.iloc[:,-6:])
                    prinComponents = mergedDF.iloc[:,1:4]
                
                    print(' will do fitlm') 
                            
                    wprinComponents = prinComponents
                    wcurrencyValTrans= currencyVal

                    for i in range(len(assets)):
                        linear_regressor = LinearRegression()  
                        linear_regressor.fit(wprinComponents, wcurrencyValTrans.iloc[:,i])  # perform linear regression
        
                        r_sq = linear_regressor.score(wprinComponents, wcurrencyValTrans.iloc[:,i])
                        
                        intercept = float(linear_regressor.intercept_)
                        RSquared = float(r_sq)

                        
                        beta1 = float(linear_regressor.coef_[0])
                        
                        beta2=  float(linear_regressor.coef_[1])
                        beta3 = float(linear_regressor.coef_[2])
                        
                        pComp1 = beta1 * wprinComponents.iloc[:,0].iloc[len(wprinComponents)-1]
                        pComp2 = beta2 * wprinComponents.iloc[:,1].iloc[len(wprinComponents)-1]
                        pComp3 = beta3 * wprinComponents.iloc[:,2].iloc[len(wprinComponents)-1]
                        forecastValue = intercept + pComp1 + pComp2 + pComp3
                        currencyValue = wcurrencyValTrans.iloc[:,i].iloc[len(wcurrencyValTrans)-1]
                        asset_code = assets[i]
                        
                        
                        
                        prediction = linear_regressor.predict(wprinComponents)
                        if (residualCalcVal == 1):  
                            residual =(currencyValue- forecastValue )/ currencyValue
                        else:
                            residual =(currencyValue - forecastValue)
                            
                                       #asOfDate ="'{}'".format(datetime.today().strftime('%m/%d/%Y'))
                                       #asOfDate ="'{}'".format(datetime.today().strftime('%m/%d/%Y'))
                                       
                        # asOfDate = datetime.today().date()
                        #asOfDate = datetime.today().date()
                        windowId = float(windowids[0])
                        sqlStr =  'insert into regresults( asset_code, windowid, asofdt,  RSquared, intercept,\
                                           beta1, beta2,beta3 ,noDays) values ( "%s",%d, str_to_date("%s","%s") \
                                                                               ,%f,%f,%f,%f,%f,%f)' \
                                           % (asset_code,windowId,localScoreDtStr, "%m/%d/%Y",RSquared,intercept,beta1,\
                                              beta2,beta3,noDays)
                                            
                                            
                                
                        cur = con.cursor()

                                                          
                        cur.execute(sqlStr)
                        con.commit()
                                
                                
                        sqlStr =  'insert into residuals( asset_code, score_dt,asofdt, windowid, forecast, marketval, residual) \
                            values ( "%s", str_to_date("%s","%s"), str_to_date("%s","%s"),%d,%f,%f,%f)' %\
                                (asset_code, localScoreDtStr, "%m/%d/%Y",localScoreDtStr, "%m/%d/%Y",windowId, forecastValue, currencyValue, residual)
                                                                               
 
                        cur = con.cursor()
                        cur.execute(sqlStr)

                        con.commit()
                        
                        
                   
                    k = k+1
            
                    prediction = linear_regressor.predict(wprinComponents)


            

        else:
            print (' do nothing)')
            print('Mode not specified')    
if __name__ == "__main__":

    #Now check for extra arguments
    if (len(sys.argv) == 6):
        argument1 = sys.argv[1] # window id
        argument2 = sys.argv[2] # compute sensitivities
        argument3 = sys.argv[3] # snap hr
        argument4 = sys.argv[4] # princ comp window id
        argument5 = sys.argv[5] # window type
        print("Arguments:", argument1, argument2, argument3, argument4)
    elif (len(sys.argv)==5):
            argument1 = sys.argv[1]
            argument2 =sys.argv[2]
            argument3 = '9'
            argument4 = '1'
            argument5 = '0'
    else:
        argument1 ='3' ## window id
        argument2 = '0' ## compute sensitivities
        argument3 = '9' ## snaphr 
        argument4 = '1' ## princ comp windowid 
        argument5 = '0'
    
    argument1 ='5' ## window id
    argument3 = '645' ## snaphr 
    largument1 = int(argument1)
    largument2 = int(argument2)
    largument3 = int(argument3)
    largument4 = int(argument4)
    largument5 = int(argument5)
    
    pricComp( largument1, largument2, largument3, largument4, largument5)
    print('done') 
   
 
 