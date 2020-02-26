import pandas as pd
import pyodbc 
import numpy as np
from ftplib import FTP
import sys, traceback
from datetime import date
from io import StringIO
from decimal import getcontext, Decimal
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#getcontext().prec = 2

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


server = "172.16.12.39"
username = "unusers"
password = "Unusers@1"

today = date.today()

#filename = "Coupon_Sales_&_Refund_Detail_" + today.strftime("%Y%m%d") + ".csv"
filename = "Coupon_Sales_&_Refund_Detail_20200218.csv"

ftp = FTP(server)
ftp.login(user=username, passwd=password)

ftp.cwd("/UNOperations/HITIT/")

with open(filename, "wb") as file:
    ftp.retrbinary("RETR " + filename, file.write)


sio = StringIO(open(filename).read().replace('&amp;', '&').replace('&#x2F;', '&'))
#sio = StringIO(open(filename).read().replace('&#x2F;', '&'))

df = pd.read_csv(sio, delimiter = ';',encoding='ISO-8859_1', dtype=None, error_bad_lines = False, skiprows=1,engine='python', escapechar='\\', names = ['FLT_NO','AA_CODE','DEP_PORT','ARR_PORT','SCH_DEP_DT','SCH_DEP_DT_WITH_TIME','SCH_ARR_DT','SCH_ARR_DT_WITH_TIME','AC_NAME','AC_TYPE','LEG_STATUS','LS_SEQ','SEG_ISN','SEG_DEP_PORT','SEG_ARR_PORT','SEG_SCH_DEP_DT','DATE_OF_ACTION','DATE_OF_SALE','DATE_OF_ACTION_WITH_TIME','ACTION','USER_NAME','FULL_NAME','LEGAL_NAME','SR_ISN','SR_CODE','SR_NAME','SR_TYPE','SR_CITY','SR_COUNTRY','SR_GROUP1','SR_GROUP2','SR_GROUP3','SR_GROUP4','GSA_SR_NAME','FIRST_SR_CODE','FIRST_SR_NAME','ISSUED_SR_NAME','CAPACITY','SEG_AUTHORIZATION','OPER_TYPE','SERVICE_TYPE','CURRENCY','TKT_NO','CPN_NO','REZ_CLASS','PNR_NO','IS_GROUP','DSCNT_CODE','PAX_NAME','VAT_DIVISOR','PNR_GROUP_NAME','FLIGHT_GROUP_NAME','REPORTING_GROUP_NAME','DOMESTIC','FARE_BASIS','PNR_COMPANY_NAME','SOURCE','LEG_ISN','TKT_TYPE','SSR_CODE','SSR_GROUP','CPN_STATUS','OPS_CODE','RCAT','PAX_GENDER','DAYS_TO_FLIGHT','EXCESS_BAGG_FARE_AMOUNT','WB_AMOUNT','XB_AMOUNT','NTBL_AMOUNT','SEAT_AMOUNT','CATERING_AMOUNT','OTHER_SSR_FARE_AMOUNT','PAX_FARE_AMOUNT','EXTRA_SEAT_AMOUNT','TOTAL_FARE_AMOUNT','SSR_FARE_AMOUNT','TAX_AMOUNT','SURCHARGE_AMOUNT','CARBON_EMISSION_AMOUNT','TF_AMOUNT','PS_AMOUNT','SERVICE_FEE_AMOUNT','DISCOUNT_AMOUNT','PENALTY_AMOUNT','COMMISSION_AMOUNT','MU_AMOUNT','FF_AMOUNT','LOC_EXCESS_BAGG_FARE_AMOUNT','LOC_WB_AMOUNT','LOC_XB_AMOUNT','LOC_NTBL_AMOUNT','LOC_SEAT_AMOUNT','LOC_CATERING_AMOUNT','LOC_OTHER_SSR_FARE_AMOUNT','LOC_PAX_FARE_AMOUNT','LOC_EXTRA_SEAT_FARE_AMOUNT','LOC_TOTAL_FARE_AMOUNT','LOC_SSR_FARE_AMOUNT','LOC_TAX_AMOUNT','LOC_SURCHARGE_AMOUNT','LOC_CARBON_EMISSION_AMOUNT','LOC_TF_AMOUNT','LOC_PS_AMOUNT','LOC_SERVICE_FEE_AMOUNT','LOC_DISCOUNT_AMOUNT','LOC_PENALTY_AMOUNT','LOC_COMMISSION_AMOUNT','LOC_FF_AMOUNT','LOC_MU_AMOUNT','REP_EXCESS_BAGG_FARE_AMOUNT','REP_WB_AMOUNT','REP_XB_AMOUNT','REP_NTBL_AMOUNT','REP_SEAT_AMOUNT','REP_CATERING_AMOUNT','REP_OTHER_SSR_FARE_AMOUNT','REP_PAX_FARE_AMOUNT','REP_EXTRA_SEAT_FARE_AMOUNT','REP_TOTAL_FARE_AMOUNT','REP_SSR_FARE_AMOUNT','REP_TAX_AMOUNT','REP_SURCHARGE_AMOUNT','REP_CARBON_EMISSION_AMOUNT','REP_TF_AMOUNT','REP_PS_AMOUNT','REP_SERVICE_FEE_AMOUNT','REP_DISCOUNT_AMOUNT','REP_PENALTY_AMOUNT','REP_COMMISSION_AMOUNT','REP_FF_AMOUNT','REP_MU_AMOUNT','REP_INSURANCE_AMOUNT','COUPON_COUNT','TOTAL_SS_COUNT','PAX_OTHER_COUNT','PAX_ADULT_COUNT','PAX_CHILD_COUNT','PAX_INFANT_COUNT','PAX_OTHER_COUNT_FLOWN','PAX_ADULT_COUNT_FLOWN','PAX_CHILD_COUNT_FLOWN','PAX_INFANT_COUNT_FLOWN','PAD_PASS_COUNT_FLOWN','EXTRA_SEAT_COUNT','PAD_PASS_COUNT','BAGG_CABIN_COUNT','BAGG_EXCESS_COUNT','WEIGHT_BAGG_COUNT','CATERING_COUNT','GRPSS_COUNT','NTBL_COUNT','SEAT_COUNT','CNT_02_DAYS_TO_FLIGHT','CNT_07_DAYS_TO_FLIGHT','CNT_15_DAYS_TO_FLIGHT','CNT_21_DAYS_TO_FLIGHT','CNT_30_DAYS_TO_FLIGHT','CNT_45_DAYS_TO_FLIGHT','CNT_60_DAYS_TO_FLIGHT','CNT_A60_DAYS_TO_FLIGHT','OD_INFO','FIRST_DEP_PORT','LAST_DEP_PORT','PAX_OD_COUNT','FIRST_TKT_NO','TICKET_DEP_PORT','INSURANCE_AMOUNT','LOC_INSURANCE_AMOUNT','MEMBER_ID','IT','BUNDLE_TYPE','REP_ISN','EMAIL','PHONE','PAX_BIRTHDAY','FCMI','CONSENT','IS_RE_ACCOMMODATED','PNR_MEMBER_ID','RPSI','PARENT_TKT_NO','PNR_CREATED_BY','CABIN_CLASS','BAG_WEIGHT','CHECKIN_USER_NAME','CRS_PNR_NO','SR_GROUP5','SR_GROUP6','SR_GROUP7','EMPLOYEE_REGISTRATION_NO','SECTOR'])

df = df[df.FLT_NO.str[0]!='F']
df['LOC_TAX_AMOUNT'] = df['LOC_TAX_AMOUNT'].fillna(0)
df['REP_TAX_AMOUNT'] = df['REP_TAX_AMOUNT'].fillna(0)
df = df.replace(np.NaN, '')
df['DateKey'] =  pd.to_datetime(df['SCH_DEP_DT'],format="%Y-%m-%d %H:%M:%S").dt.strftime('%Y%m%d')
df['TKT_NO']= df['TKT_NO'].astype('str').str[:13]
df['SECTOR'] = df['DEP_PORT'] + df['ARR_PORT']
df['PARENT_TKT_NO']= df['PARENT_TKT_NO'].astype('str').str[:13]
df['FIRST_TKT_NO']= df['FIRST_TKT_NO'].astype(str).str[:13]
df['SCH_DEP_DT'] =  pd.to_datetime(df['SCH_DEP_DT'],format="%Y-%m-%d %H:%M:%S").dt.strftime('%Y-%m-%d')
df['SCH_DEP_DT_WITH_TIME'] =  pd.to_datetime(df['SCH_DEP_DT_WITH_TIME'],format="%Y-%m-%d %H:%M:%S").dt.strftime('%Y-%m-%d %H:%M:%S')
df['SCH_ARR_DT'] =  pd.to_datetime(df['SCH_ARR_DT'], format="%Y-%m-%d %H:%M:%S" ).dt.strftime('%Y-%m-%d')
df['SCH_ARR_DT_WITH_TIME'] =  pd.to_datetime(df['SCH_ARR_DT_WITH_TIME'],format="%Y-%m-%d %H:%M:%S").dt.strftime('%Y-%m-%d %H:%M:%S')
df['SEG_SCH_DEP_DT'] =  pd.to_datetime(df['SEG_SCH_DEP_DT'], format="%Y-%m-%d %H:%M:%S" ).dt.strftime('%Y-%m-%d')
df['DATE_OF_ACTION'] =  pd.to_datetime(df['DATE_OF_ACTION'], format="%Y-%m-%d %H:%M:%S" ).dt.strftime('%Y-%m-%d')
df['DATE_OF_SALE'] =  pd.to_datetime(df['DATE_OF_SALE'], format="%Y-%m-%d %H:%M:%S" ).dt.strftime('%Y-%m-%d')
df['DATE_OF_ACTION_WITH_TIME'] =  pd.to_datetime(df['DATE_OF_ACTION_WITH_TIME'],format="%Y-%m-%d %H:%M:%S").dt.strftime('%Y-%m-%d %H:%M:%S')
#df[['LOC_PAX_FARE_AMOUNT','TAX_AMOUNT','LOC_TOTAL_FARE_AMOUNT','LOC_SURCHARGE_AMOUNT']].apply(lambda x: pd.Series.round(x, 2))
df[['DAYS_TO_FLIGHT', 'LS_SEQ', 'CAPACITY']] = df[['DAYS_TO_FLIGHT', 'LS_SEQ', 'CAPACITY']].astype('int64')
df['EXCESS_BAGG_FARE_AMOUNT'] = round(df['EXCESS_BAGG_FARE_AMOUNT'], 2)
df['XB_AMOUNT'] = round(df['XB_AMOUNT'],2)
df['LOC_PAX_FARE_AMOUNT'] = round(df['LOC_PAX_FARE_AMOUNT'], 2)
df['TAX_AMOUNT'] = round(df['TAX_AMOUNT'], 2)
df['LOC_SURCHARGE_AMOUNT'] = round(df['LOC_SURCHARGE_AMOUNT'], 2)
df['LOC_TOTAL_FARE_AMOUNT'] = round(df['LOC_TOTAL_FARE_AMOUNT'], 2)
df['PAX_FARE_AMOUNT'] = round(df['PAX_FARE_AMOUNT'], 2)
df['TOTAL_FARE_AMOUNT'] = round(df['TOTAL_FARE_AMOUNT'], 2)
df['SSR_FARE_AMOUNT'] = round(df['SSR_FARE_AMOUNT'], 2)
df['DISCOUNT_AMOUNT'] = round(df['DISCOUNT_AMOUNT'], 2)
df['SERVICE_FEE_AMOUNT'] = round(df['SERVICE_FEE_AMOUNT'], 2)
df['PENALTY_AMOUNT'] = round(df['PENALTY_AMOUNT'], 2)
df['SERVICE_FEE_AMOUNT'] = round(df['SERVICE_FEE_AMOUNT'], 2)
df['INSURANCE_AMOUNT'] = round(df['INSURANCE_AMOUNT'], 2)

df['LOC_EXCESS_BAGG_FARE_AMOUNT'] = round(df['LOC_EXCESS_BAGG_FARE_AMOUNT'], 2)
df['LOC_XB_AMOUNT'] = round(df['LOC_XB_AMOUNT'], 2)
df['LOC_TAX_AMOUNT'] = round(df['LOC_TAX_AMOUNT'], 2)
df['LOC_PAX_FARE_AMOUNT'] = round(df['LOC_PAX_FARE_AMOUNT'], 2)
df['LOC_TOTAL_FARE_AMOUNT'] = round(df['TOTAL_FARE_AMOUNT'], 2)
df['LOC_SSR_FARE_AMOUNT'] = round(df['LOC_SSR_FARE_AMOUNT'], 2)
df['LOC_DISCOUNT_AMOUNT'] = round(df['LOC_DISCOUNT_AMOUNT'], 2)
df['LOC_SERVICE_FEE_AMOUNT'] = round(df['LOC_SERVICE_FEE_AMOUNT'], 2)
df['LOC_PENALTY_AMOUNT'] = round(df['LOC_PENALTY_AMOUNT'], 2)
df['LOC_SERVICE_FEE_AMOUNT'] = round(df['LOC_SERVICE_FEE_AMOUNT'], 2)
df['LOC_INSURANCE_AMOUNT'] = round(df['LOC_INSURANCE_AMOUNT'], 2)

#df.head()
#df[['DAYS_TO_FLIGHT', 'LS_SEQ', 'CAPACITY']] = df[['DAYS_TO_FLIGHT', 'LS_SEQ', 'CAPACITY']].astype('float64')

#df.info(verbose=True)
#df.isnull().sum()
# df.to_csv("Errorfiles11-11.csv");

def mail():
        
    msg = MIMEMultipart()

    message = "" + filename + " data was succesfully inserted in database" 
    toaddr = 'taiwo.aiyerin@arikair.com','aderiye.tayo@arikair.com','adedoyin.osibanjo@arikair.com','nwamaka.okafor@arikair.com','ogechi_nwosu@arikair.com'

    # setup the parameters of the message
    password = "welcome@1"
    msg['From'] = "aderiye.tayo@arikair.com"
    msg['To'] =  ','.join(toaddr)

    msg['Subject'] = "Coupon Sales and Refund Details File Loaded"

    # add in the message body
    msg.attach(MIMEText(message, 'plain'))

    #create server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()

    # Login Credentials for sending the mail
    server.login(msg['From'], password)


    # send the message via the server.
    server.sendmail(msg['From'], msg['To'].split(','), msg.as_string())

    server.quit()

    print("Successfully sent Email")
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ARIKDW02;'
                      'Database=crane;'
                      'Trusted_Connection=yes;')

#df.head()

cursor = conn.cursor()

try:
    for index, data in df.iterrows():
   
        cursor.execute("INSERT INTO dbo.CouponSalesFlownRefundArik (FLT_NO,AA_CODE,DEP_PORT,ARR_PORT,SCH_DEP_DT,SCH_DEP_DT_WITH_TIME,SCH_ARR_DT,SCH_ARR_DT_WITH_TIME,AC_NAME,AC_TYPE,LEG_STATUS,LS_SEQ,SEG_ISN,SEG_DEP_PORT,SEG_ARR_PORT,SEG_SCH_DEP_DT,DATE_OF_ACTION,DATE_OF_SALE,DATE_OF_ACTION_WITH_TIME,ACTION,USER_NAME,FULL_NAME,LEGAL_NAME,SR_ISN,SR_CODE,SR_NAME,SR_TYPE,SR_CITY,SR_COUNTRY,SR_GROUP1,SR_GROUP2,SR_GROUP3,SR_GROUP4,GSA_SR_NAME,FIRST_SR_CODE,FIRST_SR_NAME,ISSUED_SR_NAME,CAPACITY,SEG_AUTHORIZATION,OPER_TYPE,SERVICE_TYPE,CURRENCY,TKT_NO,CPN_NO,REZ_CLASS,PNR_NO,IS_GROUP,DSCNT_CODE,PAX_NAME,VAT_DIVISOR,PNR_GROUP_NAME,FLIGHT_GROUP_NAME,REPORTING_GROUP_NAME,DOMESTIC,FARE_BASIS,PNR_COMPANY_NAME,SOURCE,LEG_ISN,TKT_TYPE,SSR_CODE,SSR_GROUP,CPN_STATUS,OPS_CODE,RCAT,PAX_GENDER,DAYS_TO_FLIGHT,EXCESS_BAGG_FARE_AMOUNT,WB_AMOUNT,XB_AMOUNT,NTBL_AMOUNT,SEAT_AMOUNT,CATERING_AMOUNT,OTHER_SSR_FARE_AMOUNT,PAX_FARE_AMOUNT,EXTRA_SEAT_AMOUNT,TOTAL_FARE_AMOUNT,SSR_FARE_AMOUNT,TAX_AMOUNT,SURCHARGE_AMOUNT,CARBON_EMISSION_AMOUNT,TF_AMOUNT,PS_AMOUNT,SERVICE_FEE_AMOUNT,DISCOUNT_AMOUNT,PENALTY_AMOUNT,COMMISSION_AMOUNT,MU_AMOUNT,FF_AMOUNT,LOC_EXCESS_BAGG_FARE_AMOUNT,LOC_WB_AMOUNT,LOC_XB_AMOUNT,LOC_NTBL_AMOUNT,LOC_SEAT_AMOUNT,LOC_CATERING_AMOUNT,LOC_OTHER_SSR_FARE_AMOUNT,LOC_PAX_FARE_AMOUNT,LOC_EXTRA_SEAT_FARE_AMOUNT,LOC_TOTAL_FARE_AMOUNT,LOC_SSR_FARE_AMOUNT,LOC_TAX_AMOUNT,LOC_SURCHARGE_AMOUNT,LOC_CARBON_EMISSION_AMOUNT,LOC_TF_AMOUNT,LOC_PS_AMOUNT,LOC_SERVICE_FEE_AMOUNT,LOC_DISCOUNT_AMOUNT,LOC_PENALTY_AMOUNT,LOC_COMMISSION_AMOUNT,LOC_FF_AMOUNT,LOC_MU_AMOUNT,REP_EXCESS_BAGG_FARE_AMOUNT,REP_WB_AMOUNT,REP_XB_AMOUNT,REP_NTBL_AMOUNT,REP_SEAT_AMOUNT,REP_CATERING_AMOUNT,REP_OTHER_SSR_FARE_AMOUNT,REP_PAX_FARE_AMOUNT,REP_EXTRA_SEAT_FARE_AMOUNT,REP_TOTAL_FARE_AMOUNT,REP_SSR_FARE_AMOUNT,REP_TAX_AMOUNT,REP_SURCHARGE_AMOUNT,REP_CARBON_EMISSION_AMOUNT,REP_TF_AMOUNT,REP_PS_AMOUNT,REP_SERVICE_FEE_AMOUNT,REP_DISCOUNT_AMOUNT,REP_PENALTY_AMOUNT,REP_COMMISSION_AMOUNT,REP_FF_AMOUNT,REP_MU_AMOUNT,REP_INSURANCE_AMOUNT,COUPON_COUNT,TOTAL_SS_COUNT,PAX_OTHER_COUNT,PAX_ADULT_COUNT,PAX_CHILD_COUNT,PAX_INFANT_COUNT,PAX_OTHER_COUNT_FLOWN,PAX_ADULT_COUNT_FLOWN,PAX_CHILD_COUNT_FLOWN,PAX_INFANT_COUNT_FLOWN,PAD_PASS_COUNT_FLOWN,EXTRA_SEAT_COUNT,PAD_PASS_COUNT,BAGG_CABIN_COUNT,BAGG_EXCESS_COUNT,WEIGHT_BAGG_COUNT,CATERING_COUNT,GRPSS_COUNT,NTBL_COUNT,SEAT_COUNT,CNT_02_DAYS_TO_FLIGHT,CNT_07_DAYS_TO_FLIGHT,CNT_15_DAYS_TO_FLIGHT,CNT_21_DAYS_TO_FLIGHT,CNT_30_DAYS_TO_FLIGHT,CNT_45_DAYS_TO_FLIGHT,CNT_60_DAYS_TO_FLIGHT,CNT_A60_DAYS_TO_FLIGHT,OD_INFO,FIRST_DEP_PORT,LAST_DEP_PORT,PAX_OD_COUNT,FIRST_TKT_NO,TICKET_DEP_PORT,INSURANCE_AMOUNT,LOC_INSURANCE_AMOUNT,MEMBER_ID,IT,BUNDLE_TYPE,REP_ISN,EMAIL,PHONE,PAX_BIRTHDAY,FCMI,CONSENT,IS_RE_ACCOMMODATED,PNR_MEMBER_ID,RPSI,PARENT_TKT_NO,PNR_CREATED_BY,CABIN_CLASS,BAG_WEIGHT,CHECKIN_USER_NAME,CRS_PNR_NO,SR_GROUP5,SR_GROUP6,SR_GROUP7,EMPLOYEE_REGISTRATION_NO,SECTOR,DateKey)VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                data['FLT_NO'],data['AA_CODE'],data['DEP_PORT'],data['ARR_PORT'],data['SCH_DEP_DT'],data['SCH_DEP_DT_WITH_TIME'],data['SCH_ARR_DT'],data['SCH_ARR_DT_WITH_TIME'],data['AC_NAME'],data['AC_TYPE'],data['LEG_STATUS'],data['LS_SEQ'],data['SEG_ISN'],data['SEG_DEP_PORT'],data['SEG_ARR_PORT'],data['SEG_SCH_DEP_DT'],data['DATE_OF_ACTION'],data['DATE_OF_SALE'],data['DATE_OF_ACTION_WITH_TIME'],data['ACTION'],data['USER_NAME'],data['FULL_NAME'],data['LEGAL_NAME'],data['SR_ISN'],data['SR_CODE'],data['SR_NAME'],data['SR_TYPE'],data['SR_CITY'],data['SR_COUNTRY'],data['SR_GROUP1'],data['SR_GROUP2'],data['SR_GROUP3'],data['SR_GROUP4'],data['GSA_SR_NAME'],data['FIRST_SR_CODE'],data['FIRST_SR_NAME'],data['ISSUED_SR_NAME'],data['CAPACITY'],data['SEG_AUTHORIZATION'],data['OPER_TYPE'],data['SERVICE_TYPE'],data['CURRENCY'],data['TKT_NO'],data['CPN_NO'],data['REZ_CLASS'],data['PNR_NO'],data['IS_GROUP'],data['DSCNT_CODE'],data['PAX_NAME'],data['VAT_DIVISOR'],data['PNR_GROUP_NAME'],data['FLIGHT_GROUP_NAME'],data['REPORTING_GROUP_NAME'],data['DOMESTIC'],data['FARE_BASIS'],data['PNR_COMPANY_NAME'],data['SOURCE'],data['LEG_ISN'],data['TKT_TYPE'],data['SSR_CODE'],data['SSR_GROUP'],data['CPN_STATUS'],data['OPS_CODE'],data['RCAT'],data['PAX_GENDER'],data['DAYS_TO_FLIGHT'],data['EXCESS_BAGG_FARE_AMOUNT'],data['WB_AMOUNT'],data['XB_AMOUNT'],data['NTBL_AMOUNT'],data['SEAT_AMOUNT'],data['CATERING_AMOUNT'],data['OTHER_SSR_FARE_AMOUNT'],data['PAX_FARE_AMOUNT'],data['EXTRA_SEAT_AMOUNT'],data['TOTAL_FARE_AMOUNT'],data['SSR_FARE_AMOUNT'],data['TAX_AMOUNT'],data['SURCHARGE_AMOUNT'],data['CARBON_EMISSION_AMOUNT'],data['TF_AMOUNT'],data['PS_AMOUNT'],data['SERVICE_FEE_AMOUNT'],data['DISCOUNT_AMOUNT'],data['PENALTY_AMOUNT'],data['COMMISSION_AMOUNT'],data['MU_AMOUNT'],data['FF_AMOUNT'],data['LOC_EXCESS_BAGG_FARE_AMOUNT'],data['LOC_WB_AMOUNT'],data['LOC_XB_AMOUNT'],data['LOC_NTBL_AMOUNT'],data['LOC_SEAT_AMOUNT'],data['LOC_CATERING_AMOUNT'],data['LOC_OTHER_SSR_FARE_AMOUNT'],data['LOC_PAX_FARE_AMOUNT'],data['LOC_EXTRA_SEAT_FARE_AMOUNT'],data['LOC_TOTAL_FARE_AMOUNT'],data['LOC_SSR_FARE_AMOUNT'],data['LOC_TAX_AMOUNT'],data['LOC_SURCHARGE_AMOUNT'],data['LOC_CARBON_EMISSION_AMOUNT'],data['LOC_TF_AMOUNT'],data['LOC_PS_AMOUNT'],data['LOC_SERVICE_FEE_AMOUNT'],data['LOC_DISCOUNT_AMOUNT'],data['LOC_PENALTY_AMOUNT'],data['LOC_COMMISSION_AMOUNT'],data['LOC_FF_AMOUNT'],data['LOC_MU_AMOUNT'],data['REP_EXCESS_BAGG_FARE_AMOUNT'],data['REP_WB_AMOUNT'],data['REP_XB_AMOUNT'],data['REP_NTBL_AMOUNT'],data['REP_SEAT_AMOUNT'],data['REP_CATERING_AMOUNT'],data['REP_OTHER_SSR_FARE_AMOUNT'],data['REP_PAX_FARE_AMOUNT'],data['REP_EXTRA_SEAT_FARE_AMOUNT'],data['REP_TOTAL_FARE_AMOUNT'],data['REP_SSR_FARE_AMOUNT'],data['REP_TAX_AMOUNT'],data['REP_SURCHARGE_AMOUNT'],data['REP_CARBON_EMISSION_AMOUNT'],data['REP_TF_AMOUNT'],data['REP_PS_AMOUNT'],data['REP_SERVICE_FEE_AMOUNT'],data['REP_DISCOUNT_AMOUNT'],data['REP_PENALTY_AMOUNT'],data['REP_COMMISSION_AMOUNT'],data['REP_FF_AMOUNT'],data['REP_MU_AMOUNT'],data['REP_INSURANCE_AMOUNT'],data['COUPON_COUNT'],data['TOTAL_SS_COUNT'],data['PAX_OTHER_COUNT'],data['PAX_ADULT_COUNT'],data['PAX_CHILD_COUNT'],data['PAX_INFANT_COUNT'],data['PAX_OTHER_COUNT_FLOWN'],data['PAX_ADULT_COUNT_FLOWN'],data['PAX_CHILD_COUNT_FLOWN'],data['PAX_INFANT_COUNT_FLOWN'],data['PAD_PASS_COUNT_FLOWN'],data['EXTRA_SEAT_COUNT'],data['PAD_PASS_COUNT'],data['BAGG_CABIN_COUNT'],data['BAGG_EXCESS_COUNT'],data['WEIGHT_BAGG_COUNT'],data['CATERING_COUNT'],data['GRPSS_COUNT'],data['NTBL_COUNT'],data['SEAT_COUNT'],data['CNT_02_DAYS_TO_FLIGHT'],data['CNT_07_DAYS_TO_FLIGHT'],data['CNT_15_DAYS_TO_FLIGHT'],data['CNT_21_DAYS_TO_FLIGHT'],data['CNT_30_DAYS_TO_FLIGHT'],data['CNT_45_DAYS_TO_FLIGHT'],data['CNT_60_DAYS_TO_FLIGHT'],data['CNT_A60_DAYS_TO_FLIGHT'],data['OD_INFO'],data['FIRST_DEP_PORT'],data['LAST_DEP_PORT'],data['PAX_OD_COUNT'],data['FIRST_TKT_NO'],data['TICKET_DEP_PORT'],data['INSURANCE_AMOUNT'],data['LOC_INSURANCE_AMOUNT'],data['MEMBER_ID'],data['IT'],data['BUNDLE_TYPE'],data['REP_ISN'],data['EMAIL'],data['PHONE'],data['PAX_BIRTHDAY'],data['FCMI'],data['CONSENT'],data['IS_RE_ACCOMMODATED'],data['PNR_MEMBER_ID'],data['RPSI'],data['PARENT_TKT_NO'],data['PNR_CREATED_BY'],data['CABIN_CLASS'],data['BAG_WEIGHT'],data['CHECKIN_USER_NAME'],data['CRS_PNR_NO'],data['SR_GROUP5'],data['SR_GROUP6'],data['SR_GROUP7'],data['EMPLOYEE_REGISTRATION_NO'],data['SECTOR'],data['DateKey'])
                   
    cursor.commit()
    commit = conn.commit()
    print('Data successfully inserted to database')
        
except Exception as e:
    print('Error is ' + str(e))
  

    cursor.close()
    conn.close()
mail()