import pandas as pd
from ftplib import FTP
from io import StringIO
import pyodbc 
import numpy as np
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

today = date.today()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

server = "172.16.12.39"
username = "unusers"
password = "Unusers@1"

today = date.today()

#filename = "DFS" + today.strftime("%d%m%Y") + "0001.csv"
filename = "DFS250220200001.csv"
#filename = "1-01092018-01122018.csv"

ftp = FTP(server)
ftp.login(user=username, passwd=password)

ftp.cwd("/UNOperations/Hitit-FCC-Reports/")

with open(filename, "wb") as file:
    ftp.retrbinary("RETR " + filename, file.write)

df = pd.read_csv(filename, delimiter = ',', encoding='ISO-8859_1',index_col=False,  dtype=None, error_bad_lines = False, engine='python', escapechar='\\',skiprows=1, names = ['AC_TYPE_NAME','SERIAL_NO','AC_TYPE_CODE','AA_CODE','FLT_NO','OPERATIONAL_SUFFIX','SERVICE_TYPE','FC_STATUS','STATUS','FLT_SUFFIX','FLIGHT_GROUP_NAME','REPORTING_GROUP_NAME','TOUR_OPERATOR','DEP_COUNTRY','DEP_CITY','DEP_PORT','ARR_COUNTRY','ARR_CITY','ARR_PORT','SCH_DEP_DATE','SCH_DEP_TIME','SCH_ARR_DATE','ARR_DATE','DURATION','EST_DEP_DT','EST_ARR_DT','ACT_DEP_DT','ATD','AC_AB_DT','ATAB','ACT_TD_DT','ATTD','ACT_ARR_DT','ATA','DL_CODE_1','DL_CODE_2','DL_CODE_3','DL_CODE_4','DL_D1','DL_D2','DL_D3','DL_D4','DELAY1','DELAY2','DELAY3','DELAY4','DIV_PORT','DIV_COUNTRY','BOARDING_GATE','SCH_REMARKS','Y_CAP','C_CAP','F_CAP','MTOW','FP_FLIGHT_TIME','MVT_PAX','MSG_SPECIAL_INFO','CRC_COMMENT','VALIDITY_MESSAGE','VALUES_LOCKED','ERR_MESSAGE','SLOT_CTOT','SLOT_CTOT_WARNING','IMP_STATUS','HAS_FLT_PLAN','HAS_LOG_REPORT','LOG_REPORT_NO','LEG_RZ_COUNTS','LEG_CI_COUNTS','SEG_RZ_COUNTS','SEG_CI_COUNTS','LEG_SEG_RZ_TOT_COUNTS','LEG_SEG_CI_TOT_COUNTS','LEG_SEG_RZ_CI_TOTAL_COUNTS','LDM_ADULT_TOT_CNT','LDM_PAX_CHILD_CNT','LDM_PAX_INFANT_CNT','LDM_PAX_PAD_CNT','LDM_ADULT_TOT_CNT2','LDM_PAX_CHILD_CNT2','LDM_PAX_INFANT_CNT2','LDM_PAX_PAD_CNT2','LDM_ADULT_TOTAL','LDM_CHILD_TOTAL','LDM_INFANT_TOTAL','LDM_PAD_TOTAL','LR_OFFBLOCK_DATE','LR_ONBLOCK_DATE','LR_TAKEOFF_DATE','LR_LANDING_DATE','LR_B_OFF','LR_T_OFF','ATD_LR_B_OFF','AIRBOTNE_LR_T_OFF','TOUCHDOWN_LR_LAND','LR_LAND','LR_B_ON','LR_FLT_HOURS','LR_BLCK_HOURS','ATA_LR_B_ON','LR_B_TIME_MIN_DELAY','LR_MIN_DELAY','LR_TOTAL_PAX','LR_ADULT_CNT','LR_CHILD_CNT','LR_PICKED_UP_PAX','LR_PAX_INF','LR_PAX_ADULT','LR_PAX_CHD','LR_PAX_PAD_CNT','LR_CARGO_CNT','LR_LOG_GROUND_TIME','MVT_BLCK_HOURS','MVT_FLT_HOURS','FUEL_SUPPLIER','LITERS_UPLIFT','KG_UPLIFT','AFTER_UPLIFT_KG','FLT_PLAN_FUEL_KG','DEP_FUEL_KG','LANDING_FUEL_KG','TOTAL_BURN_OFF_KG','FLT_PLAN_BURN_OF_KG','BURN_OFF_DEVIATIONS','B_OFF_KG_HOUR','LOAD_FACTOR','AC_NO','CABIN','COCKPIT','FUEL_DENSITY','FLIGHT_PLAN_MIN_REQ_F','FUEL_DOCUMENT_NO','FUEL_REM_BEF_FLT','LOG_NUMBER','TRANSIT_PAX','CARGO_AGENT'])
server = 'ARIKDW02'
database = 'crane'
# df = df.to_csv("error.csv")

#df = df[df.STATUS.str[0]!='C']
#df = df[df.REPORTING_GROUP_NAME.str[0]!='M']
# df = df[df.DEP_COUNTRY.str[0]!='F']
df['SCH_DEP_DATE'] =  pd.to_datetime(df['SCH_DEP_DATE'], format="%d/%m/%Y").dt.strftime('%Y-%m-%d')
df['SCH_ARR_DATE'] =  pd.to_datetime(df['SCH_ARR_DATE'],format="%d/%m/%Y").dt.strftime('%Y-%m-%d')
df['AC_AB_DT'] = df['AC_AB_DT'].replace(np.NaN, today.strftime("%d/%m/%Y"))
df['ACT_TD_DT'] = df['ACT_TD_DT'].replace(np.NaN, today.strftime("%d/%m/%Y"))
df['ACT_DEP_DT'] = df['ACT_DEP_DT'].replace(np.NaN, today.strftime("%d/%m/%Y"))
df['ACT_ARR_DT'] = df['ACT_ARR_DT'].replace(np.NaN, today.strftime("%d/%m/%Y"))
df['LR_OFFBLOCK_DATE'] = df['LR_OFFBLOCK_DATE'].replace(np.NaN, today.strftime("%d/%m/%Y %H:%M"))
df['LR_TAKEOFF_DATE'] = df['LR_TAKEOFF_DATE'].replace(np.NaN, today.strftime("%d/%m/%Y %H:%M"))
df['LR_ONBLOCK_DATE'] = df['LR_ONBLOCK_DATE'].replace(np.NaN, today.strftime("%d/%m/%Y %H:%M"))
df['LR_LANDING_DATE'] = df['LR_LANDING_DATE'].replace(np.NaN, today.strftime("%d/%m/%Y %H:%M"))
df['ATD'] = df['ATD'].replace(np.NaN, "00:00")
df['ATAB'] = df['ATAB'].replace(np.NaN, "00:00")
df['ATTD'] = df['ATTD'].replace(np.NaN, "00:00")
df['ATA'] = df['ATA'].replace(np.NaN, "00:00")
df['DateKey'] =  pd.to_datetime(df['SCH_DEP_DATE'],format="%Y-%m-%d %H:%M:%S").dt.strftime('%Y%m%d')
df['SECTOR'] = df['DEP_PORT'] + df['ARR_PORT']

df = df.replace(np.NaN, '')

#df.isnull().sum()
#df
df['AC_AB_DT'] =  pd.to_datetime(df['AC_AB_DT'], format="%d/%m/%Y").dt.strftime('%Y-%m-%d')
df['ACT_TD_DT'] =  pd.to_datetime(df['ACT_TD_DT'], format="%d/%m/%Y").dt.strftime('%Y-%m-%d')
df['ACT_DEP_DT'] =  pd.to_datetime(df['ACT_DEP_DT'], format="%d/%m/%Y").dt.strftime('%Y-%m-%d')
df['ACT_ARR_DT'] =  pd.to_datetime(df['ACT_ARR_DT'], format="%d/%m/%Y").dt.strftime('%Y-%m-%d')
df['LR_OFFBLOCK_DATE'] =  pd.to_datetime(df['LR_OFFBLOCK_DATE'], format="%d/%m/%Y %H:%M").dt.strftime('%Y-%m-%d %H:%M')
df['LR_TAKEOFF_DATE'] =  pd.to_datetime(df['LR_TAKEOFF_DATE'], format="%d/%m/%Y %H:%M").dt.strftime('%Y-%m-%d %H:%M')
df['LR_ONBLOCK_DATE'] =  pd.to_datetime(df['LR_ONBLOCK_DATE'], format="%d/%m/%Y %H:%M").dt.strftime('%Y-%m-%d %H:%M')
df['LR_LANDING_DATE'] =  pd.to_datetime(df['LR_LANDING_DATE'], format="%d/%m/%Y %H:%M").dt.strftime('%Y-%m-%d %H:%M')

df['SCH_DEP_TIME'] =  pd.to_datetime(df['SCH_DEP_TIME'], format="%H:%M").dt.strftime('%H:%M')
df['ARR_DATE'] =  pd.to_datetime(df['ARR_DATE'], format="%H:%M").dt.strftime('%H:%M')
df['EST_DEP_DT'] =  pd.to_datetime(df['EST_DEP_DT'], format="%H:%M").dt.strftime('%H:%M')
df['EST_ARR_DT'] =  pd.to_datetime(df['EST_ARR_DT'], format="%H:%M").dt.strftime('%H:%M')
df['ATD'] =  pd.to_datetime(df['ATD'], format="%H:%M").dt.strftime('%H:%M')
df['ATAB'] =  pd.to_datetime(df['ATAB'], format="%H:%M").dt.strftime('%H:%M')
df['ATTD'] =  pd.to_datetime(df['ATTD'], format="%H:%M").dt.strftime('%H:%M')
df['ATA'] =  pd.to_datetime(df['ATA'], format="%H:%M").dt.strftime('%H:%M')

def mail():
    
    
    msg = MIMEMultipart()


    message = "DFS" + today.strftime("%d%m%Y") + "0001.csv Data was succesfully inserted in DataBase"
    toaddr = 'taiwo.aiyerin@arikair.com','aderiye.tayo@arikair.com','adedoyin.osibanjo@arikair.com'

    # setup the parameters of the message
    password = "welcome@1"
    msg['From'] = "aderiye.tayo@arikair.com"
    msg['To'] =  ','.join(toaddr)

    msg['Subject'] = "FCC"

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


cursor = conn.cursor()


for index, data in df.iterrows():
   
    cursor.execute("INSERT INTO dbo.FCC (AC_TYPE_NAME,SERIAL_NO,AC_TYPE_CODE,AA_CODE,FLT_NO,OPERATIONAL_SUFFIX,SERVICE_TYPE,FC_STATUS,STATUS,FLT_SUFFIX,FLIGHT_GROUP_NAME,REPORTING_GROUP_NAME,TOUR_OPERATOR,DEP_COUNTRY,DEP_CITY,DEP_PORT,ARR_COUNTRY,ARR_CITY,ARR_PORT,SCH_DEP_DATE,SCH_DEP_TIME,SCH_ARR_DATE,ARR_DATE,DURATION,EST_DEP_DT,EST_ARR_DT,ACT_DEP_DT,ATD,AC_AB_DT,ATAB,ACT_TD_DT,ATTD,ACT_ARR_DT,ATA,DL_CODE_1,DL_CODE_2,DL_CODE_3,DL_CODE_4,DL_D1,DL_D2,DL_D3,DL_D4,DELAY1,DELAY2,DELAY3,DELAY4,DIV_PORT,DIV_COUNTRY,BOARDING_GATE,SCH_REMARKS,Y_CAP,C_CAP,F_CAP,MTOW,FP_FLIGHT_TIME,MVT_PAX,MSG_SPECIAL_INFO,CRC_COMMENT,VALIDITY_MESSAGE,VALUES_LOCKED,ERR_MESSAGE,SLOT_CTOT,SLOT_CTOT_WARNING,IMP_STATUS,HAS_FLT_PLAN,HAS_LOG_REPORT,LOG_REPORT_NO,LEG_RZ_COUNTS,LEG_CI_COUNTS,SEG_RZ_COUNTS,SEG_CI_COUNTS,LEG_SEG_RZ_TOT_COUNTS,LEG_SEG_CI_TOT_COUNTS,LEG_SEG_RZ_CI_TOTAL_COUNTS,LDM_ADULT_TOT_CNT,LDM_PAX_CHILD_CNT,LDM_PAX_INFANT_CNT,LDM_PAX_PAD_CNT,LDM_ADULT_TOT_CNT2,LDM_PAX_CHILD_CNT2,LDM_PAX_INFANT_CNT2,LDM_PAX_PAD_CNT2,LDM_ADULT_TOTAL,LDM_CHILD_TOTAL,LDM_INFANT_TOTAL,LDM_PAD_TOTAL,LR_OFFBLOCK_DATE,LR_ONBLOCK_DATE,LR_TAKEOFF_DATE,LR_LANDING_DATE,LR_B_OFF,LR_T_OFF,ATD_LR_B_OFF,AIRBOTNE_LR_T_OFF,TOUCHDOWN_LR_LAND,LR_LAND,LR_B_ON,LR_FLT_HOURS,LR_BLCK_HOURS,ATA_LR_B_ON,LR_B_TIME_MIN_DELAY,LR_MIN_DELAY,LR_TOTAL_PAX,LR_ADULT_CNT,LR_CHILD_CNT,LR_PICKED_UP_PAX,LR_PAX_INF,LR_PAX_ADULT,LR_PAX_CHD,LR_PAX_PAD_CNT,LR_CARGO_CNT,LR_LOG_GROUND_TIME,MVT_BLCK_HOURS,MVT_FLT_HOURS,FUEL_SUPPLIER,LITERS_UPLIFT,KG_UPLIFT,AFTER_UPLIFT_KG,FLT_PLAN_FUEL_KG,DEP_FUEL_KG,LANDING_FUEL_KG,TOTAL_BURN_OFF_KG,FLT_PLAN_BURN_OF_KG,BURN_OFF_DEVIATIONS,B_OFF_KG_HOUR,LOAD_FACTOR,AC_NO,CABIN,COCKPIT,FUEL_DENSITY,FLIGHT_PLAN_MIN_REQ_F,FUEL_DOCUMENT_NO,FUEL_REM_BEF_FLT,LOG_NUMBER,TRANSIT_PAX,CARGO_AGENT,SECTOR,DateKey)VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
       data['AC_TYPE_NAME'],data['SERIAL_NO'],data['AC_TYPE_CODE'],data['AA_CODE'],data['FLT_NO'],data['OPERATIONAL_SUFFIX'],data['SERVICE_TYPE'],data['FC_STATUS'],data['STATUS'],data['FLT_SUFFIX'],data['FLIGHT_GROUP_NAME'],data['REPORTING_GROUP_NAME'],data['TOUR_OPERATOR'],data['DEP_COUNTRY'],data['DEP_CITY'],data['DEP_PORT'],data['ARR_COUNTRY'],data['ARR_CITY'],data['ARR_PORT'],data['SCH_DEP_DATE'],data['SCH_DEP_TIME'],data['SCH_ARR_DATE'],data['ARR_DATE'],data['DURATION'],data['EST_DEP_DT'],data['EST_ARR_DT'],data['ACT_DEP_DT'],data['ATD'],data['AC_AB_DT'],data['ATAB'],data['ACT_TD_DT'],data['ATTD'],data['ACT_ARR_DT'],data['ATA'],data['DL_CODE_1'],data['DL_CODE_2'],data['DL_CODE_3'],data['DL_CODE_4'],data['DL_D1'],data['DL_D2'],data['DL_D3'],data['DL_D4'],data['DELAY1'],data['DELAY2'],data['DELAY3'],data['DELAY4'],data['DIV_PORT'],data['DIV_COUNTRY'],data['BOARDING_GATE'],data['SCH_REMARKS'],data['Y_CAP'],data['C_CAP'],data['F_CAP'],data['MTOW'],data['FP_FLIGHT_TIME'],data['MVT_PAX'],data['MSG_SPECIAL_INFO'],data['CRC_COMMENT'],data['VALIDITY_MESSAGE'],data['VALUES_LOCKED'],data['ERR_MESSAGE'],data['SLOT_CTOT'],data['SLOT_CTOT_WARNING'],data['IMP_STATUS'],data['HAS_FLT_PLAN'],data['HAS_LOG_REPORT'],data['LOG_REPORT_NO'],data['LEG_RZ_COUNTS'],data['LEG_CI_COUNTS'],data['SEG_RZ_COUNTS'],data['SEG_CI_COUNTS'],data['LEG_SEG_RZ_TOT_COUNTS'],data['LEG_SEG_CI_TOT_COUNTS'],data['LEG_SEG_RZ_CI_TOTAL_COUNTS'],data['LDM_ADULT_TOT_CNT'],data['LDM_PAX_CHILD_CNT'],data['LDM_PAX_INFANT_CNT'],data['LDM_PAX_PAD_CNT'],data['LDM_ADULT_TOT_CNT2'],data['LDM_PAX_CHILD_CNT2'],data['LDM_PAX_INFANT_CNT2'],data['LDM_PAX_PAD_CNT2'],data['LDM_ADULT_TOTAL'],data['LDM_CHILD_TOTAL'],data['LDM_INFANT_TOTAL'],data['LDM_PAD_TOTAL'],data['LR_OFFBLOCK_DATE'],data['LR_ONBLOCK_DATE'],data['LR_TAKEOFF_DATE'],data['LR_LANDING_DATE'],data['LR_B_OFF'],data['LR_T_OFF'],data['ATD_LR_B_OFF'],data['AIRBOTNE_LR_T_OFF'],data['TOUCHDOWN_LR_LAND'],data['LR_LAND'],data['LR_B_ON'],data['LR_FLT_HOURS'],data['LR_BLCK_HOURS'],data['ATA_LR_B_ON'],data['LR_B_TIME_MIN_DELAY'],data['LR_MIN_DELAY'],data['LR_TOTAL_PAX'],data['LR_ADULT_CNT'],data['LR_CHILD_CNT'],data['LR_PICKED_UP_PAX'],data['LR_PAX_INF'],data['LR_PAX_ADULT'],data['LR_PAX_CHD'],data['LR_PAX_PAD_CNT'],data['LR_CARGO_CNT'],data['LR_LOG_GROUND_TIME'],data['MVT_BLCK_HOURS'],data['MVT_FLT_HOURS'],data['FUEL_SUPPLIER'],data['LITERS_UPLIFT'],data['KG_UPLIFT'],data['AFTER_UPLIFT_KG'],data['FLT_PLAN_FUEL_KG'],data['DEP_FUEL_KG'],data['LANDING_FUEL_KG'],data['TOTAL_BURN_OFF_KG'],data['FLT_PLAN_BURN_OF_KG'],data['BURN_OFF_DEVIATIONS'],data['B_OFF_KG_HOUR'],data['LOAD_FACTOR'],data['AC_NO'],data['CABIN'],data['COCKPIT'],data['FUEL_DENSITY'],data['FLIGHT_PLAN_MIN_REQ_F'],data['FUEL_DOCUMENT_NO'],data['FUEL_REM_BEF_FLT'],data['LOG_NUMBER'],data['TRANSIT_PAX'],data['CARGO_AGENT'],data['SECTOR'],data['DateKey'])

cursor.commit()

commit = conn.commit()
print('Data Inserted to DataBase')
cursor.close()
conn.close()
# mail()