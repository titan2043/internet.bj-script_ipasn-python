#Modules
import pandas as pd
import sqlalchemy
import datetime as dt
import config


#database init
intbjDB = config.DB()
intbjDB.init()


#get bj data from afrinic file and return a dataframe
def getdata(url):
    headers = ['Registry', 'Country Code', 'Type', 'Start', 'Value', 'Date', 'Status', 'Extensions']
    c=pd.read_csv(url,delimiter='|', comment='#', names=headers, dtype=str, keep_default_na=False, na_values = [''], encoding='utf-8')[4:]
    bjData = c[(c['Country Code'] == 'BJ')]
    return bjData


#generate asn data
def genasn(bjData,saveDate):
    asn = bjData[(bjData['Type'] == 'asn')][['Start', 'Date', 'Status']]
    asn['savedate'] = saveDate
    tbl = 'asn'
    col = ['numasn','statusasn','createafrinicat','savedate']
    #insert data in db
    for index, row in asn.iterrows():
        data = [row["Start"],row["Status"],row["Date"], row["savedate"]]
        intbjDB.insert(tbl, col, data)
    print('asn data inserted successful in database')

#generate ip data
def genip(bjData,saveDate):
    ip = bjData[(bjData['Type'] == 'ipv4') | (bjData['Type'] == 'ipv6')][['Type','Start', 'Value', 'Date','Status']]

    ip['savedate'] = saveDate
    tbl = 'ip'
    col = ['typeip','blocip','cidrip','statusip','createafrinicat','savedate']
    #insert data in db
    for index, row in ip.iterrows():
        data = [row["Type"],row["Start"],row["Value"],row["Date"],row["Status"],row["savedate"]]
        intbjDB.insert(tbl, col, data)
    print('ip data inserted successful in database')


def getTestDate():
    return str(dt.datetime.now().strftime("%Y-%m-%d"))

if __name__ == "__main__":
    saveDate = getTestDate()
    url = 'http://ftp.afrinic.net/stats/afrinic/delegated-afrinic-extended-latest'
    dataBJ = getdata(url)
    genasn(dataBJ,saveDate)
    genip(dataBJ,saveDate)
