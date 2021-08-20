from pyhive import hive
from datetime import datetime
import pandas as pd
import MySQLdb

# def getConnMySQL():
# 	connMySQL = MySQLdb.connect(
# 		host='177.104.61.65',
# 		user='fgv',
# 		passwd='fgv',
# 		db='stockfgv'
# 	)
# 	return connMySQL
def getConnHive():
    
    #local
    connHive = hive.Connection(host='ffborelli.ddns.net', 
    port=10000, 
        username='hive', password = 'hive', auth = 'CUSTOM')
    return connHive
connMySQL = getConnMySQL()
connHive = getConnHive()
dfStocksFgv = pd.read_sql ("SELECT * FROM stockfgv.stocks limit 15", connMySQL)
#print(dfStocksFgv)

connMySQL.close()
for row in dfStocksFgv.iterrows():
    data = str(row[1]['date_'])
    volume = str(row[1]['volume'])
    open = str(row[1]['open'])
    close = str(row[1]['close'])
    high = str(row[1]['high'])
    low = str(row[1]['low'])
    adjclose = str(row[1]['adjclose'])
    query = "INSERT INTO big_data.karina_figueredo VALUES ('{data}', {volume}, {open}, {close}, {high}, {low}, {adjclose})".format(data=data, volume=volume, open=open , close=close, high=high, low=low, adjclose=adjclose)
    print(query)

    cur = connHive.cursor()
    cur.execute(query)
    connHive.commit()