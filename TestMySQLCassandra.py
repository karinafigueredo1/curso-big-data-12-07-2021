#from cassandra.cluster import Cluster
#import MySQLdb
import pandas as pd
#Importando o modulo inteiro
import connection


#cluster = Cluster(['ffborelli.ddns.net'])
#session = cluster.connect()

# def getConnMySQL():
# 	connMySQL = MySQLdb.connect(
# 		host='177.104.61.65',
# 		user='fgv',
# 		passwd='fgv',
# 		db='stockfgv'
# 	)
# 	return connMySQL
connMySQL = connection.getConnMySQL()

dfStocksFgv = pd.read_sql ("""SELECT stocks.date_, stocks.close, users.username, portfolio.shares, portfolio.symbol FROM stockfgv.portfolio 
	RIGHT JOIN users  ON stockfgv.portfolio.user_id = users.id
	INNER JOIN stocks ON portfolio.symbol = stocks.symbol 
	WHERE stocks.date_ = '2020-05-22';""", connMySQL)
session = connection.getConnCassandra()

print(dfStocksFgv)

for row in dfStocksFgv.iterrows():
    date = str(row[1]['date_'])
    close = str(row[1]['close'])
    username = str(row[1]['username'])
    shares = str(row[1]['shares']) 
    symbol = str(row[1]['symbol']) 
    query = """
        INSERT INTO karina_figueredo.Novo_Cassandra
        (id, date, close, username, shares, symbol)
        VALUES
        (uuid(), '{date}', {close}, '{username}', {shares}, '{symbol}')
        """.format(date=date, close=close, username=username, shares=shares, symbol=symbol)
    session.execute(query)
print(__name__)