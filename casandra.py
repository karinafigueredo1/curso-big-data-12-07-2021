from cassandra.cluster import Cluster


# cluster = Cluster(['ffborelli.ddns.net'])
# session = cluster.connect()
# print(session)

#rows = session.execute("Select * from karina_figueredo.student")
#print(rows)

#for row in rows:
#    print(row)

#session.execute(
#    """INSERT INTO big_data.karina_figueredo (id, date, volume, open, 
#	    high, 
#	    low, 
#	    close, 
#	    adjclose) VALUES (uuid(), '2020-07-02', 7690600 , 185.0, 186.119995117, 183.86000061 , 184.460006714 , 184.460006714)
#    """
#)

#import csv

#csvfile = open('/home/virtual/Downloads/ABT.csv', "r")
#csvreader = csv.reader(csvfile)
import pandas

csvreader = pandas.read_csv("/home/virtual/Downloads/ABT.csv")

for row in csvreader.iterrows():
    date = str(row[1]['date'])
    volume = str(row[1]['volume'])
    open = str(row[1]['open'])
    close = str(row[1]['close'])
    high = str(row[1]['high'])
    low = str(row[1]['low'])
    adjclose = str(row[1]['adjclose'])
    query = """
        INSERT INTO big_data.karina_figueredo
        (id, date, volume, open, high, low, close, adjclose)
        VALUES
        (uuid(), '{date}', {volume}, {open}, {high}, {low}, {close}, {adjclose})
        """.format(date=date,volume=volume,open=open,high=high,low=low,close=close,adjclose=adjclose)
    session.execute(query)