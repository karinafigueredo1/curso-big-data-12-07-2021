import connection
import pandas as pd


#client = MongoClient('ffborelli.ddns.net', 27017)

client = connection.getConnMongo()

# db = client.big_data

# collection = db.karina_figueredo

# all = collection.find({})
# def getConnMySQL():
# 	connMySQL = MySQLdb.connect(
# 		host='177.104.61.65',
# 		user='fgv',
# 		passwd='fgv',
# 		db='stockfgv'
# 	)
# 	return connMySQL
connMySQL = connection.getConnMySQL()
dfStocksFgv = pd.read_sql ("SELECT * FROM stockfgv.stocks limit 15", connMySQL)
for row in dfStocksFgv.iterrows():
    data = str(row[1]['date_'])
    volume = str(row[1]['volume'])
    open = str(row[1]['open'])
    close = str(row[1]['close'])
    high = str(row[1]['high'])
    low = str(row[1]['low'])
    adjclose = str(row[1]['adjclose'])
    client.insert({
        "data" : data,
        "volume" : volume,
        "open" : open, 
        "close" : close,
        "high" : high,
        "low" : low,
        "adjclose" : adjclose
    })
    
#for  obj in all:
#    print(obj)

#one = collection.find_one({"id":3})
#print(one)

#insert_obj = collection.insert( { "id" : 6, "name": "Karina F" } )
#print(insert_obj)

#collection.update(
#    {"id": 1},
#
#    {
#        "$set":{
#            "name": "Name 66"
#        }

#    }
#)